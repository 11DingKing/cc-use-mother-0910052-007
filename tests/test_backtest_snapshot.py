"""回测不可变输入快照与运行生命周期的测试。

覆盖：
- 快照不可变性与幂等创建（参数别名归一）
- 行情补录不影响既有快照与运行
- 失败后断点续跑、取消后重试、同快照重放
- 并发启动幂等去重
- 跨账户隔离
- 结果归档与报告的完整性/输入锁定声明
"""

import json
import threading
from datetime import datetime, timedelta
from typing import List

import pytest
from hypothesis import given, strategies as st, settings

from app.backtest.engine import (
    BacktestCancelled,
    BacktestConfig,
    BacktestEngine,
)
from app.backtest.snapshot import (
    build_calendar,
    content_hash,
    freeze_candles,
    freeze_signals,
    thaw_candles,
    thaw_signals,
)
from app.chan.models import RawCandle, Signal, SignalType
from app.config import db_session_scope, get_engine
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.entities.backtest_snapshot import (
    Base as SnapshotBase,
    BacktestRun,
    BacktestSnapshot,
)
from app.entities.stock import StockCandle
from app.mappers.stock_mapper import StockMapper
from app.middleware.exception_handler import (
    ConflictException,
    NotFoundException,
)
from app.services.backtest_run_service import BacktestRunService


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

ACCOUNT_A = "researcher-a"
ACCOUNT_B = "researcher-b"

START = datetime(2024, 1, 1)
DAYS = 40
END = START + timedelta(days=DAYS - 1)


@pytest.fixture(autouse=True)
def clean_db():
    """每个测试使用全新的快照/运行表，并清理测试行情与结果数据。"""
    from app.config import init_database

    init_database()
    engine = get_engine()
    SnapshotBase.metadata.drop_all(bind=engine)
    SnapshotBase.metadata.create_all(bind=engine)
    yield
    SnapshotBase.metadata.drop_all(bind=engine)
    SnapshotBase.metadata.create_all(bind=engine)
    with db_session_scope() as session:
        session.query(StockCandle).filter(
            StockCandle.stock_code.like("9%")
        ).delete(synchronize_session=False)
        session.query(BacktestResultEntity).filter(
            BacktestResultEntity.stock_code.like("9%")
        ).delete(synchronize_session=False)


@pytest.fixture
def service():
    return BacktestRunService()


def seed_candles(
    stock_code: str,
    days: int = DAYS,
    start: datetime = START,
    base_price: float = 100.0,
    period: str = "daily",
) -> List[StockCandle]:
    """向行情库写入测试K线（模拟已缓存的行情数据）。"""
    with db_session_scope() as session:
        mapper = StockMapper(session)
        candles = []
        for i in range(days):
            ts = start + timedelta(days=i)
            price = base_price + (i % 7) * 1.5
            candles.append(StockCandle(
                stock_code=stock_code,
                period=period,
                timestamp=ts,
                open=price * 0.99,
                high=price * 1.02,
                low=price * 0.98,
                close=price,
                volume=10000.0 + i,
            ))
        mapper.upsert_candles(candles)
    return candles


def backfill_candles(stock_code: str, period: str = "daily") -> None:
    """模拟数据补录：修正既有K线价格并追加新K线。"""
    with db_session_scope() as session:
        mapper = StockMapper(session)
        existing = mapper.get_candles(stock_code, period, START, END)
        for candle in existing[:5]:
            candle.close = candle.close * 1.5
            mapper.upsert_candles([candle])
        extra = [
            StockCandle(
                stock_code=stock_code,
                period=period,
                timestamp=END + timedelta(days=i + 1),
                open=110.0,
                high=112.0,
                low=108.0,
                close=110.0 + i,
                volume=20000.0,
            )
            for i in range(5)
        ]
        mapper.upsert_candles(extra)


def run_params(stock_code: str, **overrides):
    params = {
        "stock_code": stock_code,
        "period": "daily",
        "start_date": START,
        "end_date": END,
        "initial_capital": 100000.0,
        "position_size": 1.0,
        "commission_rate": 0.001,
        "slippage": 0.001,
    }
    params.update(overrides)
    return params


def metrics_of(report: dict) -> dict:
    """提取可比较的结果指标（去掉每次运行都不同的 result_id）。"""
    result = dict(report["result"])
    result.pop("result_id", None)
    return result


# ---------------------------------------------------------------------------
# 快照不可变性与别名归一
# ---------------------------------------------------------------------------

class TestSnapshotImmutability:
    """业务模块说明。"""

    def test_snapshot_get_or_create_is_idempotent(self, service):
        """相同输入重复创建得到同一快照，且库中只有一行。"""
        seed_candles("900101")
        first = service.create_snapshot(ACCOUNT_A, **run_params("900101"))
        second = service.create_snapshot(ACCOUNT_A, **run_params("900101"))

        assert first["snapshot_id"] == second["snapshot_id"]
        assert first["snapshot_hash"] == second["snapshot_hash"]

        with db_session_scope() as session:
            count = session.query(BacktestSnapshot).filter(
                BacktestSnapshot.account_id == ACCOUNT_A
            ).count()
        assert count == 1

    def test_snapshot_freezes_all_inputs(self, service):
        """快照冻结行情、策略、日历、费用四类输入并分别计算哈希。"""
        seed_candles("900102")
        snapshot = service.create_snapshot(ACCOUNT_A, **run_params("900102"))

        assert snapshot["candle_count"] == DAYS
        assert snapshot["calendar_days"] == DAYS
        assert snapshot["candles_hash"]
        assert snapshot["signals_hash"]
        assert snapshot["calendar_hash"]
        assert snapshot["fee_hash"]
        assert snapshot["strategy_version"]
        assert snapshot["snapshot_hash"]

    def test_period_aliases_normalize_to_same_snapshot(self, service):
        """参数别名（d/1d/day）归一后映射到同一快照，不会各建一份。"""
        seed_candles("900103")
        canonical = service.create_snapshot(
            ACCOUNT_A, **run_params("900103", period="daily")
        )
        for alias in ("d", "1d", "day"):
            aliased = service.create_snapshot(
                ACCOUNT_A, **run_params("900103", period=alias)
            )
            assert aliased["snapshot_id"] == canonical["snapshot_id"]

        fetched = service.get_snapshot(ACCOUNT_A, canonical["snapshot_id"])
        assert fetched["period"] == "daily"

    def test_different_fee_policy_yields_different_snapshot(self, service):
        """费用口径不同则快照不同，绝不共用。"""
        seed_candles("900104")
        base = service.create_snapshot(ACCOUNT_A, **run_params("900104"))
        other_fee = service.create_snapshot(
            ACCOUNT_A, **run_params("900104", commission_rate=0.002)
        )
        other_capital = service.create_snapshot(
            ACCOUNT_A, **run_params("900104", initial_capital=50000.0)
        )

        assert base["snapshot_id"] != other_fee["snapshot_id"]
        assert base["snapshot_id"] != other_capital["snapshot_id"]
        assert base["fee_hash"] != other_fee["fee_hash"]

    def test_backfill_does_not_change_existing_snapshot(self, service):
        """数据补录后，既有快照内容保持不变；重新创建会得到新快照。"""
        seed_candles("900105")
        before = service.create_snapshot(ACCOUNT_A, **run_params("900105"))

        backfill_candles("900105")

        # 既有快照不变
        after = service.get_snapshot(ACCOUNT_A, before["snapshot_id"])
        assert after["candles_hash"] == before["candles_hash"]
        assert after["candle_count"] == before["candle_count"]
        assert after["snapshot_hash"] == before["snapshot_hash"]

        # 相同参数重新创建：内容已变化，得到不同的快照
        recreated = service.create_snapshot(ACCOUNT_A, **run_params("900105"))
        assert recreated["snapshot_id"] != before["snapshot_id"]
        assert recreated["candles_hash"] != before["candles_hash"]


# ---------------------------------------------------------------------------
# 运行与报告
# ---------------------------------------------------------------------------

class TestRunAndReport:
    """业务模块说明。"""

    def test_run_binds_snapshot_and_report_locks_inputs(self, service):
        """报告明确列出被锁定的输入（各分量哈希）并声明结果完整。"""
        seed_candles("900110")
        report = service.start_run(
            ACCOUNT_A, name="趋势策略", **run_params("900110")
        )

        assert report["status"] == "completed"
        assert report["is_complete"] is True
        assert report["completeness"]["processed_candles"] == DAYS
        assert report["completeness"]["expected_candles"] == DAYS
        assert report["completeness"]["reason"] is None

        locked = report["inputs_locked"]
        assert locked["locked"] is True
        assert locked["snapshot_id"] == report["snapshot_id"]
        assert locked["snapshot_hash"]
        assert locked["market_data"]["candles_hash"]
        assert locked["market_data"]["candle_count"] == DAYS
        assert locked["strategy"]["strategy_version"]
        assert locked["strategy"]["signals_hash"]
        assert locked["calendar"]["calendar_hash"]
        assert locked["calendar"]["trading_days"] == DAYS
        assert locked["fee_policy"]["fee_hash"]
        assert locked["fee_policy"]["commission_rate"] == 0.001
        assert locked["canonical_params"]["period"] == "daily"

        assert report["result"] is not None
        assert report["result"]["final_capital"] == 100000.0  # 无信号时资金不变

    def test_replay_produces_identical_results(self, service):
        """同快照重放：新运行、同快照、结果完全一致。"""
        seed_candles("900111")
        original = service.start_run(
            ACCOUNT_A, name="重放验证", **run_params("900111")
        )
        replayed = service.replay_run(ACCOUNT_A, original["run_id"])

        assert replayed["run_id"] != original["run_id"]
        assert replayed["snapshot_id"] == original["snapshot_id"]
        assert replayed["status"] == "completed"
        assert metrics_of(replayed) == metrics_of(original)

    def test_backfill_does_not_affect_run_or_replay(self, service):
        """行情补录后，续跑/重放仍使用原快照，结果与补录前一致。"""
        seed_candles("900112")
        original = service.start_run(
            ACCOUNT_A, name="补录隔离", **run_params("900112")
        )

        backfill_candles("900112")

        replayed = service.replay_run(ACCOUNT_A, original["run_id"])
        assert replayed["snapshot_id"] == original["snapshot_id"]
        assert metrics_of(replayed) == metrics_of(original)

    def test_failed_run_report_shows_incomplete(self, service):
        """失败运行的报告声明结果不完整及原因。"""
        seed_candles("900113")

        def fail_at_15(state):
            if state["next_index"] == 15:
                raise RuntimeError("boom")

        service.checkpoint_listeners.append(fail_at_15)
        report = service.start_run(
            ACCOUNT_A, name="失败报告", **run_params("900113")
        )

        assert report["status"] == "failed"
        assert report["is_complete"] is False
        assert report["completeness"]["processed_candles"] == 15
        assert report["completeness"]["expected_candles"] == DAYS
        assert "boom" in report["error_message"]
        assert "失败" in report["completeness"]["reason"]
        assert report["result"] is None


# ---------------------------------------------------------------------------
# 失败后继续 / 取消后重试
# ---------------------------------------------------------------------------

class TestResumeAndRetry:
    """业务模块说明。"""

    def test_failure_resume_from_checkpoint(self, service):
        """失败后从断点继续：结果与一次性运行完全一致。"""
        seed_candles("900120")

        def fail_at_15(state):
            if state["next_index"] == 15:
                raise RuntimeError("simulated crash")

        service.checkpoint_listeners.append(fail_at_15)
        failed = service.start_run(
            ACCOUNT_A, name="断点续跑", **run_params("900120")
        )
        assert failed["status"] == "failed"
        assert failed["processed_candles"] == 15

        service.checkpoint_listeners.clear()
        resumed = service.resume_run(ACCOUNT_A, failed["run_id"])

        assert resumed["status"] == "completed"
        assert resumed["attempt"] == 2
        assert resumed["snapshot_id"] == failed["snapshot_id"]
        assert resumed["is_complete"] is True

        # 与同一快照的全新重放结果一致
        replayed = service.replay_run(ACCOUNT_A, failed["run_id"])
        assert metrics_of(resumed) == metrics_of(replayed)

    def test_resume_rejects_non_resumable_status(self, service):
        """已完成或运行中的运行不可续跑。"""
        seed_candles("900121")
        report = service.start_run(
            ACCOUNT_A, name="状态校验", **run_params("900121")
        )
        assert report["status"] == "completed"
        with pytest.raises(ConflictException):
            service.resume_run(ACCOUNT_A, report["run_id"])

    def test_cancel_then_retry_reuses_same_snapshot(self, service):
        """取消后重试：即使期间发生数据补录，仍沿用原快照。"""
        seed_candles("900122")
        holder = {}

        def cancel_at_10(state):
            if state["next_index"] == 10:
                run_id = service.list_runs(ACCOUNT_A)[0]["run_id"]
                holder["run_id"] = run_id
                service.cancel_run(ACCOUNT_A, run_id)

        service.checkpoint_listeners.append(cancel_at_10)
        cancelled = service.start_run(
            ACCOUNT_A, name="取消重试", **run_params("900122")
        )
        assert cancelled["status"] == "cancelled"
        assert cancelled["processed_candles"] == 10
        assert cancelled["is_complete"] is False
        assert "取消" in cancelled["completeness"]["reason"]

        # 取消后发生数据补录
        backfill_candles("900122")

        # 重试仍使用原快照（不受补录影响）
        service.checkpoint_listeners.clear()
        resumed = service.resume_run(ACCOUNT_A, holder["run_id"])
        assert resumed["status"] == "completed"
        assert resumed["snapshot_id"] == cancelled["snapshot_id"]

        replayed = service.replay_run(ACCOUNT_A, holder["run_id"])
        assert metrics_of(resumed) == metrics_of(replayed)

    def test_cancel_terminal_run_conflicts(self, service):
        """已结束的运行不可取消。"""
        seed_candles("900123")
        report = service.start_run(
            ACCOUNT_A, name="取消校验", **run_params("900123")
        )
        with pytest.raises(ConflictException):
            service.cancel_run(ACCOUNT_A, report["run_id"])


# ---------------------------------------------------------------------------
# 并发启动
# ---------------------------------------------------------------------------

class TestConcurrentStarts:
    """业务模块说明。"""

    def test_same_idempotency_key_yields_single_run(self, service):
        """并发携带同一幂等键启动：只产生一个运行，且快照一致。"""
        seed_candles("900130", days=60)
        barrier = threading.Barrier(2)
        results = [None, None]
        errors = [None, None]

        def worker(index):
            try:
                barrier.wait(timeout=10)
                results[index] = service.start_run(
                    ACCOUNT_A,
                    name="并发回测",
                    idempotency_key="req-123",
                    **run_params("900130", start_date=START,
                                  end_date=START + timedelta(days=59)),
                )
            except Exception as e:  # pragma: no cover - 便于诊断
                errors[index] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert errors == [None, None]
        assert results[0]["run_id"] == results[1]["run_id"]
        assert results[0]["snapshot_id"] == results[1]["snapshot_id"]

        runs = service.list_runs(ACCOUNT_A)
        assert len(runs) == 1

        final = service.get_run_report(ACCOUNT_A, results[0]["run_id"])
        assert final["status"] == "completed"
        assert final["is_complete"] is True

    def test_different_keys_create_isolated_consistent_runs(self, service):
        """并发各自启动：两个运行各自绑定一致快照，互不混用。"""
        seed_candles("900131", days=60)
        barrier = threading.Barrier(2)
        results = [None, None]
        errors = [None, None]

        def worker(index):
            try:
                barrier.wait(timeout=10)
                results[index] = service.start_run(
                    ACCOUNT_A,
                    name=f"并发回测-{index}",
                    idempotency_key=f"req-{index}",
                    **run_params("900131", start_date=START,
                                  end_date=START + timedelta(days=59)),
                )
            except Exception as e:  # pragma: no cover
                errors[index] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

        assert errors == [None, None]
        assert results[0]["run_id"] != results[1]["run_id"]
        # 相同输入内容 → 同一份快照（内容寻址），两个运行都完整
        assert results[0]["snapshot_id"] == results[1]["snapshot_id"]
        for report in results:
            final = service.get_run_report(ACCOUNT_A, report["run_id"])
            assert final["status"] == "completed"
            assert final["is_complete"] is True
        assert metrics_of(results[0]) == metrics_of(results[1])

    def test_idempotency_key_reuse_with_different_params_conflicts(self, service):
        """同一幂等键绑定不同输入（不同快照）时拒绝，防止混用。"""
        seed_candles("900132")
        service.start_run(
            ACCOUNT_A,
            name="幂等键",
            idempotency_key="req-xyz",
            **run_params("900132"),
        )
        with pytest.raises(ConflictException):
            service.start_run(
                ACCOUNT_A,
                name="幂等键",
                idempotency_key="req-xyz",
                **run_params("900132", commission_rate=0.005),
            )


# ---------------------------------------------------------------------------
# 跨账户隔离
# ---------------------------------------------------------------------------

class TestCrossAccountIsolation:
    """业务模块说明。"""

    def test_cross_account_access_is_invisible(self, service):
        """账户B无法看到或操作账户A的快照与运行。"""
        seed_candles("900140")
        snapshot = service.create_snapshot(ACCOUNT_A, **run_params("900140"))
        report = service.start_run(
            ACCOUNT_A, name="私有回测", snapshot_id=snapshot["snapshot_id"]
        )
        run_id = report["run_id"]

        with pytest.raises(NotFoundException):
            service.get_snapshot(ACCOUNT_B, snapshot["snapshot_id"])
        with pytest.raises(NotFoundException):
            service.get_run_report(ACCOUNT_B, run_id)
        with pytest.raises(NotFoundException):
            service.resume_run(ACCOUNT_B, run_id)
        with pytest.raises(NotFoundException):
            service.replay_run(ACCOUNT_B, run_id)
        with pytest.raises(NotFoundException):
            service.cancel_run(ACCOUNT_B, run_id)
        with pytest.raises(NotFoundException):
            service.archive_run(ACCOUNT_B, run_id)

        assert service.list_runs(ACCOUNT_B) == []
        assert service.list_snapshots(ACCOUNT_B) == []

    def test_same_inputs_different_accounts_get_distinct_snapshots(self, service):
        """相同输入在不同账户下是各自独立的快照，互不混用。"""
        seed_candles("900141")
        snap_a = service.create_snapshot(ACCOUNT_A, **run_params("900141"))
        snap_b = service.create_snapshot(ACCOUNT_B, **run_params("900141"))

        assert snap_a["snapshot_id"] != snap_b["snapshot_id"]
        # 内容哈希相同（输入一致），但快照实体各自独立
        assert snap_a["snapshot_hash"] == snap_b["snapshot_hash"]


# ---------------------------------------------------------------------------
# 结果归档
# ---------------------------------------------------------------------------

class TestArchiving:
    """业务模块说明。"""

    def test_archive_completed_run(self, service):
        """归档已完成运行：报告标记归档，运行变为只读。"""
        seed_candles("900150")
        report = service.start_run(
            ACCOUNT_A, name="归档测试", **run_params("900150")
        )
        archived = service.archive_run(ACCOUNT_A, report["run_id"])

        assert archived["archived"] is True
        assert archived["status"] == "completed"
        assert archived["is_complete"] is True
        assert archived["result"] is not None

        # 归档后不可取消、不可续跑，但仍可同快照重放（产生新运行）
        with pytest.raises(ConflictException):
            service.cancel_run(ACCOUNT_A, report["run_id"])
        with pytest.raises(ConflictException):
            service.resume_run(ACCOUNT_A, report["run_id"])

        replayed = service.replay_run(ACCOUNT_A, report["run_id"])
        assert replayed["archived"] is False
        assert replayed["snapshot_id"] == report["snapshot_id"]

    def test_archive_requires_completed_status(self, service):
        """未完成的运行不可归档。"""
        seed_candles("900151")

        def fail_early(state):
            if state["next_index"] == 5:
                raise RuntimeError("boom")

        service.checkpoint_listeners.append(fail_early)
        failed = service.start_run(
            ACCOUNT_A, name="归档校验", **run_params("900151")
        )
        assert failed["status"] == "failed"
        with pytest.raises(ConflictException):
            service.archive_run(ACCOUNT_A, failed["run_id"])


# ---------------------------------------------------------------------------
# 引擎级断点/恢复/取消
# ---------------------------------------------------------------------------

def _engine_candle(day: int, price: float = 100.0) -> RawCandle:
    return RawCandle(
        timestamp=START + timedelta(days=day),
        open=price * 0.99,
        high=price * 1.02,
        low=price * 0.98,
        close=price,
        volume=1000.0,
    )


def _engine_signal(day: int, signal_type: SignalType, price: float = 100.0) -> Signal:
    return Signal(
        stock_code="TEST",
        signal_type=signal_type,
        timestamp=START + timedelta(days=day),
        price=price,
        level="daily",
        strength=0.5,
    )


def _engine_config(days: int = 30) -> BacktestConfig:
    return BacktestConfig(
        stock_code="TEST",
        period="daily",
        start_date=START,
        end_date=START + timedelta(days=days),
        initial_capital=100000.0,
        position_size=1.0,
    )


class TestEngineCheckpointResume:
    """业务模块说明。"""

    def test_resume_matches_single_shot(self):
        """引擎从断点恢复的最终结果与一次性运行逐位一致。"""
        candles = [_engine_candle(i, 100 + i) for i in range(30)]
        signals = [
            _engine_signal(2, SignalType.BUY_1, 102),
            _engine_signal(25, SignalType.SELL_1, 125),
        ]
        config = _engine_config()

        single = BacktestEngine().run(config, candles, signals)

        states = []
        BacktestEngine().run(
            config, candles, signals, checkpoint_callback=states.append
        )
        assert len(states) == len(candles)

        # 从第10根K线后的断点恢复（此时持仓未平仓）
        resumed = BacktestEngine().run(
            config, candles, signals, resume_state=states[9]
        )

        assert resumed.final_capital == single.final_capital
        assert resumed.total_trades == single.total_trades
        assert len(resumed.equity_curve) == len(candles)
        assert resumed.equity_curve == single.equity_curve
        for resumed_trade, single_trade in zip(resumed.trades, single.trades):
            assert resumed_trade.entry_time == single_trade.entry_time
            assert resumed_trade.entry_price == single_trade.entry_price
            assert resumed_trade.exit_price == single_trade.exit_price
            assert resumed_trade.profit == single_trade.profit
            assert resumed_trade.is_closed == single_trade.is_closed

    def test_checkpoint_state_is_json_serializable(self):
        """断点状态必须可 JSON 序列化且恢复后语义不变。"""
        candles = [_engine_candle(i, 100 + i) for i in range(20)]
        signals = [_engine_signal(2, SignalType.BUY_1, 102)]
        config = _engine_config()

        states = []
        BacktestEngine().run(
            config, candles, signals, checkpoint_callback=states.append
        )
        state = json.loads(json.dumps(states[9]))
        resumed = BacktestEngine().run(
            config, candles, signals, resume_state=state
        )
        assert len(resumed.equity_curve) == len(candles)

    def test_cancel_stops_at_next_candle(self):
        """取消标记置位后，引擎在下一根K线处停止。"""
        candles = [_engine_candle(i) for i in range(20)]
        config = _engine_config()
        calls = {"n": 0}

        def should_cancel():
            calls["n"] += 1
            return calls["n"] > 5

        with pytest.raises(BacktestCancelled):
            BacktestEngine().run(
                config, candles, [], should_cancel=should_cancel
            )


# ---------------------------------------------------------------------------
# 快照冻结/解冻与哈希
# ---------------------------------------------------------------------------

class TestSnapshotFreezing:
    """业务模块说明。"""

    def test_freeze_sorts_candles_before_hash(self):
        """行情冻结前按时间排序：输入顺序不影响内容哈希。"""
        c1 = _engine_candle(1, 101)
        c2 = _engine_candle(2, 102)
        assert content_hash(freeze_candles([c1, c2])) == content_hash(
            freeze_candles([c2, c1])
        )

    def test_signal_freeze_thaw_roundtrip(self):
        """信号冻结/解冻后保持全部字段。"""
        signal = Signal(
            stock_code="TEST",
            signal_type=SignalType.BUY_2,
            timestamp=datetime(2024, 3, 5, 10, 30),
            price=12.34,
            level="daily",
            strength=0.8,
            details={"source": "unit-test"},
        )
        thawed = thaw_signals(freeze_signals([signal]))[0]
        assert thawed.stock_code == signal.stock_code
        assert thawed.signal_type == signal.signal_type
        assert thawed.timestamp == signal.timestamp
        assert thawed.price == signal.price
        assert thawed.level == signal.level
        assert thawed.strength == signal.strength

    def test_calendar_built_from_frozen_candles(self):
        """交易日历由冻结的行情片段推导，随片段锁定。"""
        candles = [_engine_candle(i) for i in (0, 1, 1, 3)]
        calendar = build_calendar(candles)
        assert calendar == ["2024-01-01", "2024-01-02", "2024-01-04"]


@st.composite
def raw_candle_strategy(draw):
    """业务模块说明。"""
    return RawCandle(
        timestamp=draw(st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 1, 1),
        )),
        open=draw(st.floats(1.0, 1000.0, allow_nan=False, allow_infinity=False)),
        high=draw(st.floats(1.0, 1000.0, allow_nan=False, allow_infinity=False)),
        low=draw(st.floats(1.0, 1000.0, allow_nan=False, allow_infinity=False)),
        close=draw(st.floats(1.0, 1000.0, allow_nan=False, allow_infinity=False)),
        volume=draw(st.floats(0.0, 1e9, allow_nan=False, allow_infinity=False)),
    )


class TestSnapshotFreezingProperties:
    """业务模块说明。"""

    @given(st.lists(raw_candle_strategy(), max_size=30))
    @settings(max_examples=50, deadline=None)
    def test_freeze_thaw_roundtrip(self, candles: List[RawCandle]):
        """任意行情片段冻结/解冻后与排序后的原序列完全一致。"""
        thawed = thaw_candles(freeze_candles(candles))
        assert thawed == sorted(candles, key=lambda c: c.timestamp)

    @given(st.lists(raw_candle_strategy(), max_size=30))
    @settings(max_examples=50, deadline=None)
    def test_json_roundtrip_preserves_hash(self, candles: List[RawCandle]):
        """冻结内容经 JSON 落库再读回，内容哈希不变。"""
        frozen = freeze_candles(candles)
        restored = json.loads(json.dumps(frozen))
        assert content_hash(restored) == content_hash(frozen)


# ---------------------------------------------------------------------------
# API 层
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """业务模块说明。"""
    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c:
        yield c


class TestSnapshotRunAPI:
    """业务模块说明。"""

    def test_snapshot_run_report_flow(self, client):
        """创建快照 → 启动运行 → 报告声明锁定输入与完整性。"""
        seed_candles("900201")

        resp = client.post("/api/backtest/snapshots", json={
            "account_id": ACCOUNT_A,
            "stock_code": "900201",
            "period": "d",  # 别名
            "start_date": "2024-01-01",
            "end_date": "2024-02-09",
        })
        assert resp.status_code == 200
        snapshot = resp.json()
        assert snapshot["period"] == "daily"  # 别名已归一
        assert snapshot["candle_count"] == DAYS

        # 相同输入重复创建 → 同一快照
        resp2 = client.post("/api/backtest/snapshots", json={
            "account_id": ACCOUNT_A,
            "stock_code": "900201",
            "period": "daily",
            "start_date": "2024-01-01",
            "end_date": "2024-02-09",
        })
        assert resp2.json()["snapshot_id"] == snapshot["snapshot_id"]

        # 启动运行
        resp = client.post("/api/backtest/runs", json={
            "account_id": ACCOUNT_A,
            "name": "API回测",
            "snapshot_id": snapshot["snapshot_id"],
            "idempotency_key": "api-req-1",
        })
        assert resp.status_code == 200
        report = resp.json()
        assert report["status"] == "completed"
        assert report["is_complete"] is True
        assert report["inputs_locked"]["locked"] is True
        assert report["inputs_locked"]["snapshot_id"] == snapshot["snapshot_id"]
        run_id = report["run_id"]

        # 幂等重发 → 同一运行
        resp = client.post("/api/backtest/runs", json={
            "account_id": ACCOUNT_A,
            "name": "API回测",
            "snapshot_id": snapshot["snapshot_id"],
            "idempotency_key": "api-req-1",
        })
        assert resp.json()["run_id"] == run_id

        # 查询报告
        resp = client.get(
            f"/api/backtest/runs/{run_id}",
            params={"account_id": ACCOUNT_A},
        )
        assert resp.status_code == 200
        assert resp.json()["completeness"]["is_complete"] is True

        # 重放
        resp = client.post(
            f"/api/backtest/runs/{run_id}/replay",
            params={"account_id": ACCOUNT_A},
        )
        assert resp.status_code == 200
        replayed = resp.json()
        assert replayed["run_id"] != run_id
        assert replayed["snapshot_id"] == snapshot["snapshot_id"]

        # 归档
        resp = client.post(
            f"/api/backtest/runs/{run_id}/archive",
            params={"account_id": ACCOUNT_A},
        )
        assert resp.status_code == 200
        assert resp.json()["archived"] is True

        # 归档后取消 → 409
        resp = client.post(
            f"/api/backtest/runs/{run_id}/cancel",
            params={"account_id": ACCOUNT_A},
        )
        assert resp.status_code == 409

    def test_cross_account_access_returns_404(self, client):
        """跨账户查询运行/快照返回404，不泄露信息。"""
        seed_candles("900202")
        resp = client.post("/api/backtest/runs", json={
            "account_id": ACCOUNT_A,
            "name": "私有",
            "stock_code": "900202",
            "period": "daily",
            "start_date": "2024-01-01",
            "end_date": "2024-02-09",
        })
        assert resp.status_code == 200
        report = resp.json()

        resp = client.get(
            f"/api/backtest/runs/{report['run_id']}",
            params={"account_id": ACCOUNT_B},
        )
        assert resp.status_code == 404

        resp = client.get(
            f"/api/backtest/snapshots/{report['snapshot_id']}",
            params={"account_id": ACCOUNT_B},
        )
        assert resp.status_code == 404

        resp = client.get(
            "/api/backtest/runs", params={"account_id": ACCOUNT_B}
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_start_run_requires_stock_or_snapshot(self, client):
        """既不给快照也不给股票代码时返回参数错误。"""
        resp = client.post("/api/backtest/runs", json={
            "account_id": ACCOUNT_A,
            "name": "缺参数",
        })
        assert resp.status_code == 400

    def test_list_runs_scoped_by_account(self, client):
        """运行列表按账户隔离。"""
        seed_candles("900203")
        client.post("/api/backtest/runs", json={
            "account_id": ACCOUNT_A,
            "name": "列表测试",
            "stock_code": "900203",
            "start_date": "2024-01-01",
            "end_date": "2024-02-09",
        })
        resp = client.get(
            "/api/backtest/runs", params={"account_id": ACCOUNT_A}
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        assert resp.json()["runs"][0]["name"] == "列表测试"
