"""不可变输入快照特性测试。

覆盖：
- 四类输入（行情/策略版本/交易日历/费用）被锁定且封存后不可改；
- 内容寻址：同输入同快照、行情补录产生新快照且旧快照不变；
- 参数别名归一化后锁定，冲突参数被拒绝；
- 失败后继续（构建断点续建、运行 FAILED 后续跑）；
- 同快照重放结果确定性一致；
- 取消后续跑；并发启动幂等；跨账户严格隔离；
- 结果一次性归档、报告标注锁定输入与完整性。
"""

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.backtest.engine import BacktestCancelled, BacktestEngine
from app.backtest.strategy_pipeline import compute_signals
from app.chan.models import RawCandle
from app.data.fetcher import FetchResult
from app.entities.backtest import Base as BacktestBase
from app.snapshot.fingerprint import fingerprint_candles
from app.snapshot.models import RunStatus
from app.snapshot.params import (
    ParameterConflictError,
    normalize_strategy_params,
)
from app.snapshot.runner import SnapshotBacktestRunner
from app.snapshot.store import (
    SnapshotAccessDenied,
    SnapshotAlreadySealed,
    SnapshotStore,
)
from app.config import db_session_scope as _real_scope  # noqa: F401


# --------------------------------------------------------------------------- #
# 测试替身
# --------------------------------------------------------------------------- #


def _candle(day: int, price: float = 100.0) -> RawCandle:
    return RawCandle(
        timestamp=datetime(2024, 1, 1) + timedelta(days=day),
        open=price * 0.99,
        high=price * 1.02,
        low=price * 0.98,
        close=price,
        volume=1000.0,
    )


def _rising_candles(n: int = 60) -> list:
    return [_candle(i, 100 + i * 0.5) for i in range(n)]


class FakeStockService:
    """不触网、不依赖全局库的行情替身。"""

    def __init__(self, candles=None, source: str = "akshare", fail_times: int = 0):
        self.candles = candles if candles is not None else _rising_candles()
        self.source = source
        self.fail_times = fail_times
        self.calls = 0
        self.saved_to_cache = False

    def _get_from_cache(self, *args, **kwargs):
        return None

    def _save_to_cache(self, candles, code, period):
        self.saved_to_cache = True

    def _fetch_from_source(self, code, period, start, end):
        self.calls += 1
        if self.calls <= self.fail_times:
            return FetchResult(
                [], code, period, start, end, "none", False, "simulated outage"
            )
        return FetchResult(
            list(self.candles), code, period, start, end, self.source, True
        )


@pytest.fixture
def tmp_archive(tmp_path, monkeypatch):
    """隔离的归档目录 + 隔离的结果库。"""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app import config

    engine = create_engine(
        f"sqlite:///{tmp_path / 'results.db'}",
        connect_args={"check_same_thread": False},
    )
    BacktestBase.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    @contextmanager
    def fake_scope():
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    import app.snapshot.runner as runner_mod

    monkeypatch.setattr(runner_mod, "db_session_scope", fake_scope)
    return tmp_path


@pytest.fixture
def runner(tmp_archive):
    r = SnapshotBacktestRunner(
        store=SnapshotStore(tmp_archive / "archive"),
        stock_service=FakeStockService(),
        archive_root=tmp_archive / "archive",
    )
    return r


START = "2024-01-01"
END = "2024-03-01"


# --------------------------------------------------------------------------- #
# 1. 四类输入锁定与不可变性
# --------------------------------------------------------------------------- #


class TestSnapshotLocking:
    def test_four_inputs_locked(self, runner):
        locked = runner.build_snapshot("acct-a", "000001", "daily",
                                       datetime(2024, 1, 1), datetime(2024, 3, 1),
                                       {"commission_rate": 0.002})
        assert locked["status"] == "sealed"
        assert locked["market"]["content_hash"]
        assert locked["strategy"]["params_hash"]
        assert locked["calendar"]["calendar_hash"]
        assert locked["fees"]["fees_hash"]
        assert locked["content_fingerprint"]
        # 费用口径按传入值锁定
        assert locked["fees"]["commission_rate"] == 0.002
        # 策略版本包含代码版本与检测器链
        assert locked["strategy"]["code_version"].startswith("code-")
        assert "BacktestEngine.run" in locked["strategy"]["detector_chain"]
        # 交易日历与行情绑定
        assert locked["calendar"]["day_count"] == locked["market"]["candle_count"]

    def test_sealed_snapshot_is_immutable(self, runner):
        locked = runner.build_snapshot("acct-a", "000001", "daily",
                                       datetime(2024, 1, 1), datetime(2024, 3, 1))
        # 找到对应构建现场
        builds = runner.list_builds("acct-a")
        assert builds[0]["sealed_snapshot_id"] == locked["snapshot_id"]
        with pytest.raises(SnapshotAlreadySealed):
            runner.store.save_build_part(
                "acct-a", builds[0]["build_id"], "fees", {"tampered": True}
            )
        # 封存标记存在且只读
        snap_dir = runner.store.root.glob(
            f"accounts/acct-a/snapshots/*/SEALED"
        )
        assert list(snap_dir), "缺少 SEALED 封存标记"

    def test_fingerprint_order_independent(self):
        a = _rising_candles(30)
        b = list(reversed(a))
        assert fingerprint_candles(a) == fingerprint_candles(b)

    def test_fingerprint_changes_when_any_bar_changes(self):
        a = _rising_candles(30)
        b = [dict(timestamp=c.timestamp.isoformat(), open=c.open, high=c.high,
                  low=c.low, close=c.close + 0.01, volume=c.volume) for c in a]
        assert fingerprint_candles(a) != fingerprint_candles(b)


# --------------------------------------------------------------------------- #
# 2. 内容寻址 / 数据补录隔离
# --------------------------------------------------------------------------- #


class TestContentAddressing:
    def test_same_inputs_same_snapshot(self, runner):
        s1 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        s2 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        assert s1["snapshot_id"] == s2["snapshot_id"]
        assert len(runner.list_snapshots("acct-a")) == 1

    def test_backfilled_market_creates_new_snapshot_but_old_stays(self, runner):
        old_candles = _rising_candles(40)
        runner.stock_service = FakeStockService(old_candles, source="cache")
        s1 = runner.build_snapshot("acct-a", "600000", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        old_locked = runner.store.load_snapshot_candles(
            "acct-a", s1["snapshot_id"]
        )

        # 行情被补录（追加 + 修订一根 K 线）
        backfilled = _rising_candles(60)
        backfilled[5] = _candle(5, price=999.0)
        runner.stock_service = FakeStockService(backfilled, source="akshare")
        s2 = runner.build_snapshot("acct-a", "600000", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))

        assert s1["snapshot_id"] != s2["snapshot_id"]
        assert s1["market"]["content_hash"] != s2["market"]["content_hash"]
        # 旧快照的行情片段保持补录前的内容
        old_after = runner.store.load_snapshot_candles(
            "acct-a", s1["snapshot_id"]
        )
        assert old_after == old_locked
        assert len(old_after) == 40
        assert s2["market"]["candle_count"] == 60


# --------------------------------------------------------------------------- #
# 3. 参数别名
# --------------------------------------------------------------------------- #


class TestParamAliases:
    def test_aliases_normalize_to_canonical(self):
        params, aliases = normalize_strategy_params(
            {"comm": 0.002, "滑点": 0.003, "本金": 50000}
        )
        assert params["commission_rate"] == 0.002
        assert params["slippage"] == 0.003
        assert params["initial_capital"] == 50000
        assert aliases == {
            "comm": "commission_rate",
            "滑点": "slippage",
            "本金": "initial_capital",
        }

    def test_alias_and_canonical_produce_same_snapshot(self, runner):
        s1 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1),
                                   {"commission_rate": 0.002})
        s2 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1),
                                   {"手续费率": 0.002})
        assert s1["snapshot_id"] == s2["snapshot_id"]
        # 别名映射被记录在清单里
        assert s2["strategy"]["param_aliases"] == {"手续费率": "commission_rate"}

    def test_conflicting_aliases_rejected(self):
        with pytest.raises(ParameterConflictError):
            normalize_strategy_params({"comm": 0.002, "commission_rate": 0.003})

    def test_unknown_params_kept_in_fingerprint(self):
        p1, _ = normalize_strategy_params({"custom_alpha": 1})
        p2, _ = normalize_strategy_params({"custom_alpha": 2})
        assert p1 != p2


# --------------------------------------------------------------------------- #
# 4. 构建断点续建
# --------------------------------------------------------------------------- #


class TestBuildResume:
    def test_resume_after_market_fetch_failure(self, tmp_archive):
        store = SnapshotStore(tmp_archive / "archive")
        # 首次拉取失败，第二次成功
        flaky = FakeStockService(fail_times=1)
        r = SnapshotBacktestRunner(store=store, stock_service=flaky,
                                   archive_root=tmp_archive / "archive")

        build_id = store.new_build_id()
        # 复刻 build_snapshot 的请求落盘
        from app.snapshot.runner import BuildRequest

        req = BuildRequest(
            account_id="acct-a", stock_code="000001", period="daily",
            start_date=datetime(2024, 1, 1).isoformat(),
            end_date=datetime(2024, 3, 1).isoformat(), params={},
        )
        r._save_build_request("acct-a", build_id, req)

        with pytest.raises(Exception):
            r._collect_and_seal("acct-a", build_id, req)

        state = store.load_build("acct-a", build_id)
        assert "market" in state["stages_missing"]
        assert "strategy" in state["stages_present"]

        # 失败后继续：不重建现场，补齐缺失阶段
        locked = r.resume_build("acct-a", build_id)
        assert locked["status"] == "sealed"
        assert flaky.calls == 2

    def test_seal_is_idempotent(self, runner):
        s1 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        build_id = runner.list_builds("acct-a")[0]["build_id"]
        s2 = runner.resume_build("acct-a", build_id)
        assert s1["snapshot_id"] == s2["snapshot_id"]


# --------------------------------------------------------------------------- #
# 5. 执行：重放确定性、失败续跑、取消重试、归档
# --------------------------------------------------------------------------- #


class TestRunExecution:
    def _build(self, runner, account="acct-a"):
        return runner.build_snapshot(
            account, "000001", "daily",
            datetime(2024, 1, 1), datetime(2024, 3, 1),
        )

    def test_replay_same_snapshot_deterministic(self, runner):
        snap = self._build(runner)
        report1 = runner.run_sync("acct-a", snap["snapshot_id"])
        report2 = runner.replay_run("acct-a", report1["run_id"])

        assert report1["snapshot"]["snapshot_id"] == report2["snapshot"]["snapshot_id"]
        assert report1["run_id"] != report2["run_id"]
        assert report2["replay_of"] == report1["run_id"]
        assert report1["performance"] == report2["performance"]
        assert report1["equity_curve"] == report2["equity_curve"]

    def test_failed_run_can_continue(self, runner):
        snap = self._build(runner)
        run = runner.start_run("acct-a", snap["snapshot_id"])

        calls = {"n": 0}
        real = compute_signals

        def flaky(code, period, candles):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("信号服务暂时不可用")
            return real(code, period, candles)

        runner._compute_signals = flaky

        with pytest.raises(RuntimeError):
            runner.execute_run("acct-a", run["run_id"])
        failed = runner.get_run("acct-a", run["run_id"])
        assert failed["status"] == RunStatus.FAILED.value
        assert "信号服务" in failed["last_error"]

        # 失败后继续：同一 run、同一快照、attempts 增加
        report = runner.retry_run("acct-a", run["run_id"])
        done = runner.get_run("acct-a", run["run_id"])
        assert done["status"] == RunStatus.COMPLETED.value
        assert done["attempts"] == 2
        assert report["performance"]["total_trades"] >= 0

    def test_cancelled_run_then_retry_succeeds(self, runner):
        snap = self._build(runner)
        run = runner.start_run("acct-a", snap["snapshot_id"])

        state = {"attempt": 0}

        def factory():
            state["attempt"] += 1
            if state["attempt"] == 1:
                class _CancellingEngine:
                    def run(self, config, candles, signals, should_cancel=None):
                        assert should_cancel is not None
                        raise BacktestCancelled("用户取消")
                return _CancellingEngine()
            return BacktestEngine()

        runner._engine_factory = factory

        # 先请求取消，首个引擎在取消点停止
        runner.cancel_run("acct-a", run["run_id"])
        with pytest.raises(BacktestCancelled):
            runner.execute_run("acct-a", run["run_id"])
        assert runner.get_run("acct-a", run["run_id"])["status"] == \
            RunStatus.CANCELLED.value

        # 取消后重试：清标记、同快照续跑
        report = runner.retry_run("acct-a", run["run_id"])
        done = runner.get_run("acct-a", run["run_id"])
        assert done["status"] == RunStatus.COMPLETED.value
        assert done["cancel_requested"] is False
        assert report["snapshot"]["snapshot_id"] == snap["snapshot_id"]

    def test_cancel_pending_before_start(self, runner):
        snap = self._build(runner)
        run = runner.start_run("acct-a", snap["snapshot_id"])
        runner.cancel_run("acct-a", run["run_id"])
        assert runner.get_run("acct-a", run["run_id"])["status"] == \
            RunStatus.CANCELLED.value
        report = runner.retry_run("acct-a", run["run_id"])
        assert report["run"]["status"] == RunStatus.COMPLETED.value

    def test_completed_run_is_idempotent_and_archive_immutable(self, runner):
        snap = self._build(runner)
        report = runner.run_sync("acct-a", snap["snapshot_id"])
        again = runner.execute_run("acct-a", report["run_id"])
        assert again["run_id"] == report["run_id"]
        assert again["performance"] == report["performance"]
        with pytest.raises(SnapshotAlreadySealed):
            runner.store.archive_result(
                "acct-a", report["run_id"], {"x": 1}, {"y": 2}
            )

    def test_report_declares_locked_inputs_and_completeness(self, runner):
        snap = self._build(runner)
        report = runner.run_sync("acct-a", snap["snapshot_id"])
        assert "snapshot" in report
        locked = report["snapshot"]["locked_inputs"]
        assert {"market", "strategy", "calendar", "fees"} <= set(locked)
        comp = report["completeness"]
        assert comp["status"] in ("complete", "partial", "unverified")
        assert isinstance(comp["is_complete"], bool)
        assert "detail" in comp


# --------------------------------------------------------------------------- #
# 6. 完整性判定
# --------------------------------------------------------------------------- #


class TestCompleteness:
    def test_contiguous_range_is_complete(self, tmp_archive):
        candles = [_candle(i) for i in range(30)]
        r = SnapshotBacktestRunner(
            store=SnapshotStore(tmp_archive / "a"),
            stock_service=FakeStockService(candles),
            archive_root=tmp_archive / "a",
        )
        locked = r.build_snapshot("acct-a", "000001", "daily",
                                  datetime(2024, 1, 1), datetime(2024, 1, 30))
        assert locked["completeness"] == "complete"

    def test_internal_gap_is_partial(self, tmp_archive):
        candles = [_candle(i) for i in range(10)] + [
            _candle(i) for i in range(40, 50)
        ]
        r = SnapshotBacktestRunner(
            store=SnapshotStore(tmp_archive / "a"),
            stock_service=FakeStockService(candles),
            archive_root=tmp_archive / "a",
        )
        locked = r.build_snapshot("acct-a", "000001", "daily",
                                  datetime(2024, 1, 1), datetime(2024, 2, 20))
        assert locked["completeness"] == "partial"
        gaps = locked["completeness_detail"]["missing_ranges"]
        assert any(g["kind"] == "internal" for g in gaps)


# --------------------------------------------------------------------------- #
# 7. 并发启动幂等
# --------------------------------------------------------------------------- #


class TestConcurrentStart:
    def test_same_idempotency_key_single_run(self, runner):
        snap = runner.build_snapshot("acct-a", "000001", "daily",
                                     datetime(2024, 1, 1), datetime(2024, 3, 1))
        ids = []
        errors = []

        def worker():
            try:
                run = runner.start_run(
                    "acct-a", snap["snapshot_id"], idempotency_key="idem-001"
                )
                ids.append(run["run_id"])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(set(ids)) == 1
        assert len(ids) == 8

    def test_idempotency_key_cannot_cross_snapshot(self, runner):
        s1 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1),
                                   {"commission_rate": 0.001})
        s2 = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1),
                                   {"commission_rate": 0.002})
        runner.start_run("acct-a", s1["snapshot_id"], idempotency_key="k")
        with pytest.raises(SnapshotAlreadySealed):
            runner.start_run("acct-a", s2["snapshot_id"], idempotency_key="k")


# --------------------------------------------------------------------------- #
# 8. 跨账户隔离
# --------------------------------------------------------------------------- #


class TestAccountIsolation:
    def test_snapshot_cannot_be_read_cross_account(self, runner):
        snap = runner.build_snapshot("acct-a", "000001", "daily",
                                     datetime(2024, 1, 1), datetime(2024, 3, 1))
        with pytest.raises(SnapshotAccessDenied):
            runner.get_snapshot("acct-b", snap["snapshot_id"])
        with pytest.raises(SnapshotAccessDenied):
            runner.store.load_snapshot_candles("acct-b", snap["snapshot_id"])
        with pytest.raises(SnapshotAccessDenied):
            runner.start_run("acct-b", snap["snapshot_id"])
        assert runner.list_snapshots("acct-b") == []

    def test_run_and_replay_isolated_by_account(self, runner):
        snap = runner.build_snapshot("acct-a", "000001", "daily",
                                     datetime(2024, 1, 1), datetime(2024, 3, 1))
        report = runner.run_sync("acct-a", snap["snapshot_id"])
        # B 看不到 A 的 run，也不能重放
        with pytest.raises(Exception):
            runner.get_run("acct-b", report["run_id"])
        with pytest.raises(Exception):
            runner.replay_run("acct-b", report["run_id"])
        assert runner.list_runs("acct-b") == []
        assert len(runner.list_runs("acct-a")) == 1

    def test_same_inputs_under_different_accounts_are_distinct(self, runner):
        sa = runner.build_snapshot("acct-a", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        sb = runner.build_snapshot("acct-b", "000001", "daily",
                                   datetime(2024, 1, 1), datetime(2024, 3, 1))
        # 内容指纹一致，但快照 ID 带账户命名空间且互不可见
        assert sa["content_fingerprint"] == sb["content_fingerprint"]
        assert sa["snapshot_id"] != sb["snapshot_id"]


# --------------------------------------------------------------------------- #
# 9. HTTP 接口
# --------------------------------------------------------------------------- #


@pytest.fixture
def client_env(tmp_archive, monkeypatch):
    from app.controllers import snapshot_controller as sc
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.main import app
    from app import config

    sc.runner.store = SnapshotStore(tmp_archive / "archive")
    sc.runner.stock_service = FakeStockService()
    sc.runner._engine_factory = None

    engine = create_engine(
        f"sqlite:///{tmp_archive / 'api_results.db'}",
        connect_args={"check_same_thread": False},
    )
    BacktestBase.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    @contextmanager
    def fake_scope():
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    import app.snapshot.runner as runner_mod

    monkeypatch.setattr(runner_mod, "db_session_scope", fake_scope)

    with TestClient(app) as client:
        yield client, sc.runner


BUILD_BODY = {
    "stock_code": "000001",
    "period": "daily",
    "start_date": START,
    "end_date": END,
}


class TestSnapshotAPI:
    def test_requires_account_header(self, client_env):
        client, _ = client_env
        resp = client.post("/api/backtest/run-snapshot", json={"build": BUILD_BODY})
        assert resp.status_code == 400

    def test_build_run_replay_flow(self, client_env):
        client, _ = client_env
        headers = {"X-Account-Id": "acct-a"}

        resp = client.post(
            "/api/backtest/run-snapshot",
            headers=headers,
            json={"build": BUILD_BODY},
        )
        assert resp.status_code == 200, resp.text
        report = resp.json()
        assert report["snapshot"]["locked_inputs"]["fees"]["commission_rate"] == 0.001
        run_id = report["run_id"]
        snapshot_id = report["snapshot"]["snapshot_id"]

        # 归档报告可独立取回
        resp = client.get(
            f"/api/backtest/runs/{run_id}/report", headers=headers
        )
        assert resp.status_code == 200
        assert resp.json()["snapshot"]["snapshot_id"] == snapshot_id

        # 同快照重放
        resp = client.post(
            f"/api/backtest/runs/{run_id}/replay", headers=headers, json={}
        )
        assert resp.status_code == 201, resp.text
        replayed = resp.json()
        assert replayed["replay_of"] == run_id
        assert replayed["snapshot"]["snapshot_id"] == snapshot_id
        assert replayed["performance"] == report["performance"]

    def test_alias_params_via_api(self, client_env):
        client, _ = client_env
        body = {"build": {**BUILD_BODY, "extra_params": {"手续费率": 0.0025}}}
        resp = client.post(
            "/api/backtest/run-snapshot",
            headers={"X-Account-Id": "acct-a"},
            json=body,
        )
        assert resp.status_code == 200, resp.text
        fees = resp.json()["snapshot"]["locked_inputs"]["fees"]
        assert fees["commission_rate"] == 0.0025

    def test_cross_account_query_forbidden(self, client_env):
        client, _ = client_env
        r = client.post(
            "/api/backtest/run-snapshot",
            headers={"X-Account-Id": "acct-a"},
            json={"build": BUILD_BODY},
        )
        snapshot_id = r.json()["snapshot"]["snapshot_id"]
        run_id = r.json()["run_id"]

        assert client.get(
            f"/api/backtest/snapshots/{snapshot_id}",
            headers={"X-Account-Id": "acct-b"},
        ).status_code == 403
        assert client.get(
            f"/api/backtest/runs/{run_id}/report",
            headers={"X-Account-Id": "acct-b"},
        ).status_code in (403, 404)
        assert client.post(
            f"/api/backtest/runs/{run_id}/replay",
            headers={"X-Account-Id": "acct-b"},
            json={},
        ).status_code in (403, 404)

    def test_concurrent_starts_dedup_by_idempotency_key(self, client_env):
        client, runner = client_env
        headers = {"X-Account-Id": "acct-a"}
        snap = runner.build_snapshot(
            "acct-a", "000001", "daily",
            datetime(2024, 1, 1), datetime(2024, 3, 1),
        )
        results = []

        def call():
            resp = client.post(
                f"/api/backtest/snapshots/{snap['snapshot_id']}/runs",
                headers=headers,
                json={"idempotency_key": "api-key-1"},
            )
            results.append(resp)

        threads = [threading.Thread(target=call) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        run_ids = {r.json()["run_id"] for r in results}
        assert len(run_ids) == 1

    def test_cancel_and_retry_flow_via_api(self, client_env):
        client, runner = client_env
        headers = {"X-Account-Id": "acct-a"}
        snap = runner.build_snapshot(
            "acct-a", "000001", "daily",
            datetime(2024, 1, 1), datetime(2024, 3, 1),
        )
        r = client.post(
            f"/api/backtest/snapshots/{snap['snapshot_id']}/runs",
            headers=headers, json={},
        )
        run_id = r.json()["run_id"]

        # PENDING 状态取消
        assert client.post(
            f"/api/backtest/runs/{run_id}/cancel", headers=headers
        ).status_code == 200
        # 取消后重试成功
        r = client.post(f"/api/backtest/runs/{run_id}/retry", headers=headers)
        assert r.status_code == 200, r.text
        assert r.json()["run"]["status"] == "completed"
