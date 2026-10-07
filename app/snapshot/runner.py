"""快照化回测编排。

把"采集输入 -> 封存快照 -> 执行 -> 归档"串成一条可恢复、可重放的流水线：

- :meth:`build_snapshot` 分四个阶段落盘（market/strategy/calendar/fees），
  任一阶段失败后可用 :meth:`resume_build` 断点续建，已完成阶段不重跑；
- 封存后的快照内容寻址、只读，行情补录或参数改写只会产生**新快照**；
- :meth:`execute_run` 只从快照读输入，确定性执行；失败/取消后可用同一快照
  续跑（:meth:`retry_run`），也可发起一次重放（:meth:`replay_run`）；
- 所有方法都强制带 ``account_id``，快照/运行按账户目录隔离。
"""

import json
import os
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import db_session_scope, BASE_DIR
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.backtest.engine import (
    BacktestCancelled,
    BacktestConfig,
    BacktestEngine,
)
from app.backtest.report import BacktestReportGenerator
from app.backtest.strategy_pipeline import DETECTOR_CHAIN, compute_signals
from app.chan.models import RawCandle
from app.services.stock_service import StockService
from app.snapshot.fingerprint import (
    STRATEGY_CODE_VERSION,
    fingerprint_calendar,
    fingerprint_candles,
    fingerprint_fees,
    fingerprint_strategy,
)
from app.snapshot.models import CompletenessStatus, RunStatus
from app.snapshot.params import normalize_strategy_params
from app.snapshot.store import SnapshotNotFound, SnapshotStore
from app.utils.validators import (
    validate_period,
    validate_stock_code,
    validate_time_range,
)
from app.middleware.exception_handler import AnalysisException

logger = logging.getLogger(__name__)

ARCHIVE_ROOT = Path(
    os.getenv("BACKTEST_ARCHIVE_DIR", str(BASE_DIR / "data" / "archive"))
)

# 日线内部缺口超过该天数（自然日）即判定区间内缺数据（已含周末缓冲）
_DAILY_GAP_TOLERANCE_DAYS = 4
_HEAD_TAIL_TOLERANCE_DAYS = 4


@dataclass
class BuildRequest:
    """构建快照的原始请求（先落盘，保证续建无需调用方重传参数）。"""

    account_id: str
    stock_code: str
    period: str
    start_date: str
    end_date: str
    params: Dict[str, Any]

    def to_json(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "stock_code": self.stock_code,
            "period": self.period,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "params": self.params,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BuildRequest":
        return cls(
            account_id=data["account_id"],
            stock_code=data["stock_code"],
            period=data["period"],
            start_date=data["start_date"],
            end_date=data["end_date"],
            params=data.get("params", {}),
        )


class SnapshotBacktestRunner:
    """快照化回测的唯一编排入口。"""

    def __init__(
        self,
        store: Optional[SnapshotStore] = None,
        stock_service: Optional[StockService] = None,
        archive_root: Optional[Path] = None,
        engine_factory: Optional[Any] = None,
    ):
        self.store = store or SnapshotStore(archive_root or ARCHIVE_ROOT)
        self.stock_service = stock_service or StockService()
        self.report_generator = BacktestReportGenerator()
        # 可注入引擎工厂（测试用来挂取消点/故障）
        self._engine_factory = engine_factory

    def _new_engine(self):
        return self._engine_factory() if self._engine_factory else BacktestEngine()

    def _compute_signals(self, stock_code: str, period: str, candles):
        """信号计算入口，测试可覆写以注入故障。"""
        return compute_signals(stock_code, period, candles)

    # ------------------------------------------------------------------ #
    # 快照构建（分阶段、可续建）
    # ------------------------------------------------------------------ #

    def build_snapshot(
        self,
        account_id: str,
        stock_code: str,
        period: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """新建构建现场并逐阶段采集，最后封存；返回快照锁定信息。"""
        req = self._make_build_request(
            account_id, stock_code, period, start_date, end_date, params
        )
        build_id = self.store.new_build_id()
        # 请求先单独落盘，保证失败后续建无需调用方重传参数
        self._save_build_request(account_id, build_id, req)
        return self._collect_and_seal(account_id, build_id, req)

    def resume_build(self, account_id: str, build_id: str) -> Dict[str, Any]:
        """断点续建：读取构建现场，补齐缺失阶段后封存。"""
        state = self.store.load_build(account_id, build_id)
        if state["sealed_snapshot_id"]:
            manifest = self.store.load_snapshot(
                account_id, state["sealed_snapshot_id"]
            )
            return manifest.locked_inputs()
        req = self._load_build_request(account_id, build_id)
        return self._collect_and_seal(account_id, build_id, req)

    def list_builds(self, account_id: str) -> List[Dict[str, Any]]:
        return self.store.list_builds(account_id)

    def _collect_and_seal(
        self, account_id: str, build_id: str, req: BuildRequest
    ) -> Dict[str, Any]:
        start_dt = datetime.fromisoformat(req.start_date)
        end_dt = datetime.fromisoformat(req.end_date)

        # 阶段顺序：strategy/fees 不依赖外部数据先落盘，market 最易失败单独采集，
        # calendar 依赖 market。已存在的阶段不重跑（失败后继续）。
        state = self.store.load_build(account_id, build_id)
        present = set(state["stages_present"])

        if "strategy" not in present:
            strategy_part = self._capture_strategy(req.params)
            self.store.save_build_part(account_id, build_id, "strategy", strategy_part)

        if "fees" not in present:
            fees_part = self._capture_fees(req.params)
            self.store.save_build_part(account_id, build_id, "fees", fees_part)

        if "market" not in present:
            market_part = self._capture_market(
                req.stock_code, req.period, start_dt, end_dt
            )
            self.store.save_build_part(account_id, build_id, "market", market_part)

        # 重新加载以拿到 market/calendar
        state = self.store.load_build(account_id, build_id)
        if "calendar" not in present:
            market = state["parts"]["market"]
            calendar_part, completeness, detail = self._capture_calendar(
                req.stock_code, req.period, market, start_dt, end_dt
            )
            self.store.save_build_part(account_id, build_id, "calendar", calendar_part)
        else:
            # 续建时完整性可从已锁定的交易日历确定性重算
            cal = state["parts"]["calendar"]
            completeness, detail = self._assess_completeness(
                req.period, cal["trading_days"], start_dt, end_dt
            )

        manifest = self.store.seal_build(
            account_id,
            build_id,
            completeness=completeness,
            completeness_detail=detail,
        )
        return manifest.locked_inputs()

    # -- 各输入阶段采集 ---------------------------------------------------- #

    def _capture_market(
        self,
        stock_code: str,
        period: str,
        start_dt: datetime,
        end_dt: datetime,
    ) -> Dict[str, Any]:
        """采集实际参与计算的行情片段（优先缓存，否则拉数据源并回填缓存）。"""
        cached = self.stock_service._get_from_cache(
            stock_code, period, start_dt, end_dt
        )
        if cached:
            candles, source = cached, "cache"
        else:
            fetched = self.stock_service._fetch_from_source(
                stock_code, period, start_dt, end_dt
            )
            if not fetched.success or not fetched.candles:
                raise AnalysisException(
                    message="No candle data available for backtest snapshot",
                    stock_code=stock_code,
                    period=period,
                )
            candles = fetched.candles
            source = fetched.source
            self.stock_service._save_to_cache(candles, stock_code, period)

        frozen = [self._candle_to_dict(c) for c in sorted(candles, key=lambda c: c.timestamp)]
        return {
            "stock_code": stock_code,
            "period": period,
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "candles": frozen,
            "candle_count": len(frozen),
            "first_bar_time": frozen[0]["timestamp"] if frozen else None,
            "last_bar_time": frozen[-1]["timestamp"] if frozen else None,
            "content_hash": fingerprint_candles(candles),
            "source": source,
            "captured_at": datetime.utcnow().isoformat() + "Z",
        }

    def _capture_strategy(self, raw_params: Dict[str, Any]) -> Dict[str, Any]:
        normalized, aliases = normalize_strategy_params(raw_params or {})
        params_hash = fingerprint_strategy(normalized)
        return {
            "code_version": STRATEGY_CODE_VERSION,
            "params": normalized,
            "param_aliases": aliases,
            "detector_chain": list(DETECTOR_CHAIN),
            "params_hash": params_hash,
        }

    def _capture_fees(self, raw_params: Dict[str, Any]) -> Dict[str, Any]:
        normalized, _ = normalize_strategy_params(raw_params or {})
        fees = {
            "initial_capital": float(normalized["initial_capital"]),
            "position_size": float(normalized["position_size"]),
            "commission_rate": float(normalized["commission_rate"]),
            "slippage": float(normalized["slippage"]),
            "fee_model": "bilateral_commission_v1",
        }
        fees_hash = fingerprint_fees(fees)
        return {**fees, "fees_hash": fees_hash}

    def _capture_calendar(
        self,
        stock_code: str,
        period: str,
        market_part: Dict[str, Any],
        start_dt: datetime,
        end_dt: datetime,
    ) -> tuple:
        """从锁定 K 线推导交易日历，并核验声明区间的覆盖完整性。"""
        days = sorted({c["timestamp"][:10] for c in market_part["candles"]})
        calendar_hash = fingerprint_calendar(days)
        calendar_part = {
            "exchange": self._infer_exchange(stock_code),
            "trading_days": days,
            "day_count": len(days),
            "declared_start": start_dt.date().isoformat(),
            "declared_end": end_dt.date().isoformat(),
            "calendar_hash": calendar_hash,
        }
        completeness, detail = self._assess_completeness(
            period, days, start_dt, end_dt
        )
        return calendar_part, completeness, detail

    @staticmethod
    def _assess_completeness(
        period: str,
        trading_days: List[str],
        start_dt: datetime,
        end_dt: datetime,
    ) -> tuple:
        """对照声明区间核验：头部缺口、尾部缺口、区间内部缺口。"""
        if not trading_days:
            return (
                CompletenessStatus.PARTIAL.value,
                {"reason": "no_trading_days", "missing_ranges": []},
            )

        from datetime import datetime as _dt

        def _d(s: str):
            return _dt.strptime(s, "%Y-%m-%d").date()

        declared_start = start_dt.date()
        # 只能核验到"今天"为止；未来的声明终点不应当作缺口
        declared_end = min(end_dt.date(), datetime.utcnow().date())

        first_day = _d(trading_days[0])
        last_day = _d(trading_days[-1])
        missing_ranges: List[Dict[str, str]] = []

        if (first_day - declared_start).days > _HEAD_TAIL_TOLERANCE_DAYS:
            missing_ranges.append(
                {
                    "kind": "head",
                    "from": declared_start.isoformat(),
                    "to": first_day.isoformat(),
                }
            )
        if (declared_end - last_day).days > _HEAD_TAIL_TOLERANCE_DAYS:
            missing_ranges.append(
                {
                    "kind": "tail",
                    "from": last_day.isoformat(),
                    "to": declared_end.isoformat(),
                }
            )

        if period == "daily":
            for prev, cur in zip(trading_days, trading_days[1:]):
                gap = (_d(cur) - _d(prev)).days
                if gap > _DAILY_GAP_TOLERANCE_DAYS:
                    missing_ranges.append(
                        {
                            "kind": "internal",
                            "from": prev,
                            "to": cur,
                            "calendar_days": gap,
                        }
                    )

        detail = {
            "declared_range": [declared_start.isoformat(), declared_end.isoformat()],
            "actual_range": [trading_days[0], trading_days[-1]],
            "trading_day_count": len(trading_days),
            "missing_ranges": missing_ranges,
            "period": period,
        }
        if missing_ranges:
            return CompletenessStatus.PARTIAL.value, detail
        return CompletenessStatus.COMPLETE.value, detail

    # ------------------------------------------------------------------ #
    # 运行：启动 / 执行 / 取消 / 续跑 / 重放
    # ------------------------------------------------------------------ #

    def start_run(
        self,
        account_id: str,
        snapshot_id: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        run = self.store.create_run(
            account_id,
            snapshot_id,
            idempotency_key=idempotency_key,
        )
        return run.to_dict()

    def execute_run(
        self,
        account_id: str,
        run_id: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """执行一次运行。全部输入只来自锁定快照，结果一次性归档。"""
        run = self.store.get_run(account_id, run_id)
        snapshot_id = run.snapshot_id

        if run.status == RunStatus.COMPLETED.value:
            # 已完成：直接回放归档报告（天然幂等）
            return self.get_run_report(account_id, run_id)

        run = self.store.transition_run(account_id, run_id, RunStatus.RUNNING)
        manifest = self.store.load_snapshot(account_id, snapshot_id)

        try:
            candles_raw = self.store.load_snapshot_candles(account_id, snapshot_id)
            candles = [self._dict_to_candle(c) for c in candles_raw]

            # 信号只由锁定 K 线离线算出，不读可变的分析结果表
            signals = self._compute_signals(
                manifest.market.stock_code, manifest.market.period, candles
            )

            fees = manifest.fees
            config = BacktestConfig(
                stock_code=manifest.market.stock_code,
                period=manifest.market.period,
                start_date=datetime.fromisoformat(manifest.market.start_date),
                end_date=datetime.fromisoformat(manifest.market.end_date),
                initial_capital=fees.initial_capital,
                position_size=fees.position_size,
                commission_rate=fees.commission_rate,
                slippage=fees.slippage,
            )

            engine = self._new_engine()
            result = engine.run(
                config,
                candles,
                signals,
                should_cancel=lambda: self.store.is_cancel_requested(
                    account_id, run_id
                ),
            )

            report = self.report_generator.generate(result)
            result_id = self._save_result_entity(
                account_id, run_id, manifest, result
            )

            result_payload = self._build_result_payload(
                run_id, manifest, result
            )
            full_report = self._assemble_report(
                run, manifest, report, result_id
            )
            result_ref = self.store.archive_result(
                account_id, run_id, result_payload, full_report
            )
            self.store.attach_result(account_id, run_id, result_ref, result_id)
            self.store.transition_run(account_id, run_id, RunStatus.COMPLETED)
            return self.get_run_report(account_id, run_id)

        except BacktestCancelled:
            logger.info("运行 %s 被取消", run_id)
            self.store.transition_run(account_id, run_id, RunStatus.CANCELLED)
            raise
        except Exception as exc:
            logger.error("运行 %s 失败: %s", run_id, exc, exc_info=True)
            self.store.transition_run(
                account_id, run_id, RunStatus.FAILED, error=str(exc)
            )
            raise

    def run_sync(
        self,
        account_id: str,
        snapshot_id: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """启动并立即执行（同步接口/测试用）。"""
        run = self.store.create_run(
            account_id, snapshot_id, idempotency_key=idempotency_key
        )
        return self.execute_run(account_id, run.run_id)

    def cancel_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        run = self.store.request_cancel(account_id, run_id)
        return run.to_dict()

    def retry_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """失败/取消后续跑：沿用同一个 run 与同一快照，attempts +1。"""
        run = self.store.get_run(account_id, run_id)
        if run.status not in (RunStatus.FAILED.value, RunStatus.CANCELLED.value):
            raise AnalysisException(
                message=f"运行状态为 {run.status}，不能续跑；请使用重放",
                stock_code="",
                period="",
            )
        return self.execute_run(account_id, run_id)

    def replay_run(
        self,
        account_id: str,
        source_run_id: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """同快照重放：基于源运行的快照创建一个新运行并执行。"""
        source = self.store.get_run(account_id, source_run_id)
        new_run = self.store.create_run(
            account_id,
            source.snapshot_id,
            replay_of=source_run_id,
            idempotency_key=idempotency_key,
        )
        return self.execute_run(account_id, new_run.run_id)

    # ------------------------------------------------------------------ #
    # 查询（按账户隔离；报告说明锁定输入与完整性）
    # ------------------------------------------------------------------ #

    def get_snapshot(self, account_id: str, snapshot_id: str) -> Dict[str, Any]:
        return self.store.load_snapshot(account_id, snapshot_id).locked_inputs()

    def list_snapshots(self, account_id: str) -> List[Dict[str, Any]]:
        return self.store.list_snapshots(account_id)

    def get_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        return self.store.get_run(account_id, run_id).to_dict()

    def list_runs(
        self, account_id: str, snapshot_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self.store.list_runs(account_id, snapshot_id)]

    def get_run_report(self, account_id: str, run_id: str) -> Dict[str, Any]:
        run = self.store.get_run(account_id, run_id)
        report = self.store.load_archived_report(account_id, run_id)
        report["run"] = run.to_dict()
        return report

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #

    def _make_build_request(
        self,
        account_id: str,
        stock_code: str,
        period: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        params: Optional[Dict[str, Any]],
    ) -> BuildRequest:
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        return BuildRequest(
            account_id=account_id,
            stock_code=stock_code,
            period=period,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            params=dict(params or {}),
        )

    def _save_build_request(
        self, account_id: str, build_id: str, req: BuildRequest
    ) -> None:
        from app.snapshot.store import atomic_write_json

        bdir = self.store._build_dir(account_id, build_id)
        bdir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(bdir / "request.json", req.to_json())

    def _load_build_request(self, account_id: str, build_id: str) -> BuildRequest:
        from app.snapshot.store import read_json

        bdir = self.store._build_dir(account_id, build_id)
        path = bdir / "request.json"
        if not path.exists():
            raise SnapshotNotFound(
                f"构建 {build_id} 缺少 request.json，无法续建"
            )
        return BuildRequest.from_json(read_json(path))

    @staticmethod
    def _infer_exchange(stock_code: str) -> str:
        if stock_code.startswith("sh"):
            return "SSE"
        if stock_code.startswith("sz"):
            return "SZSE"
        if stock_code.startswith("bj"):
            return "BSE"
        if stock_code and stock_code[0].isdigit():
            return "CN"
        return "UNKNOWN"

    @staticmethod
    def _candle_to_dict(c: RawCandle) -> Dict[str, Any]:
        return {
            "timestamp": c.timestamp.isoformat(),
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
        }

    @staticmethod
    def _dict_to_candle(d: Dict[str, Any]) -> RawCandle:
        return RawCandle(
            timestamp=datetime.fromisoformat(d["timestamp"]),
            open=float(d["open"]),
            high=float(d["high"]),
            low=float(d["low"]),
            close=float(d["close"]),
            volume=float(d["volume"]),
        )

    def _assemble_report(
        self, run, manifest, base_report: Dict[str, Any], result_id: Optional[int]
    ) -> Dict[str, Any]:
        """报告显式列出锁定输入与结果完整性。"""
        locked = manifest.locked_inputs()
        return {
            "id": result_id,
            "run_id": run.run_id,
            "replay_of": run.replay_of,
            "summary": base_report["summary"],
            "performance": base_report["performance"],
            "trades": base_report["trades"],
            "equity_curve": base_report["equity_curve"],
            "snapshot": {
                "snapshot_id": locked["snapshot_id"],
                "content_fingerprint": locked["content_fingerprint"],
                "sealed_at": locked["sealed_at"],
                "schema_version": locked["schema_version"],
                "locked_inputs": {
                    "market": locked["market"],
                    "strategy": locked["strategy"],
                    "calendar": locked["calendar"],
                    "fees": locked["fees"],
                },
            },
            "completeness": {
                "status": manifest.completeness,
                "is_complete": manifest.completeness
                == CompletenessStatus.COMPLETE.value,
                "detail": manifest.completeness_detail,
            },
        }

    def _build_result_payload(self, run_id: str, manifest, result) -> Dict[str, Any]:
        """归档的机器可读结果（不含报告格式化字符串，便于跨次比对确定性）。"""
        return {
            "run_id": run_id,
            "snapshot_id": manifest.snapshot_id,
            "content_fingerprint": manifest.content_fingerprint,
            "final_capital": result.final_capital,
            "total_return": result.total_return,
            "annual_return": result.annual_return,
            "max_drawdown": result.max_drawdown,
            "win_rate": result.win_rate,
            "profit_loss_ratio": result.profit_loss_ratio,
            "sharpe_ratio": result.sharpe_ratio,
            "total_trades": result.total_trades,
            "trades": [
                {
                    "entry_time": t.entry_time.isoformat(),
                    "entry_price": t.entry_price,
                    "entry_signal": t.entry_signal.value if t.entry_signal else None,
                    "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                    "exit_price": t.exit_price,
                    "exit_signal": t.exit_signal.value if t.exit_signal else None,
                    "shares": t.shares,
                    "profit": t.profit,
                    "profit_pct": t.profit_pct,
                    "is_closed": t.is_closed,
                }
                for t in result.trades
            ],
            "equity_curve": result.equity_curve,
        }

    def _save_result_entity(self, account_id, run_id, manifest, result) -> Optional[int]:
        """把结果写入关系库（失败不影响归档主流程，与旧行为一致）。"""
        try:
            with db_session_scope() as session:
                trades_payload = self._build_result_payload(
                    run_id, manifest, result
                )["trades"]
                entity = BacktestResultEntity(
                    stock_code=result.config.stock_code,
                    period=result.config.period,
                    start_date=result.config.start_date,
                    end_date=result.config.end_date,
                    initial_capital=result.config.initial_capital,
                    final_capital=result.final_capital,
                    total_return=result.total_return,
                    annual_return=result.annual_return,
                    max_drawdown=result.max_drawdown,
                    win_rate=result.win_rate,
                    profit_loss_ratio=result.profit_loss_ratio,
                    sharpe_ratio=result.sharpe_ratio,
                    total_trades=result.total_trades,
                    winning_trades=result.winning_trades,
                    losing_trades=result.losing_trades,
                    trades_json=json.dumps(trades_payload),
                    equity_curve_json=json.dumps(result.equity_curve),
                    status=RunStatus.COMPLETED.value,
                    completed_at=datetime.utcnow(),
                    snapshot_id=manifest.snapshot_id,
                    run_id=run_id,
                    account_id=account_id,
                    content_fingerprint=manifest.content_fingerprint,
                    completeness=manifest.completeness,
                    input_lock_json=json.dumps(manifest.locked_inputs(), ensure_ascii=False),
                )
                session.add(entity)
                session.flush()
                return entity.id
        except Exception as exc:  # 归档文件才是权威存储，库失败仅告警
            logger.warning("写入 backtest_results 失败: %s", exc)
            return None
