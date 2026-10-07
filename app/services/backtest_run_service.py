"""回测运行编排服务：不可变快照 + 运行生命周期管理。

核心约束：
- 每次运行绑定且仅绑定一份不可变快照（行情片段、策略版本、交易日历、费用口径）；
- 快照创建即冻结输入并计算内容哈希，之后的行情补录、参数别名变化都不影响既有快照；
- 失败后继续、取消后重试都从原快照的断点恢复，同快照重放产生全新运行但输入完全一致；
- 并发启动通过幂等键去重，同一账户同一键只会存在一个运行；
- 所有查询按账户隔离，跨账户访问一律返回“不存在”，不泄露任何快照/运行信息。
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.exc import IntegrityError

from app.backtest.engine import (
    BacktestCancelled,
    BacktestConfig,
    BacktestEngine,
    BacktestResult,
    trade_to_dict,
)
from app.backtest.snapshot import (
    build_calendar,
    build_fee_policy,
    canonicalize_params,
    compute_snapshot_hash,
    content_hash,
    freeze_candles,
    freeze_signals,
    make_snapshot_id,
    thaw_candles,
    thaw_signals,
)
from app.config import db_session_scope
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.entities.backtest_snapshot import (
    RUN_STATUS_CANCELLED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_FAILED,
    RUN_STATUS_PENDING,
    RUN_STATUS_RUNNING,
    TERMINAL_STATUSES,
    BacktestRun,
    BacktestSnapshot,
)
from app.middleware.exception_handler import (
    AnalysisException,
    ConflictException,
    NotFoundException,
    ValidationException,
)
from app.services.analysis_service import AnalysisService
from app.services.stock_service import StockService

logger = logging.getLogger(__name__)

# 默认账户（兼容未显式传账户的旧接口）
DEFAULT_ACCOUNT_ID = "default"


def _new_run_id() -> str:
    return f"btrun_{uuid.uuid4().hex[:16]}"


def _validate_account_id(account_id: str) -> str:
    if not account_id or not account_id.strip():
        raise ValidationException(
            message="account_id cannot be empty",
            field="account_id",
            value=account_id,
        )
    account_id = account_id.strip()
    if len(account_id) > 64:
        raise ValidationException(
            message="account_id too long",
            field="account_id",
            value=account_id,
        )
    return account_id


def _validate_run_name(name: str) -> str:
    if not name or not name.strip():
        raise ValidationException(
            message="name cannot be empty",
            field="name",
            value=name,
        )
    name = name.strip()
    if len(name) > 128:
        raise ValidationException(
            message="name too long",
            field="name",
            value=name,
        )
    return name


class BacktestRunService:
    """业务模块说明。"""

    def __init__(self):
        self.stock_service = StockService()
        self.analysis_service = AnalysisService()
        # 断点监听器：在每次断点持久化之后回调（用于进度通知与故障注入测试）
        self.checkpoint_listeners: List[Callable[[Dict[str, Any]], None]] = []

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------

    def create_snapshot(
        self,
        account_id: str,
        stock_code: str,
        period: str = "daily",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        initial_capital: float = 100000.0,
        position_size: float = 1.0,
        commission_rate: float = 0.001,
        slippage: float = 0.001,
    ) -> Dict[str, Any]:
        """创建（或复用）一份不可变输入快照。

        参数在此完成规范化（别名解析），相同语义输入在同一账户下
        永远映射到同一 snapshot_id；任何输入差异都会产生不同的快照。
        """
        account_id = _validate_account_id(account_id)
        params = canonicalize_params(
            stock_code, period, start_date, end_date,
            initial_capital, position_size, commission_rate, slippage,
        )

        # 取数并冻结行情片段
        candles = self.stock_service.get_candles(
            params["stock_code"],
            params["period"],
            datetime.fromisoformat(params["start_date"]),
            datetime.fromisoformat(params["end_date"]),
        )
        if not candles:
            raise AnalysisException(
                message="No candle data available for backtest",
                stock_code=params["stock_code"],
                period=params["period"],
            )

        # 策略信号必须来自被冻结的行情片段，而不是事后的最新分析结果
        signals = self.analysis_service.analyze_candles(
            params["stock_code"], params["period"], candles
        )["signals"]

        frozen_candles = freeze_candles(candles)
        frozen_signals = freeze_signals(signals)
        calendar = build_calendar(candles)
        fee_policy = build_fee_policy(params)

        candles_hash = content_hash(frozen_candles)
        signals_hash = content_hash(frozen_signals)
        calendar_hash = content_hash(calendar)
        fee_hash = content_hash(fee_policy)
        snapshot_hash = compute_snapshot_hash(
            candles_hash, signals_hash, calendar_hash, fee_hash, params
        )
        snapshot_id = make_snapshot_id(account_id, snapshot_hash)

        try:
            with db_session_scope() as session:
                existing = self._find_snapshot_by_hash(session, account_id, snapshot_hash)
                if existing:
                    return existing.to_dict()

                entity = BacktestSnapshot(
                    snapshot_id=snapshot_id,
                    account_id=account_id,
                    stock_code=params["stock_code"],
                    period=params["period"],
                    start_date=datetime.fromisoformat(params["start_date"]),
                    end_date=datetime.fromisoformat(params["end_date"]),
                    canonical_params_json=json.dumps(params),
                    candles_json=json.dumps(frozen_candles),
                    candle_count=len(frozen_candles),
                    candles_hash=candles_hash,
                    signals_json=json.dumps(frozen_signals),
                    signal_count=len(frozen_signals),
                    signals_hash=signals_hash,
                    strategy_version=params["strategy_version"],
                    calendar_json=json.dumps(calendar),
                    calendar_days=len(calendar),
                    calendar_hash=calendar_hash,
                    fee_policy_json=json.dumps(fee_policy),
                    fee_hash=fee_hash,
                    snapshot_hash=snapshot_hash,
                )
                session.add(entity)
                session.flush()
                return entity.to_dict()
        except IntegrityError:
            # 并发创建同一快照：内容哈希唯一约束生效，复用已存在的那一份
            with db_session_scope() as session:
                existing = self._find_snapshot_by_hash(session, account_id, snapshot_hash)
                if existing:
                    return existing.to_dict()
            raise

    def get_snapshot(self, account_id: str, snapshot_id: str) -> Dict[str, Any]:
        """按账户隔离查询快照元数据（不含冻结数据本体）。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            entity = session.query(BacktestSnapshot).filter(
                BacktestSnapshot.snapshot_id == snapshot_id,
                BacktestSnapshot.account_id == account_id,
            ).first()
            if not entity:
                raise NotFoundException(
                    message="Backtest snapshot not found",
                    resource_type="BacktestSnapshot",
                    resource_id=snapshot_id,
                )
            return entity.to_dict()

    def list_snapshots(
        self,
        account_id: str,
        stock_code: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """按账户隔离列出快照。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            query = session.query(BacktestSnapshot).filter(
                BacktestSnapshot.account_id == account_id
            )
            if stock_code:
                query = query.filter(BacktestSnapshot.stock_code == stock_code.strip())
            rows = query.order_by(BacktestSnapshot.created_at.desc()).limit(limit).all()
            return [r.to_dict() for r in rows]

    # ------------------------------------------------------------------
    # 运行生命周期
    # ------------------------------------------------------------------

    def start_run(
        self,
        account_id: str,
        name: str,
        snapshot_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        **snapshot_params,
    ) -> Dict[str, Any]:
        """启动一次回测运行（同步执行完成后返回报告）。

        - 指定 snapshot_id 时直接绑定该快照；否则先按参数创建快照；
        - idempotency_key 用于并发启动去重：同账户同键只会有一个运行，
          且该键绑定的快照不可更换。
        """
        account_id = _validate_account_id(account_id)
        name = _validate_run_name(name)

        if snapshot_id:
            snapshot = self.get_snapshot(account_id, snapshot_id)
        else:
            snapshot = self.create_snapshot(account_id=account_id, **snapshot_params)

        if idempotency_key:
            existing = self._find_run_by_idempotency_key(account_id, idempotency_key)
            if existing:
                if existing["snapshot_id"] != snapshot["snapshot_id"]:
                    raise ConflictException(
                        message="幂等键已被其他快照的运行占用，不能混用快照",
                        resource_type="BacktestRun",
                        resource_id=existing["run_id"],
                    )
                return self.get_run_report(account_id, existing["run_id"])

        run_id = _new_run_id()
        try:
            with db_session_scope() as session:
                session.add(BacktestRun(
                    run_id=run_id,
                    account_id=account_id,
                    name=name,
                    snapshot_id=snapshot["snapshot_id"],
                    status=RUN_STATUS_PENDING,
                    idempotency_key=idempotency_key,
                ))
                session.flush()
        except IntegrityError:
            # 并发启动同一幂等键：返回先创建的那个运行，绝不另起运行混用快照
            existing = self._find_run_by_idempotency_key(account_id, idempotency_key)
            if existing:
                if existing["snapshot_id"] != snapshot["snapshot_id"]:
                    raise ConflictException(
                        message="幂等键已被其他快照的运行占用，不能混用快照",
                        resource_type="BacktestRun",
                        resource_id=existing["run_id"],
                    )
                return self.get_run_report(account_id, existing["run_id"])
            raise

        self._execute_run(run_id, resume=False)
        return self.get_run_report(account_id, run_id)

    def resume_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """失败后继续 / 取消后重试：从断点恢复，仍使用原快照。"""
        account_id = _validate_account_id(account_id)
        run = self._get_run_scoped(account_id, run_id)

        if run["archived"]:
            raise ConflictException(
                message="已归档的回测运行不可续跑",
                resource_type="BacktestRun",
                resource_id=run_id,
            )
        if run["status"] not in (RUN_STATUS_PENDING, RUN_STATUS_FAILED, RUN_STATUS_CANCELLED):
            raise ConflictException(
                message=f"仅待启动、失败或已取消的运行可以续跑，当前状态为 {run['status']}",
                resource_type="BacktestRun",
                resource_id=run_id,
            )

        self._execute_run(run_id, resume=True)
        return self.get_run_report(account_id, run_id)

    def replay_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """同快照重放：创建全新运行，绑定与原运行完全相同的快照。"""
        account_id = _validate_account_id(account_id)
        run = self._get_run_scoped(account_id, run_id)

        new_run_id = _new_run_id()
        with db_session_scope() as session:
            session.add(BacktestRun(
                run_id=new_run_id,
                account_id=account_id,
                name=run["name"],
                snapshot_id=run["snapshot_id"],
                status=RUN_STATUS_PENDING,
                idempotency_key=None,
            ))
            session.flush()

        self._execute_run(new_run_id, resume=False)
        return self.get_run_report(account_id, new_run_id)

    def cancel_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """请求取消运行。运行中的引擎会在下一根K线处停止并保留断点。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            run = self._get_run_row_scoped(session, account_id, run_id)

            if run.archived:
                raise ConflictException(
                    message="已归档的回测运行不可取消",
                    resource_type="BacktestRun",
                    resource_id=run_id,
                )
            if run.status in TERMINAL_STATUSES:
                raise ConflictException(
                    message=f"运行已结束（{run.status}），无法取消",
                    resource_type="BacktestRun",
                    resource_id=run_id,
                )
            if run.status == RUN_STATUS_PENDING:
                run.status = RUN_STATUS_CANCELLED
                run.completed_at = datetime.utcnow()
            else:
                run.cancel_requested = 1

        return self.get_run_report(account_id, run_id)

    def archive_run(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """归档已完成运行的结果。归档后运行只读，不可取消或续跑。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            run = self._get_run_row_scoped(session, account_id, run_id)

            if run.status != RUN_STATUS_COMPLETED:
                raise ConflictException(
                    message=f"仅已完成的运行可以归档，当前状态为 {run.status}",
                    resource_type="BacktestRun",
                    resource_id=run_id,
                )
            run.archived = 1

        return self.get_run_report(account_id, run_id)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_run_report(self, account_id: str, run_id: str) -> Dict[str, Any]:
        """运行报告：明确列出被锁定的输入（各分量哈希）以及结果是否完整。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            run = self._get_run_row_scoped(session, account_id, run_id)
            snapshot = session.query(BacktestSnapshot).filter(
                BacktestSnapshot.snapshot_id == run.snapshot_id,
                BacktestSnapshot.account_id == account_id,
            ).first()
            if not snapshot:
                raise NotFoundException(
                    message="Backtest snapshot not found",
                    resource_type="BacktestSnapshot",
                    resource_id=run.snapshot_id,
                )

            result_block = None
            if run.result_id:
                entity = session.query(BacktestResultEntity).filter(
                    BacktestResultEntity.id == run.result_id
                ).first()
                if entity:
                    result_block = {
                        "result_id": entity.id,
                        "final_capital": entity.final_capital,
                        "total_return": entity.total_return,
                        "annual_return": entity.annual_return,
                        "max_drawdown": entity.max_drawdown,
                        "sharpe_ratio": entity.sharpe_ratio,
                        "win_rate": entity.win_rate,
                        "profit_loss_ratio": entity.profit_loss_ratio,
                        "total_trades": entity.total_trades,
                        "winning_trades": entity.winning_trades,
                        "losing_trades": entity.losing_trades,
                    }

            run_dict = run.to_dict()
            snapshot_dict = snapshot.to_dict()
            canonical_params = json.loads(snapshot.canonical_params_json)
            fee_policy = json.loads(snapshot.fee_policy_json)

        expected = snapshot_dict["candle_count"]
        processed = run_dict["processed_candles"]
        is_complete = (
            run_dict["status"] == RUN_STATUS_COMPLETED and processed == expected
        )

        return {
            **run_dict,
            "is_complete": is_complete,
            "completeness": {
                "is_complete": is_complete,
                "processed_candles": processed,
                "expected_candles": expected,
                "reason": self._incomplete_reason(run_dict, processed, expected),
            },
            "inputs_locked": {
                "locked": True,
                "snapshot_id": snapshot_dict["snapshot_id"],
                "snapshot_hash": snapshot_dict["snapshot_hash"],
                "market_data": {
                    "candles_hash": snapshot_dict["candles_hash"],
                    "candle_count": snapshot_dict["candle_count"],
                    "start_date": snapshot_dict["start_date"],
                    "end_date": snapshot_dict["end_date"],
                },
                "strategy": {
                    "strategy_version": snapshot_dict["strategy_version"],
                    "signals_hash": snapshot_dict["signals_hash"],
                    "signal_count": snapshot_dict["signal_count"],
                },
                "calendar": {
                    "calendar_hash": snapshot_dict["calendar_hash"],
                    "trading_days": snapshot_dict["calendar_days"],
                },
                "fee_policy": {
                    "fee_hash": snapshot_dict["fee_hash"],
                    **fee_policy,
                },
                "canonical_params": canonical_params,
            },
            "result": result_block,
        }

    def list_runs(
        self,
        account_id: str,
        name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """按账户隔离列出运行。"""
        account_id = _validate_account_id(account_id)
        with db_session_scope() as session:
            query = session.query(BacktestRun).filter(
                BacktestRun.account_id == account_id
            )
            if name:
                query = query.filter(BacktestRun.name == name.strip())
            if status:
                query = query.filter(BacktestRun.status == status)
            rows = query.order_by(BacktestRun.created_at.desc()).limit(limit).all()
            return [r.to_dict() for r in rows]

    # ------------------------------------------------------------------
    # 运行执行（内部）
    # ------------------------------------------------------------------

    def _execute_run(self, run_id: str, resume: bool) -> None:
        """执行引擎：从快照解冻输入，逐K线写断点，支持取消与失败续跑。"""
        with db_session_scope() as session:
            run = session.query(BacktestRun).filter(
                BacktestRun.run_id == run_id
            ).first()
            if not run:
                raise NotFoundException(
                    message="Backtest run not found",
                    resource_type="BacktestRun",
                    resource_id=run_id,
                )
            snapshot = session.query(BacktestSnapshot).filter(
                BacktestSnapshot.snapshot_id == run.snapshot_id
            ).first()
            if not snapshot:
                raise NotFoundException(
                    message="Backtest snapshot not found",
                    resource_type="BacktestSnapshot",
                    resource_id=run.snapshot_id,
                )

            # 在会话内取出全部所需数据（提交后实体会过期）
            candles = thaw_candles(json.loads(snapshot.candles_json))
            signals = thaw_signals(json.loads(snapshot.signals_json))
            params = json.loads(snapshot.canonical_params_json)
            fee_policy = json.loads(snapshot.fee_policy_json)
            total_candles = snapshot.candle_count
            snapshot_id = snapshot.snapshot_id
            resume_state = (
                json.loads(run.checkpoint_json)
                if resume and run.checkpoint_json
                else None
            )

            run.status = RUN_STATUS_RUNNING
            run.started_at = datetime.utcnow()
            run.completed_at = None
            run.error_message = None
            run.cancel_requested = 0
            if resume:
                run.attempt = (run.attempt or 1) + 1

        config = BacktestConfig(
            stock_code=params["stock_code"],
            period=params["period"],
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            initial_capital=fee_policy["initial_capital"],
            position_size=fee_policy["position_size"],
            commission_rate=fee_policy["commission_rate"],
            slippage=fee_policy["slippage"],
        )

        engine = BacktestEngine()
        try:
            result = engine.run(
                config,
                candles,
                signals,
                resume_state=resume_state,
                checkpoint_callback=lambda state: self._on_checkpoint(run_id, state),
                should_cancel=lambda: self._is_cancel_requested(run_id),
            )
        except BacktestCancelled:
            logger.info(f"Backtest run {run_id} cancelled")
            with db_session_scope() as session:
                run = session.query(BacktestRun).filter(
                    BacktestRun.run_id == run_id
                ).first()
                run.status = RUN_STATUS_CANCELLED
                run.completed_at = datetime.utcnow()
            return
        except Exception as e:
            logger.error(f"Backtest run {run_id} failed: {e}", exc_info=True)
            with db_session_scope() as session:
                run = session.query(BacktestRun).filter(
                    BacktestRun.run_id == run_id
                ).first()
                run.status = RUN_STATUS_FAILED
                run.error_message = str(e)
                run.completed_at = datetime.utcnow()
            return

        # 成功：结果与运行状态在同一事务内落库
        with db_session_scope() as session:
            entity = self._build_result_entity(result, run_id, snapshot_id, params)
            session.add(entity)
            session.flush()
            result_id = entity.id

            run = session.query(BacktestRun).filter(
                BacktestRun.run_id == run_id
            ).first()
            run.status = RUN_STATUS_COMPLETED
            run.result_id = result_id
            run.processed_candles = total_candles
            run.completed_at = datetime.utcnow()

    def _on_checkpoint(self, run_id: str, state: Dict[str, Any]) -> None:
        """持久化断点，然后通知监听器（监听器异常会使运行失败）。"""
        with db_session_scope() as session:
            run = session.query(BacktestRun).filter(
                BacktestRun.run_id == run_id
            ).first()
            if run:
                run.checkpoint_json = json.dumps(state)
                run.processed_candles = state["next_index"]
        for listener in self.checkpoint_listeners:
            listener(state)

    def _is_cancel_requested(self, run_id: str) -> bool:
        with db_session_scope() as session:
            run = session.query(BacktestRun).filter(
                BacktestRun.run_id == run_id
            ).first()
            return bool(run and run.cancel_requested)

    @staticmethod
    def _build_result_entity(
        result: BacktestResult,
        run_id: str,
        snapshot_id: str,
        canonical_params: Dict[str, Any],
    ) -> BacktestResultEntity:
        """业务模块说明。"""
        return BacktestResultEntity(
            stock_code=result.config.stock_code,
            period=result.config.period,
            start_date=result.config.start_date,
            end_date=result.config.end_date,
            initial_capital=result.config.initial_capital,
            strategy_params_json=json.dumps({
                "snapshot_id": snapshot_id,
                "run_id": run_id,
                "canonical_params": canonical_params,
            }),
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
            trades_json=json.dumps([trade_to_dict(t) for t in result.trades]),
            equity_curve_json=json.dumps(result.equity_curve),
            status="completed",
            completed_at=datetime.utcnow(),
        )

    # ------------------------------------------------------------------
    # 行级查询辅助（账户隔离）
    # ------------------------------------------------------------------

    @staticmethod
    def _find_snapshot_by_hash(session, account_id: str, snapshot_hash: str):
        return session.query(BacktestSnapshot).filter(
            BacktestSnapshot.account_id == account_id,
            BacktestSnapshot.snapshot_hash == snapshot_hash,
        ).first()

    @staticmethod
    def _find_run_by_idempotency_key(
        account_id: str, idempotency_key: str
    ) -> Optional[Dict[str, Any]]:
        with db_session_scope() as session:
            run = session.query(BacktestRun).filter(
                BacktestRun.account_id == account_id,
                BacktestRun.idempotency_key == idempotency_key,
            ).first()
            return run.to_dict() if run else None

    @staticmethod
    def _get_run_row_scoped(session, account_id: str, run_id: str) -> BacktestRun:
        run = session.query(BacktestRun).filter(
            BacktestRun.run_id == run_id,
            BacktestRun.account_id == account_id,
        ).first()
        if not run:
            raise NotFoundException(
                message="Backtest run not found",
                resource_type="BacktestRun",
                resource_id=run_id,
            )
        return run

    def _get_run_scoped(self, account_id: str, run_id: str) -> Dict[str, Any]:
        with db_session_scope() as session:
            return self._get_run_row_scoped(session, account_id, run_id).to_dict()

    @staticmethod
    def _incomplete_reason(
        run: Dict[str, Any], processed: int, expected: int
    ) -> Optional[str]:
        if run["status"] == RUN_STATUS_COMPLETED and processed == expected:
            return None
        if run["status"] == RUN_STATUS_FAILED:
            return f"运行失败（已处理 {processed}/{expected} 根K线）：{run['error_message']}"
        if run["status"] == RUN_STATUS_CANCELLED:
            return f"运行已取消（已处理 {processed}/{expected} 根K线）"
        if run["status"] in (RUN_STATUS_PENDING, RUN_STATUS_RUNNING):
            return f"运行尚未完成（已处理 {processed}/{expected} 根K线）"
        return f"结果不完整（已处理 {processed}/{expected} 根K线）"
