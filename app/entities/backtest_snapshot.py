"""回测快照与运行记录的数据库实体。

BacktestSnapshot 是不可变的：创建后任何字段都不应再被修改，
同一账户下内容哈希相同的快照只会存在一行（get-or-create）。
BacktestRun 记录一次运行与快照的绑定关系及其生命周期状态。
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# 运行状态机：pending -> running -> completed / failed / cancelled
RUN_STATUS_PENDING = "pending"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_CANCELLED = "cancelled"

TERMINAL_STATUSES = (RUN_STATUS_COMPLETED, RUN_STATUS_FAILED, RUN_STATUS_CANCELLED)


class BacktestSnapshot(Base):
    """不可变回测输入快照。"""
    __tablename__ = "backtest_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 快照标识：btsp_ 前缀 + 账户隔离的内容摘要
    snapshot_id = Column(String(40), nullable=False, unique=True, index=True)
    account_id = Column(String(64), nullable=False, index=True)

    # 规范化后的回测范围（别名已解析）
    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)

    # 规范化请求参数（JSON）
    canonical_params_json = Column(Text, nullable=False)

    # 冻结的行情片段及其内容哈希
    candles_json = Column(Text, nullable=False)
    candle_count = Column(Integer, nullable=False, default=0)
    candles_hash = Column(String(64), nullable=False)

    # 冻结的策略信号及其内容哈希
    signals_json = Column(Text, nullable=False)
    signal_count = Column(Integer, nullable=False, default=0)
    signals_hash = Column(String(64), nullable=False)

    # 策略版本
    strategy_version = Column(String(40), nullable=False)

    # 冻结的交易日历及其内容哈希
    calendar_json = Column(Text, nullable=False)
    calendar_days = Column(Integer, nullable=False, default=0)
    calendar_hash = Column(String(64), nullable=False)

    # 冻结的费用口径及其内容哈希
    fee_policy_json = Column(Text, nullable=False)
    fee_hash = Column(String(64), nullable=False)

    # 整份快照的内容哈希
    snapshot_hash = Column(String(64), nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('account_id', 'snapshot_hash', name='uix_snapshot_account_hash'),
        Index('ix_snapshot_account_stock', 'account_id', 'stock_code'),
    )

    def __repr__(self):
        return (
            f"<BacktestSnapshot(id={self.snapshot_id}, code={self.stock_code}, "
            f"period={self.period}, candles={self.candle_count})>"
        )

    def to_dict(self) -> dict:
        """业务模块说明。"""
        return {
            "snapshot_id": self.snapshot_id,
            "account_id": self.account_id,
            "stock_code": self.stock_code,
            "period": self.period,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "candle_count": self.candle_count,
            "candles_hash": self.candles_hash,
            "signal_count": self.signal_count,
            "signals_hash": self.signals_hash,
            "strategy_version": self.strategy_version,
            "calendar_days": self.calendar_days,
            "calendar_hash": self.calendar_hash,
            "fee_hash": self.fee_hash,
            "snapshot_hash": self.snapshot_hash,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class BacktestRun(Base):
    """一次回测运行，绑定唯一一份不可变快照。"""
    __tablename__ = "backtest_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    run_id = Column(String(40), nullable=False, unique=True, index=True)
    account_id = Column(String(64), nullable=False, index=True)
    name = Column(String(128), nullable=False)

    # 绑定的快照（运行期间绝不更换）
    snapshot_id = Column(String(40), nullable=False, index=True)

    # 生命周期
    status = Column(String(20), nullable=False, default=RUN_STATUS_PENDING)
    archived = Column(Integer, nullable=False, default=0)  # 结果归档标记（归档后只读）
    attempt = Column(Integer, nullable=False, default=1)   # 第几次执行（续跑/重试递增）

    # 断点续跑状态
    checkpoint_json = Column(Text, nullable=True)
    processed_candles = Column(Integer, nullable=False, default=0)

    # 取消请求标记（运行中被置位后，引擎在下一根K线处停止）
    cancel_requested = Column(Integer, nullable=False, default=0)

    # 幂等键：并发启动去重，同一账户同一键只会有一个运行
    idempotency_key = Column(String(128), nullable=True)

    # 运行产物
    result_id = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint('account_id', 'idempotency_key', name='uix_run_account_idem'),
        Index('ix_run_account_name', 'account_id', 'name'),
        Index('ix_run_account_status', 'account_id', 'status'),
    )

    def __repr__(self):
        return (
            f"<BacktestRun(id={self.run_id}, name={self.name}, "
            f"snapshot={self.snapshot_id}, status={self.status})>"
        )

    def to_dict(self) -> dict:
        """业务模块说明。"""
        return {
            "run_id": self.run_id,
            "account_id": self.account_id,
            "name": self.name,
            "snapshot_id": self.snapshot_id,
            "status": self.status,
            "archived": bool(self.archived),
            "attempt": self.attempt,
            "processed_candles": self.processed_candles,
            "idempotency_key": self.idempotency_key,
            "result_id": self.result_id,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }
