"""快照领域模型。

所有输入分片都是 ``frozen=True`` 的 dataclass：构造后任何修改都会抛
``FrozenInstanceError``，从数据结构层面保证快照不可变。快照 ID 由四类
输入的内容指纹派生（``content_fingerprint``），相同输入必然得到同一快照，
从而支持"同快照重放"。
"""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, List, Optional

SNAPSHOT_SCHEMA_VERSION = "1.0"


class SnapshotStatus(str, Enum):
    """快照生命周期。"""

    BUILDING = "building"  # 正在收集输入，尚未封存
    SEALED = "sealed"      # 已封存：输入已锁定，可执行/重放


class RunStatus(str, Enum):
    """一次回测运行的状态。"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @classmethod
    def terminal(cls) -> "set[RunStatus]":
        return {cls.COMPLETED, cls.CANCELLED}


class CompletenessStatus(str, Enum):
    """行情覆盖完整性判定结果。"""

    COMPLETE = "complete"      # 实际行情覆盖了声明的整个区间
    PARTIAL = "partial"        # 实际行情只覆盖了部分区间（含数据补录缺口）
    UNVERIFIED = "unverified"  # 无法对照交易日历核验（如离线无日历）


@dataclass(frozen=True)
class MarketSlice:
    """行情片段：实际参与回测计算的 K 线及其内容指纹。"""

    stock_code: str
    period: str
    start_date: str                    # 声明的回测起点（ISO）
    end_date: str                      # 声明的回测终点（ISO）
    candles: List[Dict[str, Any]]     # 固化的 K 线（OHLCV + 时间戳）
    candle_count: int
    first_bar_time: Optional[str]
    last_bar_time: Optional[str]
    content_hash: str                 # 对全部 K 线规范化序列化后的哈希
    source: str = "unknown"           # 行情来源（akshare/yahoo/cache）
    captured_at: str = ""

    def to_manifest_dict(self) -> Dict[str, Any]:
        """清单中只存摘要，不重复存放 K 线本体。"""
        return {
            "stock_code": self.stock_code,
            "period": self.period,
            "declared_range": [self.start_date, self.end_date],
            "actual_range": [self.first_bar_time, self.last_bar_time],
            "candle_count": self.candle_count,
            "content_hash": self.content_hash,
            "source": self.source,
            "captured_at": self.captured_at,
        }


@dataclass(frozen=True)
class StrategyVersion:
    """策略版本：代码版本 + 归一化后的策略/信号参数。"""

    code_version: str                 # 缠论流水线代码的版本指纹
    params: Dict[str, Any]            # 已解析别名、已归一化的参数
    param_aliases: Dict[str, str]    # 本次请求实际命中的 原名 -> 规范名
    detector_chain: List[str]         # 按顺序记录参与计算的流水线组件
    params_hash: str

    def to_manifest_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradingCalendar:
    """交易日历：实际参与计算的交易日集合（与行情切片绑定）。"""

    exchange: str
    trading_days: List[str]           # 有序、去重的交易日
    day_count: int
    declared_start: str
    declared_end: str
    calendar_hash: str

    def to_manifest_dict(self) -> Dict[str, Any]:
        return {
            "exchange": self.exchange,
            "day_count": self.day_count,
            "declared_range": [self.declared_start, self.declared_end],
            "trading_days": self.trading_days,
            "calendar_hash": self.calendar_hash,
        }


@dataclass(frozen=True)
class FeeSchedule:
    """费用口径：资金/费率参数，逐笔成交据此计费。"""

    initial_capital: float
    position_size: float
    commission_rate: float
    slippage: float
    fee_model: str                    # 费用模型标识，默认 bilateral_commission
    fees_hash: str

    def to_manifest_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SnapshotManifest:
    """已封存的快照清单（输入锁的完整描述）。"""

    snapshot_id: str
    schema_version: str
    account_id: str
    created_at: str
    sealed_at: Optional[str]
    status: str
    market: MarketSlice
    strategy: StrategyVersion
    calendar: TradingCalendar
    fees: FeeSchedule
    content_fingerprint: str          # 四类输入指纹的组合哈希（= 快照去重键）
    completeness: str                 # CompletenessStatus 值
    completeness_detail: Dict[str, Any]

    def locked_inputs(self) -> Dict[str, Any]:
        """报告用：明确列出本次运行锁定了哪些输入。"""
        return {
            "snapshot_id": self.snapshot_id,
            "schema_version": self.schema_version,
            "content_fingerprint": self.content_fingerprint,
            "status": self.status,
            "sealed_at": self.sealed_at,
            "market": self.market.to_manifest_dict(),
            "strategy": self.strategy.to_manifest_dict(),
            "calendar": {
                "exchange": self.calendar.exchange,
                "day_count": self.calendar.day_count,
                "declared_range": [
                    self.calendar.declared_start,
                    self.calendar.declared_end,
                ],
                "calendar_hash": self.calendar.calendar_hash,
            },
            "fees": self.fees.to_manifest_dict(),
            "completeness": self.completeness,
            "completeness_detail": self.completeness_detail,
        }


@dataclass
class SnapshotRun:
    """绑定到某个快照的一次回测运行（支持失败继续与重放）。"""

    run_id: str
    snapshot_id: str
    account_id: str
    status: str = RunStatus.PENDING.value
    created_at: str = ""
    started_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    attempts: int = 0
    last_error: Optional[str] = None
    cancel_requested: bool = False
    result_ref: Optional[str] = None   # 归档结果引用（run 目录路径）
    result_id: Optional[int] = None   # 关联的 backtest_results 行
    replay_of: Optional[str] = None   # 重放时指向首次运行的 run_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
