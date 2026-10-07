"""回测不可变输入快照。

把一次回测运行依赖的四类输入在运行开始时固化为只读快照：

- 行情片段（实际参与计算的 K 线，逐根哈希）
- 策略版本（缠论流水线代码版本 + 策略参数）
- 交易日历（参与计算的实际交易日，区分声明区间与实际覆盖区间）
- 费用口径（手续费率、滑点等资金参数）

快照一旦封存（sealed）即不可修改，之后只能重放。
"""

from app.snapshot.models import (
    SnapshotStatus,
    RunStatus,
    MarketSlice,
    StrategyVersion,
    TradingCalendar,
    FeeSchedule,
    SnapshotManifest,
    SnapshotRun,
    CompletenessStatus,
    SNAPSHOT_SCHEMA_VERSION,
)
from app.snapshot.fingerprint import (
    fingerprint_candles,
    fingerprint_strategy,
    fingerprint_calendar,
    fingerprint_fees,
    content_fingerprint,
    canonical_json,
    STRATEGY_CODE_VERSION,
)
from app.snapshot.store import SnapshotStore, SnapshotAlreadySealed, SnapshotNotFound

__all__ = [
    "SnapshotStatus",
    "RunStatus",
    "MarketSlice",
    "StrategyVersion",
    "TradingCalendar",
    "FeeSchedule",
    "SnapshotManifest",
    "SnapshotRun",
    "CompletenessStatus",
    "SNAPSHOT_SCHEMA_VERSION",
    "SnapshotStore",
    "SnapshotAlreadySealed",
    "SnapshotNotFound",
    "fingerprint_candles",
    "fingerprint_strategy",
    "fingerprint_calendar",
    "fingerprint_fees",
    "content_fingerprint",
    "canonical_json",
    "STRATEGY_CODE_VERSION",
]
