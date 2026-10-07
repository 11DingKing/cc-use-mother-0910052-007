"""回测不可变输入快照的领域逻辑。

一次回测运行必须绑定一份不可变快照，快照冻结以下输入并分别计算内容哈希：

- 行情片段（K线序列）
- 策略版本与信号集合
- 交易日历
- 费用口径（手续费、滑点、仓位、初始资金）

所有请求参数在快照创建时完成规范化（别名解析、代码归一化），
保证同一语义输入永远映射到同一份快照，不同输入绝不共用快照。
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.chan.models import RawCandle, Signal, SignalType
from app.chan.serializer import ChanSerializer
from app.middleware.exception_handler import ValidationException
from app.utils.validators import validate_period, validate_stock_code, validate_time_range

# 策略（缠论分析管线）版本，随检测逻辑变更而升级
STRATEGY_VERSION = "chan-1.0"

# 快照ID前缀
SNAPSHOT_ID_PREFIX = "btsp_"

_serializer = ChanSerializer()


def canonical_json(obj: Any) -> str:
    """生成键序稳定的 JSON 文本，用于内容哈希。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(obj: Any) -> str:
    """对任意可 JSON 化对象计算 SHA-256 内容哈希。"""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def canonicalize_params(
    stock_code: str,
    period: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    initial_capital: float,
    position_size: float,
    commission_rate: float,
    slippage: float,
) -> Dict[str, Any]:
    """规范化回测请求参数。

    在此解析全部别名（如周期 d/day/1h）、归一化股票代码、补全默认时间范围，
    并对费用口径做边界校验。返回的规范化字典是快照内容哈希的输入之一。
    """
    stock_code = validate_stock_code(stock_code)
    period = validate_period(period)  # 别名在此归一
    start_date, end_date = validate_time_range(start_date, end_date)

    if initial_capital <= 0:
        raise ValidationException(
            message="initial_capital must be positive",
            field="initial_capital",
            value=initial_capital,
        )
    if not 0 < position_size <= 1:
        raise ValidationException(
            message="position_size must be in (0, 1]",
            field="position_size",
            value=position_size,
        )
    if not 0 <= commission_rate < 1:
        raise ValidationException(
            message="commission_rate must be in [0, 1)",
            field="commission_rate",
            value=commission_rate,
        )
    if not 0 <= slippage < 1:
        raise ValidationException(
            message="slippage must be in [0, 1)",
            field="slippage",
            value=slippage,
        )

    return {
        "stock_code": stock_code,
        "period": period,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "initial_capital": float(initial_capital),
        "position_size": float(position_size),
        "commission_rate": float(commission_rate),
        "slippage": float(slippage),
        "strategy_version": STRATEGY_VERSION,
    }


def freeze_candles(candles: List[RawCandle]) -> List[Dict[str, Any]]:
    """把行情片段冻结为可 JSON 化的规范结构（按时间升序）。"""
    ordered = sorted(candles, key=lambda c: c.timestamp)
    return [
        {
            "timestamp": c.timestamp.isoformat(),
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
        }
        for c in ordered
    ]


def thaw_candles(frozen: List[Dict[str, Any]]) -> List[RawCandle]:
    """从快照恢复行情片段。"""
    return [
        RawCandle(
            timestamp=datetime.fromisoformat(c["timestamp"]),
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c["volume"],
        )
        for c in frozen
    ]


def freeze_signals(signals: List[Signal]) -> List[Dict[str, Any]]:
    """把策略信号集合冻结为可 JSON 化的规范结构（按时间升序）。"""
    ordered = sorted(signals, key=lambda s: s.timestamp)
    return [_serializer.serialize(s) for s in ordered]


def thaw_signals(frozen: List[Dict[str, Any]]) -> List[Signal]:
    """从快照恢复策略信号集合。"""
    return [_serializer.deserialize(s, Signal) for s in frozen]


def build_calendar(candles: List[RawCandle]) -> List[str]:
    """从行情片段构建交易日历（去重后的交易日列表，升序）。"""
    days = sorted({c.timestamp.strftime("%Y-%m-%d") for c in candles})
    return days


def build_fee_policy(canonical_params: Dict[str, Any]) -> Dict[str, Any]:
    """从规范化参数中提取费用口径。"""
    return {
        "initial_capital": canonical_params["initial_capital"],
        "position_size": canonical_params["position_size"],
        "commission_rate": canonical_params["commission_rate"],
        "slippage": canonical_params["slippage"],
    }


def compute_snapshot_hash(
    candles_hash: str,
    signals_hash: str,
    calendar_hash: str,
    fee_hash: str,
    canonical_params: Dict[str, Any],
) -> str:
    """汇总各输入分量哈希，得到整份快照的内容哈希。"""
    return content_hash({
        "candles_hash": candles_hash,
        "signals_hash": signals_hash,
        "calendar_hash": calendar_hash,
        "fee_hash": fee_hash,
        "canonical_params": canonical_params,
    })


def make_snapshot_id(account_id: str, snapshot_hash: str) -> str:
    """生成快照ID。

    快照ID混入账户ID：不同账户即使输入完全相同也持有各自独立的快照，
    跨账户查询不会混用同一份快照。
    """
    digest = hashlib.sha256(f"{account_id}:{snapshot_hash}".encode("utf-8")).hexdigest()
    return f"{SNAPSHOT_ID_PREFIX}{digest[:24]}"
