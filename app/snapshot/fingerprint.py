"""快照内容指纹。

所有指纹都基于 **规范化 JSON（canonical JSON）**：键按字典序排序、
分隔符固定、datetime 统一为 ISO 字符串。这样无论输入字典构造顺序如何，
相同内容必然得到相同哈希；行情补录、参数别名等在归一化之后再计算，
避免"同一含义、不同写法"产生不同快照。
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List

from app.chan.models import RawCandle

# 策略代码版本：对参与信号计算的缠论流水线源码做哈希。
# 任一检测器/处理器代码变更都会改变该版本，从而使旧快照与新运行区分开。
_CODE_VERSION_FILES = [
    "app/chan/kline_processor.py",
    "app/chan/fractal_detector.py",
    "app/chan/bi_detector.py",
    "app/chan/duan_detector.py",
    "app/chan/zhongshu_detector.py",
    "app/chan/signal_detector.py",
    "app/backtest/engine.py",
]


def canonical_json(obj: Any) -> str:
    """确定性序列化：sort_keys + 固定分隔符。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _candle_to_row(c: Any) -> List[Any]:
    """把 K 线转成定长、定序的元组行，逐根参与哈希。"""
    if isinstance(c, RawCandle):
        ts = c.timestamp
        return [
            ts.isoformat() if isinstance(ts, datetime) else str(ts),
            _num(c.open),
            _num(c.high),
            _num(c.low),
            _num(c.close),
            _num(c.volume),
        ]
    if isinstance(c, dict):
        ts = c.get("timestamp")
        return [
            ts.isoformat() if isinstance(ts, datetime) else str(ts),
            _num(c.get("open")),
            _num(c.get("high")),
            _num(c.get("low")),
            _num(c.get("close")),
            _num(c.get("volume")),
        ]
    raise TypeError(f"Unsupported candle type: {type(c)!r}")


def _num(v: Any) -> float:
    return round(float(v), 10)


def fingerprint_candles(candles: Iterable[Any]) -> str:
    """行情片段指纹：逐根 K 线规范化后哈希。

    顺序敏感（先按时间戳排序），补录/修订任何一根 K 线都会改变指纹。
    """
    rows = [_candle_to_row(c) for c in candles]
    rows.sort(key=lambda r: r[0])
    return sha256_text(canonical_json(rows))


def fingerprint_strategy(params: Dict[str, Any]) -> str:
    """策略参数指纹（参数归一化后计算）。"""
    return sha256_text(canonical_json(_normalize(params)))


def fingerprint_calendar(trading_days: Iterable[str]) -> str:
    """交易日历指纹：排序去重后的交易日集合。"""
    days = sorted({str(d)[:10] for d in trading_days})
    return sha256_text(canonical_json(days))


def fingerprint_fees(fees: Dict[str, Any]) -> str:
    """费用口径指纹。"""
    return sha256_text(canonical_json(_normalize(fees)))


def content_fingerprint(
    market_hash: str,
    strategy_hash: str,
    calendar_hash: str,
    fees_hash: str,
) -> str:
    """组合四类输入指纹得到快照内容键（取前 16 位作为可读 ID 主体）。"""
    combined = canonical_json(
        {
            "market": market_hash,
            "strategy": strategy_hash,
            "calendar": calendar_hash,
            "fees": fees_hash,
        }
    )
    return sha256_text(combined)


def _normalize(obj: Any) -> Any:
    """递归归一化：dict 按键排序、list 保持顺序、浮点数定点化。"""
    if isinstance(obj, dict):
        return {k: _normalize(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 10)
    return obj


def compute_strategy_code_version() -> str:
    """对缠论流水线 + 回测引擎源码计算版本指纹。"""
    from pathlib import Path

    base = Path(__file__).resolve().parent.parent.parent
    hasher = hashlib.sha256()
    for rel in _CODE_VERSION_FILES:
        path = base / rel
        hasher.update(rel.encode("utf-8"))
        hasher.update(path.read_bytes() if path.exists() else b"<missing>")
    return "code-" + hasher.hexdigest()[:12]


STRATEGY_CODE_VERSION = compute_strategy_code_version()
