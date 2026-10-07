"""策略参数归一化与别名解析。

同一次回测可能以不同写法传入等价参数（``手续费率``/``commission_rate``、
``comm``、``滑点``/``slippage``）。快照必须锁定**归一化后**的口径，并记录
本次实际命中的别名映射，报告中可追溯"原名 -> 规范名"。

别名只在**入快照明细**时解析；已封存快照内的规范参数不受后续别名表
变更影响。
"""

from typing import Any, Dict, Tuple

# 规范参数名 -> 可接受的别名（全部小写后匹配）
PARAM_ALIASES: Dict[str, Tuple[str, ...]] = {
    "commission_rate": (
        "commission_rate",
        "commission",
        "comm",
        "fee_rate",
        "手续费率",
        "佣金率",
        "手续费",
    ),
    "slippage": (
        "slippage",
        "slip",
        "滑点",
    ),
    "initial_capital": (
        "initial_capital",
        "capital",
        "cash",
        "初始资金",
        "本金",
    ),
    "position_size": (
        "position_size",
        "position",
        "仓位",
        "仓位比例",
    ),
}

_DEFAULTS: Dict[str, Any] = {
    "initial_capital": 100000.0,
    "position_size": 1.0,
    "commission_rate": 0.001,
    "slippage": 0.001,
}

# 反向索引：别名（小写） -> 规范名
_ALIAS_INDEX: Dict[str, str] = {}
for _canonical, _names in PARAM_ALIASES.items():
    for _name in _names:
        _ALIAS_INDEX[_name.lower()] = _canonical


class ParameterConflictError(ValueError):
    """同一规范参数被两个不同的原名赋了不同的值。"""


def normalize_strategy_params(
    raw: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """把原始参数归一化。

    返回 ``(归一化参数, 命中的别名映射)``。别名映射形如
    ``{"comm": "commission_rate"}``；原名即规范名时不记录，保持清单精简。
    未知参数原样保留（但也纳入快照指纹，避免静默丢弃）。
    """
    normalized: Dict[str, Any] = dict(_DEFAULTS)
    aliases: Dict[str, str] = {}
    # 规范名 -> 首次遇到的原名，用于检测冲突
    first_origin: Dict[str, str] = {}

    for origin_key, value in (raw or {}).items():
        canonical = _ALIAS_INDEX.get(str(origin_key).lower())
        if canonical is None:
            # 未知参数：保留原名，避免静默丢弃
            normalized[origin_key] = value
            continue

        if canonical in first_origin:
            prev = normalized[canonical]
            if prev != value:
                raise ParameterConflictError(
                    f"参数 {canonical!r} 同时由 {first_origin[canonical]!r}="
                    f"{prev!r} 和 {origin_key!r}={value!r} 提供且取值不一致"
                )
            # 同值重复提供，忽略
            continue

        first_origin[canonical] = origin_key
        normalized[canonical] = value
        if origin_key != canonical:
            aliases[origin_key] = canonical

    # 只保留快照真正关心的四个费用/资金口径 + 未知参数，顺序固定
    ordered: Dict[str, Any] = {}
    for key in ("initial_capital", "position_size", "commission_rate", "slippage"):
        ordered[key] = normalized[key]
    for key in sorted(k for k in normalized if k not in _DEFAULTS):
        ordered[key] = normalized[key]
    return ordered, aliases
