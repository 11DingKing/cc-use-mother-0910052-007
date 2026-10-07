"""从给定 K 线离线计算交易信号。

快照运行时信号必须来自**快照内锁定的 K 线**，而不是数据库里可能已被
补录/修订的分析结果，因此这里把 :class:`AnalysisService` 的纯计算部分
抽成无副作用的函数，不读写任何表。
"""

from typing import List

from app.chan.models import RawCandle, Signal
from app.chan.kline_processor import KLineProcessor
from app.chan.fractal_detector import FractalDetector
from app.chan.bi_detector import BiDetector
from app.chan.duan_detector import DuanDetector
from app.chan.zhongshu_detector import ZhongshuDetector
from app.chan.signal_detector import SignalDetector

# 与 STRATEGY_CODE_VERSION 的指纹文件保持一致：这些组件定义了"策略版本"。
DETECTOR_CHAIN = [
    "KLineProcessor.clean",
    "KLineProcessor.merge",
    "FractalDetector.detect",
    "BiDetector.detect",
    "DuanDetector.detect",
    "ZhongshuDetector.detect",
    "SignalDetector.detect_all",
    "BacktestEngine.run",
]


def compute_signals(
    stock_code: str, period: str, candles: List[RawCandle]
) -> List[Signal]:
    """对锁定的 K 线依次执行缠论流水线，返回买卖点信号。"""
    kline_processor = KLineProcessor()
    fractal_detector = FractalDetector()
    bi_detector = BiDetector()
    duan_detector = DuanDetector()
    zhongshu_detector = ZhongshuDetector()

    cleaned, _ = kline_processor.clean(candles)
    merged = kline_processor.merge(cleaned)
    fractals = fractal_detector.detect(merged)
    bis = bi_detector.detect(fractals, merged)
    duans = duan_detector.detect(bis)
    zhongshus = zhongshu_detector.detect(bis)

    signal_detector = SignalDetector(stock_code, period)
    return signal_detector.detect_all(bis, duans, zhongshus)
