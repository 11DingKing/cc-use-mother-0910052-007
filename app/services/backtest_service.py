"""业务模块说明。"""

import json
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from app.config import db_session_scope
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.backtest.engine import BacktestEngine
from app.backtest.report import BacktestReportGenerator
from app.services.backtest_run_service import BacktestRunService, DEFAULT_ACCOUNT_ID
from app.services.stock_service import StockService
from app.services.analysis_service import AnalysisService
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import NotFoundException, AnalysisException

logger = logging.getLogger(__name__)


class BacktestService:
    """业务模块说明。"""

    def __init__(self):
        self.stock_service = StockService()
        self.analysis_service = AnalysisService()
        self.engine = BacktestEngine()
        self.report_generator = BacktestReportGenerator()
        self.run_service = BacktestRunService()

    def run_backtest(
        self,
        stock_code: str,
        period: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        initial_capital: float = 100000.0,
        position_size: float = 1.0,
    ) -> Dict[str, Any]:
        """执行回测。

        所有运行都绑定一份不可变输入快照（行情片段、策略版本、
        交易日历、费用口径），保证同名回测在不同时间运行可复核。
        """
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)

        report = self.run_service.start_run(
            account_id=DEFAULT_ACCOUNT_ID,
            name=f"{stock_code}:{period}",
            stock_code=stock_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            position_size=position_size,
        )

        if report["status"] != "completed":
            raise AnalysisException(
                message=report["error_message"] or "Backtest failed",
                stock_code=stock_code,
                period=period,
            )

        result = self.get_result(report["result"]["result_id"])
        result["snapshot_id"] = report["snapshot_id"]
        result["run_id"] = report["run_id"]
        result["inputs_locked"] = report["inputs_locked"]
        result["is_complete"] = report["is_complete"]
        return result
    
    def get_result(self, result_id: int) -> Dict[str, Any]:
        """业务模块说明。"""
        with db_session_scope() as session:
            entity = session.query(BacktestResultEntity).filter(
                BacktestResultEntity.id == result_id
            ).first()
            
            if not entity:
                raise NotFoundException(
                    message="Backtest result not found",
                    resource_type="BacktestResult",
                    resource_id=str(result_id),
                )
            
            return {
                "id": entity.id,
                "summary": {
                    "stock_code": entity.stock_code,
                    "period": entity.period,
                    "start_date": entity.start_date.isoformat(),
                    "end_date": entity.end_date.isoformat(),
                    "initial_capital": entity.initial_capital,
                    "final_capital": entity.final_capital,
                },
                "performance": {
                    "total_return": entity.total_return,
                    "annual_return": entity.annual_return,
                    "max_drawdown": entity.max_drawdown,
                    "sharpe_ratio": entity.sharpe_ratio,
                    "win_rate": entity.win_rate,
                    "profit_loss_ratio": entity.profit_loss_ratio,
                    "total_trades": entity.total_trades,
                    "winning_trades": entity.winning_trades,
                    "losing_trades": entity.losing_trades,
                },
                "trades": json.loads(entity.trades_json) if entity.trades_json else [],
                "equity_curve": json.loads(entity.equity_curve_json) if entity.equity_curve_json else [],
                "status": entity.status,
                "created_at": entity.created_at.isoformat(),
                "completed_at": entity.completed_at.isoformat() if entity.completed_at else None,
            }
    
    def list_results(
        self,
        stock_code: Optional[str] = None,
        limit: int = 20,
    ) -> list:
        """业务模块说明。"""
        with db_session_scope() as session:
            query = session.query(BacktestResultEntity)
            
            if stock_code:
                stock_code = validate_stock_code(stock_code)
                query = query.filter(BacktestResultEntity.stock_code == stock_code)
            
            results = query.order_by(
                BacktestResultEntity.created_at.desc()
            ).limit(limit).all()
            
            return [
                {
                    "id": r.id,
                    "stock_code": r.stock_code,
                    "period": r.period,
                    "total_return": r.total_return,
                    "win_rate": r.win_rate,
                    "status": r.status,
                    "created_at": r.created_at.isoformat(),
                }
                for r in results
            ]
