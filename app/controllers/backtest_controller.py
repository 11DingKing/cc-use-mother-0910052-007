"""业务模块说明。"""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.backtest_service import BacktestService
from app.services.backtest_run_service import BacktestRunService

router = APIRouter(prefix="/api/backtest", tags=["backtest"])
backtest_service = BacktestService()
run_service = BacktestRunService()


class RunBacktestRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    period: str = "daily"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000.0
    position_size: float = 1.0


class CreateSnapshotRequest(BaseModel):
    """创建不可变输入快照的请求。"""
    account_id: str
    stock_code: str
    period: str = "daily"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000.0
    position_size: float = 1.0
    commission_rate: float = 0.001
    slippage: float = 0.001


class StartRunRequest(BaseModel):
    """启动回测运行的请求。指定 snapshot_id 或内联快照参数。"""
    account_id: str
    name: str
    snapshot_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    stock_code: Optional[str] = None
    period: str = "daily"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000.0
    position_size: float = 1.0
    commission_rate: float = 0.001
    slippage: float = 0.001


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    """业务模块说明。"""
    return datetime.strptime(value, "%Y-%m-%d") if value else None


@router.post("/run")
async def run_backtest(request: RunBacktestRequest):
    """业务模块说明。"""
    start = _parse_date(request.start_date)
    end = _parse_date(request.end_date)

    return backtest_service.run_backtest(
        request.stock_code,
        request.period,
        start,
        end,
        request.initial_capital,
        request.position_size,
    )


# ---------------------------------------------------------------------------
# 不可变快照
# ---------------------------------------------------------------------------

@router.post("/snapshots")
async def create_snapshot(request: CreateSnapshotRequest):
    """创建（或复用）一份不可变输入快照。"""
    return run_service.create_snapshot(
        account_id=request.account_id,
        stock_code=request.stock_code,
        period=request.period,
        start_date=_parse_date(request.start_date),
        end_date=_parse_date(request.end_date),
        initial_capital=request.initial_capital,
        position_size=request.position_size,
        commission_rate=request.commission_rate,
        slippage=request.slippage,
    )


@router.get("/snapshots")
async def list_snapshots(
    account_id: str = Query(description="账户ID"),
    stock_code: Optional[str] = Query(default=None, description="股票代码"),
    limit: int = Query(default=20, description="返回数量"),
):
    """按账户列出快照。"""
    snapshots = run_service.list_snapshots(account_id, stock_code, limit)
    return {"count": len(snapshots), "snapshots": snapshots}


@router.get("/snapshots/{snapshot_id}")
async def get_snapshot(
    snapshot_id: str,
    account_id: str = Query(description="账户ID"),
):
    """查询快照元数据（账户隔离）。"""
    return run_service.get_snapshot(account_id, snapshot_id)


# ---------------------------------------------------------------------------
# 运行生命周期
# ---------------------------------------------------------------------------

@router.post("/runs")
async def start_run(request: StartRunRequest):
    """启动一次回测运行（绑定不可变快照，同步执行）。"""
    params = {}
    if not request.snapshot_id:
        if not request.stock_code:
            from app.middleware.exception_handler import ValidationException
            raise ValidationException(
                message="stock_code is required when snapshot_id is not provided",
                field="stock_code",
                value=request.stock_code,
            )
        params = {
            "stock_code": request.stock_code,
            "period": request.period,
            "start_date": _parse_date(request.start_date),
            "end_date": _parse_date(request.end_date),
            "initial_capital": request.initial_capital,
            "position_size": request.position_size,
            "commission_rate": request.commission_rate,
            "slippage": request.slippage,
        }

    return run_service.start_run(
        account_id=request.account_id,
        name=request.name,
        snapshot_id=request.snapshot_id,
        idempotency_key=request.idempotency_key,
        **params,
    )


@router.get("/runs")
async def list_runs(
    account_id: str = Query(description="账户ID"),
    name: Optional[str] = Query(default=None, description="回测名称"),
    status: Optional[str] = Query(default=None, description="运行状态"),
    limit: int = Query(default=20, description="返回数量"),
):
    """按账户列出运行。"""
    runs = run_service.list_runs(account_id, name, status, limit)
    return {"count": len(runs), "runs": runs}


@router.get("/runs/{run_id}")
async def get_run_report(
    run_id: str,
    account_id: str = Query(description="账户ID"),
):
    """运行报告：列出被锁定的输入以及结果是否完整。"""
    return run_service.get_run_report(account_id, run_id)


@router.post("/runs/{run_id}/resume")
async def resume_run(
    run_id: str,
    account_id: str = Query(description="账户ID"),
):
    """失败后继续 / 取消后重试（沿用原快照，从断点恢复）。"""
    return run_service.resume_run(account_id, run_id)


@router.post("/runs/{run_id}/replay")
async def replay_run(
    run_id: str,
    account_id: str = Query(description="账户ID"),
):
    """同快照重放：创建全新运行，输入与原运行完全一致。"""
    return run_service.replay_run(account_id, run_id)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    account_id: str = Query(description="账户ID"),
):
    """请求取消运行。"""
    return run_service.cancel_run(account_id, run_id)


@router.post("/runs/{run_id}/archive")
async def archive_run(
    run_id: str,
    account_id: str = Query(description="账户ID"),
):
    """归档已完成运行的结果（归档后只读）。"""
    return run_service.archive_run(account_id, run_id)


# ---------------------------------------------------------------------------
# 结果查询
# ---------------------------------------------------------------------------

@router.get("/{result_id}/report")
async def get_report(result_id: int):
    """业务模块说明。"""
    return backtest_service.get_result(result_id)


@router.get("/list")
async def list_results(
    stock_code: Optional[str] = Query(default=None, description="股票代码"),
    limit: int = Query(default=20, description="返回数量"),
):
    """业务模块说明。"""
    results = backtest_service.list_results(stock_code, limit)
    return {
        "count": len(results),
        "results": results,
    }
