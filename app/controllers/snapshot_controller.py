"""不可变输入快照与快照化回测的 HTTP 接口。

所有端点都要求 ``X-Account-Id`` 请求头：快照、运行、归档结果全部按账户
隔离，跨账户访问返回 403，因此数据补录、参数别名、并发启动、取消后重试、
跨账户查询都不会串用快照。
"""

import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from app.backtest.engine import BacktestCancelled
from app.snapshot.runner import SnapshotBacktestRunner
from app.snapshot.store import (
    InvalidRunTransition,
    SnapshotAccessDenied,
    SnapshotAlreadySealed,
    SnapshotError,
    SnapshotNotFound,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/backtest", tags=["backtest-snapshot"])

# 进程内单一编排器（归档目录由 BACKTEST_ARCHIVE_DIR 决定）
runner = SnapshotBacktestRunner()

_ACCOUNT_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _account(x_account_id: Optional[str]) -> str:
    if not x_account_id or not x_account_id.strip():
        raise HTTPException(status_code=400, detail="缺少请求头 X-Account-Id")
    account_id = x_account_id.strip()
    if not _ACCOUNT_PATTERN.match(account_id):
        raise HTTPException(
            status_code=400,
            detail="X-Account-Id 仅允许 1-64 位字母、数字、点、下划线或连字符",
        )
    return account_id


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    return datetime.strptime(value, "%Y-%m-%d") if value else None


# --------------------------------------------------------------------------- #
# 请求模型
# --------------------------------------------------------------------------- #


class BuildSnapshotRequest(BaseModel):
    """构建快照；资金/费率参数允许使用别名（如 comm、滑点、本金）。"""

    stock_code: str
    period: str = "daily"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: Optional[float] = None
    position_size: Optional[float] = None
    commission_rate: Optional[float] = None
    slippage: Optional[float] = None
    # 允许透传别名参数（中文别名/缩写），归一化后锁定
    extra_params: dict = Field(default_factory=dict)


class RunSyncRequest(BaseModel):
    """构建并同步执行（一步到位，测试/单机使用）。"""

    build: BuildSnapshotRequest
    idempotency_key: Optional[str] = None


class StartRunRequest(BaseModel):
    idempotency_key: Optional[str] = None


class ReplayRequest(BaseModel):
    idempotency_key: Optional[str] = None


def _params_from(req: BuildSnapshotRequest) -> dict:
    params = dict(req.extra_params)
    for key in ("initial_capital", "position_size", "commission_rate", "slippage"):
        value = getattr(req, key)
        if value is not None:
            if key in params and params[key] != value:
                raise HTTPException(
                    status_code=409,
                    detail=f"参数 {key} 在标准字段与 extra_params 中取值冲突",
                )
            params[key] = value
    return params


# --------------------------------------------------------------------------- #
# 快照
# --------------------------------------------------------------------------- #


@router.post("/snapshots", status_code=201)
async def build_snapshot(
    request: BuildSnapshotRequest,
    x_account_id: Optional[str] = Header(default=None),
):
    """采集四类输入并封存为不可变快照，返回锁定清单与完整性判定。"""
    account_id = _account(x_account_id)
    try:
        return runner.build_snapshot(
            account_id=account_id,
            stock_code=request.stock_code,
            period=request.period,
            start_date=_parse_date(request.start_date),
            end_date=_parse_date(request.end_date),
            params=_params_from(request),
        )
    except SnapshotError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/builds/{build_id}/seal", status_code=201)
async def resume_build(
    build_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """失败后续建：补齐缺失输入阶段并封存（幂等）。"""
    account_id = _account(x_account_id)
    try:
        return runner.resume_build(account_id, build_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/builds")
async def list_builds(x_account_id: Optional[str] = Header(default=None)):
    account_id = _account(x_account_id)
    return {"account_id": account_id, "builds": runner.list_builds(account_id)}


@router.get("/snapshots")
async def list_snapshots(x_account_id: Optional[str] = Header(default=None)):
    account_id = _account(x_account_id)
    snapshots = runner.list_snapshots(account_id)
    return {"account_id": account_id, "count": len(snapshots), "snapshots": snapshots}


@router.get("/snapshots/{snapshot_id}")
async def get_snapshot(
    snapshot_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """查看快照锁定了哪些输入（行情/策略版本/日历/费用 + 完整性）。"""
    account_id = _account(x_account_id)
    try:
        return runner.get_snapshot(account_id, snapshot_id)
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# --------------------------------------------------------------------------- #
# 运行：启动 / 执行 / 取消 / 续跑 / 重放
# --------------------------------------------------------------------------- #


@router.post("/snapshots/{snapshot_id}/runs", status_code=201)
async def start_run(
    snapshot_id: str,
    request: StartRunRequest,
    x_account_id: Optional[str] = Header(default=None),
):
    """基于锁定快照启动一次运行（幂等键防并发重复启动）。"""
    account_id = _account(x_account_id)
    try:
        return runner.start_run(
            account_id, snapshot_id, idempotency_key=request.idempotency_key
        )
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotAlreadySealed as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/runs/{run_id}/execute")
async def execute_run(
    run_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """只从锁定快照取输入执行；已完成则直接回放归档报告。"""
    account_id = _account(x_account_id)
    try:
        return runner.execute_run(account_id, run_id)
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except InvalidRunTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except BacktestCancelled:
        raise _canned(runner, account_id, run_id)
    except Exception as exc:
        # 执行失败已在编排层落 FAILED 状态；向上抛出由全局处理器统一响应
        logger.warning("execute_run %s 失败: %s", run_id, exc)
        raise


def _canned(runner: SnapshotBacktestRunner, account_id: str, run_id: str):
    run = runner.get_run(account_id, run_id)
    return HTTPException(
        status_code=409,
        detail={
            "reason": "cancelled",
            "run_id": run_id,
            "status": run["status"],
            "retry": f"/api/backtest/runs/{run_id}/retry",
        },
    )


@router.post("/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """协作式取消：打标记，执行在下一个取消点停止，运行可续跑。"""
    account_id = _account(x_account_id)
    try:
        return runner.cancel_run(account_id, run_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/runs/{run_id}/retry")
async def retry_run(
    run_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """失败/取消后继续：沿用同一 run、同一快照，attempts +1。"""
    account_id = _account(x_account_id)
    try:
        return runner.retry_run(account_id, run_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except BacktestCancelled:
        raise _canned(runner, account_id, run_id)
    except (InvalidRunTransition, SnapshotError) as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/runs/{run_id}/replay", status_code=201)
async def replay_run(
    run_id: str,
    request: ReplayRequest,
    x_account_id: Optional[str] = Header(default=None),
):
    """同快照重放：用源运行锁定的快照创建并执行一个新运行。"""
    account_id = _account(x_account_id)
    try:
        return runner.replay_run(
            account_id, run_id, idempotency_key=request.idempotency_key
        )
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotAlreadySealed as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/runs")
async def list_runs(
    snapshot_id: Optional[str] = Query(default=None),
    x_account_id: Optional[str] = Header(default=None),
):
    account_id = _account(x_account_id)
    runs = runner.list_runs(account_id, snapshot_id)
    return {"account_id": account_id, "count": len(runs), "runs": runs}


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    account_id = _account(x_account_id)
    try:
        return runner.get_run(account_id, run_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/runs/{run_id}/report")
async def get_run_report(
    run_id: str,
    x_account_id: Optional[str] = Header(default=None),
):
    """获取归档报告：含锁定输入快照与结果完整性说明。"""
    account_id = _account(x_account_id)
    try:
        return runner.get_run_report(account_id, run_id)
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# --------------------------------------------------------------------------- #
# 一步到位：构建 + 启动 + 执行
# --------------------------------------------------------------------------- #


@router.post("/run-snapshot")
async def run_with_snapshot(
    request: RunSyncRequest,
    x_account_id: Optional[str] = Header(default=None),
):
    """采集、封存、启动、执行一次完成，返回带锁定输入的报告。"""
    account_id = _account(x_account_id)
    req = request.build
    try:
        locked = runner.build_snapshot(
            account_id=account_id,
            stock_code=req.stock_code,
            period=req.period,
            start_date=_parse_date(req.start_date),
            end_date=_parse_date(req.end_date),
            params=_params_from(req),
        )
        return runner.run_sync(
            account_id,
            locked["snapshot_id"],
            idempotency_key=request.idempotency_key,
        )
    except SnapshotAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except SnapshotAlreadySealed as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SnapshotError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
