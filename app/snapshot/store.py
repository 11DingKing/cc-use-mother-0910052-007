"""快照与运行的归档存储。

归档目录布局（所有写入均为"临时文件 + ``os.replace``"原子落盘）::

    <root>/accounts/<account_id>/
        builds/<build_id>/            # 未封存的构建现场（支持失败后继续）
            market.json
            strategy.json
            calendar.json
            fees.json
        snapshots/<fingerprint>/     # 已封存、内容寻址、只读
            manifest.json
            market_candles.json
            SEALED                    # O_EXCL 封存标记，保证并发封存幂等
        runs/<run_id>/
            run.json                  # 运行状态机（可更新）
            result.json               # 完成后归档（一次写入，不再改写）
            report.json

隔离规则：

- 快照与运行都位于账户目录下，跨账户读取直接抛 :class:`SnapshotAccessDenied`，
  因此数据补录、并发启动、取消后重试、跨账户查询都不可能串用快照。
- 快照目录内容寻址：同账户、同四类输入必然得到同一快照 ID；不同输入必然
  得到不同快照。
"""

import json
import os
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.snapshot.models import (
    SNAPSHOT_SCHEMA_VERSION,
    CompletenessStatus,
    FeeSchedule,
    MarketSlice,
    RunStatus,
    SnapshotManifest,
    SnapshotRun,
    StrategyVersion,
    TradingCalendar,
)
from app.snapshot.fingerprint import content_fingerprint

_BUILD_STAGES = ("market", "strategy", "calendar", "fees")
_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class SnapshotError(RuntimeError):
    """快照存储通用错误。"""


class SnapshotNotFound(SnapshotError):
    """快照/运行不存在，或不属于当前账户（不暴露他账是否存在）。"""


class SnapshotAccessDenied(SnapshotError):
    """跨账户访问被拒绝。"""


class SnapshotAlreadySealed(SnapshotError):
    """快照已封存，输入不可再修改。"""


class InvalidRunTransition(SnapshotError):
    """非法的运行状态迁移。"""


def _now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _check_id(kind: str, value: str) -> str:
    if not value or not _SAFE_ID.match(value):
        raise SnapshotError(f"非法的 {kind}: {value!r}")
    return value


def atomic_write_json(path: Path, payload: Any) -> None:
    """临时文件 + os.replace：读端要么看到旧文件、要么看到完整新文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


class SnapshotStore:
    """文件归档存储；线程安全（进程内加锁），封存靠 O_EXCL 跨进程幂等。"""

    def __init__(self, root: os.PathLike | str):
        self.root = Path(root)
        self._locks: Dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # -- 基础路径 -----------------------------------------------------------

    def _lock(self, key: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
            return lock

    def _account_dir(self, account_id: str) -> Path:
        _check_id("account_id", account_id)
        return self.root / "accounts" / account_id

    def _build_dir(self, account_id: str, build_id: str) -> Path:
        _check_id("build_id", build_id)
        return self._account_dir(account_id) / "builds" / build_id

    def _snapshot_dir(self, account_id: str, fingerprint16: str) -> Path:
        _check_id("fingerprint", fingerprint16)
        return self._account_dir(account_id) / "snapshots" / fingerprint16

    def _run_dir(self, account_id: str, run_id: str) -> Path:
        _check_id("run_id", run_id)
        return self._account_dir(account_id) / "runs" / run_id

    # -- 构建期（失败后继续） -----------------------------------------------

    def new_build_id(self) -> str:
        return _new_id("build")

    def save_build_part(
        self, account_id: str, build_id: str, stage: str, payload: Dict[str, Any]
    ) -> None:
        """持久化某个输入阶段。已封存的构建不能再写。"""
        if stage not in _BUILD_STAGES:
            raise SnapshotError(f"未知构建阶段: {stage!r}")
        bdir = self._build_dir(account_id, build_id)
        with self._lock(str(bdir)):
            if (bdir / "SEALED_REF").exists():
                raise SnapshotAlreadySealed(
                    f"构建 {build_id} 已封存为快照，输入不可修改"
                )
            atomic_write_json(bdir / f"{stage}.json", payload)

    def load_build(self, account_id: str, build_id: str) -> Dict[str, Any]:
        """读取构建现场；返回各阶段是否就绪及其内容（用于失败后继续）。"""
        bdir = self._build_dir(account_id, build_id)
        if not bdir.exists():
            raise SnapshotNotFound(f"构建不存在: {build_id}")
        parts: Dict[str, Any] = {}
        for stage in _BUILD_STAGES:
            path = bdir / f"{stage}.json"
            if path.exists():
                parts[stage] = read_json(path)
        sealed_ref = bdir / "SEALED_REF"
        return {
            "build_id": build_id,
            "account_id": account_id,
            "stages_present": list(parts.keys()),
            "stages_missing": [s for s in _BUILD_STAGES if s not in parts],
            "parts": parts,
            "sealed_snapshot_id": read_json(sealed_ref)["snapshot_id"]
            if sealed_ref.exists()
            else None,
        }

    def list_builds(self, account_id: str) -> List[Dict[str, Any]]:
        broot = self._account_dir(account_id) / "builds"
        if not broot.exists():
            return []
        out = []
        for bdir in sorted(broot.iterdir()):
            if not bdir.is_dir():
                continue
            present = [s for s in _BUILD_STAGES if (bdir / f"{s}.json").exists()]
            sealed_ref = bdir / "SEALED_REF"
            out.append(
                {
                    "build_id": bdir.name,
                    "stages_present": present,
                    "stages_missing": [s for s in _BUILD_STAGES if s not in present],
                    "sealed_snapshot_id": read_json(sealed_ref)["snapshot_id"]
                    if sealed_ref.exists()
                    else None,
                }
            )
        return out

    # -- 封存 ----------------------------------------------------------------

    def seal_build(
        self,
        account_id: str,
        build_id: str,
        completeness: str = CompletenessStatus.UNVERIFIED.value,
        completeness_detail: Optional[Dict[str, Any]] = None,
    ) -> SnapshotManifest:
        """把四个输入阶段封存成不可变快照。

        - 缺阶段直接失败（构建现场保留，补齐后可重试）；
        - 相同输入封存为同一快照（内容寻址，O_EXCL 处理并发）；
        - 重复封存同一构建返回既有快照，保持幂等。
        """
        bdir = self._build_dir(account_id, build_id)
        with self._lock(str(bdir)):
            sealed_ref = bdir / "SEALED_REF"
            if sealed_ref.exists():
                return self.load_snapshot(
                    account_id, read_json(sealed_ref)["snapshot_id"]
                )

            parts: Dict[str, Dict[str, Any]] = {}
            missing = []
            for stage in _BUILD_STAGES:
                path = bdir / f"{stage}.json"
                if not path.exists():
                    missing.append(stage)
                else:
                    parts[stage] = read_json(path)
            if missing:
                raise SnapshotError(
                    f"构建 {build_id} 缺少输入阶段，无法封存: {missing}"
                )

            market = MarketSlice(**parts["market"])
            strategy = StrategyVersion(**parts["strategy"])
            calendar = TradingCalendar(**parts["calendar"])
            fees = FeeSchedule(**parts["fees"])

            fp = content_fingerprint(
                market.content_hash,
                strategy.params_hash,
                calendar.calendar_hash,
                fees.fees_hash,
            )
            fingerprint16 = fp[:16]
            snapshot_id = f"snap-{account_id}-{fingerprint16}"
            sdir = self._snapshot_dir(account_id, fingerprint16)
            sdir.mkdir(parents=True, exist_ok=True)
            created_at = _now()

            manifest = SnapshotManifest(
                snapshot_id=snapshot_id,
                schema_version=SNAPSHOT_SCHEMA_VERSION,
                account_id=account_id,
                created_at=created_at,
                sealed_at=created_at,
                status="sealed",
                market=market,
                strategy=strategy,
                calendar=calendar,
                fees=fees,
                content_fingerprint=fp,
                completeness=completeness,
                completeness_detail=completeness_detail or {},
            )

            # 先幂等写内容（并发封存写入的是同内容，互相覆盖也无害），
            # 再以 O_EXCL 封存标记作为"提交点"：看到 SEALED 时 manifest 必然已就绪。
            atomic_write_json(
                sdir / "market_candles.json",
                {
                    "snapshot_id": snapshot_id,
                    "content_hash": market.content_hash,
                    "candles": market.candles,
                },
            )
            atomic_write_json(
                sdir / "manifest.json",
                self._manifest_to_json(manifest),
            )

            marker = sdir / "SEALED"
            try:
                fd = os.open(
                    str(marker), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o444
                )
                os.write(fd, created_at.encode("utf-8"))
                os.close(fd)
                os.chmod(marker, 0o444)
            except FileExistsError:
                existing = read_json(sdir / "manifest.json")
                if existing["content_fingerprint"] != fp:
                    # 理论上不可能：目录名就是指纹前缀
                    raise SnapshotError(
                        "快照指纹冲突，请检查归档目录是否损坏"
                    )
                manifest = self._manifest_from_json(existing)

            # 构建现场只留下指向快照的引用；此后 save_build_part 会拒绝再写。
            atomic_write_json(sealed_ref, {"snapshot_id": snapshot_id})
            return manifest

    # -- 快照读取 -------------------------------------------------------------

    @staticmethod
    def _parse_snapshot_id(snapshot_id: str) -> Dict[str, str]:
        m = re.match(r"^snap-(?P<account>[A-Za-z0-9._-]+)-(?P<fp>[0-9a-f]{16})$",
                     snapshot_id)
        if not m:
            raise SnapshotError(f"非法 snapshot_id: {snapshot_id!r}")
        return m.groupdict()

    def load_snapshot(self, account_id: str, snapshot_id: str) -> SnapshotManifest:
        parsed = self._parse_snapshot_id(snapshot_id)
        if parsed["account"] != account_id:
            raise SnapshotAccessDenied(
                f"快照 {snapshot_id} 不属于账户 {account_id}"
            )
        sdir = self._snapshot_dir(account_id, parsed["fp"])
        manifest_path = sdir / "manifest.json"
        if not manifest_path.exists():
            raise SnapshotNotFound(f"快照不存在: {snapshot_id}")
        return self._manifest_from_json(read_json(manifest_path))

    def load_snapshot_candles(
        self, account_id: str, snapshot_id: str
    ) -> List[Dict[str, Any]]:
        # 先做账户鉴权
        self.load_snapshot(account_id, snapshot_id)
        parsed = self._parse_snapshot_id(snapshot_id)
        sdir = self._snapshot_dir(account_id, parsed["fp"])
        payload = read_json(sdir / "market_candles.json")
        if payload["snapshot_id"] != snapshot_id:
            raise SnapshotError("行情片段与快照不匹配")
        return payload["candles"]

    def list_snapshots(self, account_id: str) -> List[Dict[str, Any]]:
        sroot = self._account_dir(account_id) / "snapshots"
        if not sroot.exists():
            return []
        out = []
        for sdir in sorted(sroot.iterdir()):
            mp = sdir / "manifest.json"
            if sdir.is_dir() and mp.exists():
                out.append(self._manifest_from_json(read_json(mp)).locked_inputs())
        return out

    # -- 运行状态机 -----------------------------------------------------------

    def create_run(
        self,
        account_id: str,
        snapshot_id: str,
        run_id: Optional[str] = None,
        replay_of: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> SnapshotRun:
        """创建运行；幂等键重复时返回同一 run（并发启动不重复建跑）。"""
        manifest = self.load_snapshot(account_id, snapshot_id)
        if manifest.status != "sealed":
            raise SnapshotError(f"快照未封存: {snapshot_id}")

        account_dir = self._account_dir(account_id)
        # 账户级锁：保证"查幂等键 -> 建 run"对并发启动是原子的
        with self._lock(str(account_dir)):
            if idempotency_key:
                _check_id("idempotency_key", idempotency_key)
                existing = self._find_run_by_idempotency(account_id, idempotency_key)
                if existing is not None:
                    if existing.snapshot_id != snapshot_id:
                        raise SnapshotAlreadySealed(
                            "幂等键已绑定不同快照，禁止复用："
                            f"{existing.snapshot_id} != {snapshot_id}"
                        )
                    return existing

            run_id = run_id or _new_id("run")
            _check_id("run_id", run_id)
            rdir = self._run_dir(account_id, run_id)
            run_path = rdir / "run.json"
            if run_path.exists():
                raise SnapshotError(f"运行已存在: {run_id}")
            run = SnapshotRun(
                run_id=run_id,
                snapshot_id=snapshot_id,
                account_id=account_id,
                status=RunStatus.PENDING.value,
                created_at=_now(),
                replay_of=replay_of,
            )
            atomic_write_json(run_path, self._run_to_json(run, idempotency_key))
            return run

    def _find_run_by_idempotency(
        self, account_id: str, idempotency_key: str
    ) -> Optional[SnapshotRun]:
        rroot = self._account_dir(account_id) / "runs"
        if not rroot.exists():
            return None
        for rdir in rroot.iterdir():
            rp = rdir / "run.json"
            if not rp.exists():
                continue
            data = read_json(rp)
            if data.get("idempotency_key") == idempotency_key:
                return self._run_from_json(data)
        return None

    # 合法状态迁移；FAILED/CANCELLED 可回到 RUNNING（取消后重试/失败继续），
    # 但始终绑定同一个 snapshot_id（在调用处校验，不可能换快照）。
    _TRANSITIONS: Dict[str, set] = {
        RunStatus.PENDING.value: {RunStatus.RUNNING.value, RunStatus.CANCELLED.value},
        RunStatus.RUNNING.value: {
            RunStatus.COMPLETED.value,
            RunStatus.FAILED.value,
            RunStatus.CANCELLED.value,
        },
        RunStatus.FAILED.value: {RunStatus.RUNNING.value},
        RunStatus.CANCELLED.value: {RunStatus.RUNNING.value},
        RunStatus.COMPLETED.value: set(),
    }

    def get_run(self, account_id: str, run_id: str) -> SnapshotRun:
        rdir = self._run_dir(account_id, run_id)
        run_path = rdir / "run.json"
        if not run_path.exists():
            raise SnapshotNotFound(f"运行不存在: {run_id}")
        return self._run_from_json(read_json(run_path))

    def transition_run(
        self,
        account_id: str,
        run_id: str,
        target: RunStatus,
        *,
        error: Optional[str] = None,
    ) -> SnapshotRun:
        rdir = self._run_dir(account_id, run_id)
        with self._lock(str(rdir)):
            run = self.get_run(account_id, run_id)
            current = run.status
            if target.value not in self._TRANSITIONS.get(current, set()):
                raise InvalidRunTransition(
                    f"运行 {run_id} 状态 {current!r} 不能迁移到 {target.value!r}"
                )
            data = read_json(rdir / "run.json")
            now = _now()
            data["status"] = target.value
            data["updated_at"] = now
            if target == RunStatus.RUNNING:
                data["attempts"] = int(data.get("attempts", 0)) + 1
                data["started_at"] = data.get("started_at") or now
                data["last_error"] = None
                # 新一轮尝试显式清除上一次的取消请求
                data["cancel_requested"] = False
            elif target == RunStatus.FAILED:
                data["last_error"] = error
            elif target == RunStatus.COMPLETED:
                data["completed_at"] = now
            elif target == RunStatus.CANCELLED:
                data["cancel_requested"] = True
            atomic_write_json(rdir / "run.json", data)
            return self._run_from_json(data)

    def request_cancel(self, account_id: str, run_id: str) -> SnapshotRun:
        """请求取消：打标记供执行轮询；未启动的运行直接置 CANCELLED。"""
        rdir = self._run_dir(account_id, run_id)
        with self._lock(str(rdir)):
            data = read_json(rdir / "run.json")
            data["cancel_requested"] = True
            if data["status"] == RunStatus.PENDING.value:
                data["status"] = RunStatus.CANCELLED.value
                data["updated_at"] = _now()
            atomic_write_json(rdir / "run.json", data)
            return self._run_from_json(data)

    def is_cancel_requested(self, account_id: str, run_id: str) -> bool:
        return self.get_run(account_id, run_id).cancel_requested

    def attach_result(
        self, account_id: str, run_id: str, result_ref: str, result_id: Optional[int]
    ) -> None:
        rdir = self._run_dir(account_id, run_id)
        with self._lock(str(rdir)):
            data = read_json(rdir / "run.json")
            data["result_ref"] = result_ref
            data["result_id"] = result_id
            data["updated_at"] = _now()
            atomic_write_json(rdir / "run.json", data)

    def archive_result(
        self,
        account_id: str,
        run_id: str,
        result_payload: Dict[str, Any],
        report_payload: Dict[str, Any],
    ) -> str:
        """归档结果与报告；已归档（COMPLETED）的结果拒绝覆写。"""
        rdir = self._run_dir(account_id, run_id)
        result_path = rdir / "result.json"
        with self._lock(str(rdir)):
            if result_path.exists():
                raise SnapshotAlreadySealed(f"运行 {run_id} 的结果已归档，不可覆写")
            atomic_write_json(result_path, result_payload)
            atomic_write_json(rdir / "report.json", report_payload)
            return str(result_path)

    def load_archived_result(self, account_id: str, run_id: str) -> Dict[str, Any]:
        rdir = self._run_dir(account_id, run_id)
        path = rdir / "result.json"
        if not path.exists():
            raise SnapshotNotFound(f"运行 {run_id} 尚无归档结果")
        return read_json(path)

    def load_archived_report(self, account_id: str, run_id: str) -> Dict[str, Any]:
        rdir = self._run_dir(account_id, run_id)
        path = rdir / "report.json"
        if not path.exists():
            raise SnapshotNotFound(f"运行 {run_id} 尚无归档报告")
        return read_json(path)

    def list_runs(
        self, account_id: str, snapshot_id: Optional[str] = None
    ) -> List[SnapshotRun]:
        rroot = self._account_dir(account_id) / "runs"
        if not rroot.exists():
            return []
        out = []
        for rdir in sorted(rroot.iterdir()):
            rp = rdir / "run.json"
            if not rp.exists():
                continue
            run = self._run_from_json(read_json(rp))
            if snapshot_id is None or run.snapshot_id == snapshot_id:
                out.append(run)
        return out

    # -- 序列化 ---------------------------------------------------------------

    @staticmethod
    def _manifest_to_json(m: SnapshotManifest) -> Dict[str, Any]:
        return {
            "snapshot_id": m.snapshot_id,
            "schema_version": m.schema_version,
            "account_id": m.account_id,
            "created_at": m.created_at,
            "sealed_at": m.sealed_at,
            "status": m.status,
            "market": m.market.__dict__,
            "strategy": m.strategy.__dict__,
            "calendar": m.calendar.__dict__,
            "fees": m.fees.__dict__,
            "content_fingerprint": m.content_fingerprint,
            "completeness": m.completeness,
            "completeness_detail": m.completeness_detail,
        }

    @staticmethod
    def _manifest_from_json(data: Dict[str, Any]) -> SnapshotManifest:
        return SnapshotManifest(
            snapshot_id=data["snapshot_id"],
            schema_version=data.get("schema_version", SNAPSHOT_SCHEMA_VERSION),
            account_id=data["account_id"],
            created_at=data["created_at"],
            sealed_at=data.get("sealed_at"),
            status=data.get("status", "sealed"),
            market=MarketSlice(**data["market"]),
            strategy=StrategyVersion(**data["strategy"]),
            calendar=TradingCalendar(**data["calendar"]),
            fees=FeeSchedule(**data["fees"]),
            content_fingerprint=data["content_fingerprint"],
            completeness=data.get(
                "completeness", CompletenessStatus.UNVERIFIED.value
            ),
            completeness_detail=data.get("completeness_detail", {}),
        )

    @staticmethod
    def _run_to_json(run: SnapshotRun, idempotency_key: Optional[str] = None):
        data = run.to_dict()
        if idempotency_key:
            data["idempotency_key"] = idempotency_key
        return data

    @staticmethod
    def _run_from_json(data: Dict[str, Any]) -> SnapshotRun:
        fields = SnapshotRun.__dataclass_fields__
        return SnapshotRun(**{k: v for k, v in data.items() if k in fields})
