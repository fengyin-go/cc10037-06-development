"""按分区写入回执与依赖阻塞续跑引擎。

核心概念：
- WriteAttempt：一次分区写入尝试，携带当时的写入位置（取数表/分区/版本）、
  依赖阻塞明细（PartitionBlocker）和产出版本；
- PartitionReceipt：按分区的写入回执，关联该分区的全部尝试、上游读取关系、
  当前可读版本与本轮需要消费的更正版本；
- 续跑分类：
  * INDEPENDENT_FAILURE 直接写入失败、上游无未决更正，可独立恢复；
  * CORRECTION_GROUP   上游更正已确认，需要按更正版本重取数的续跑组；
  * DEPENDENCY_GROUP   读取前一日累计结果、上游更正尚未走完的依赖组，必须等待；
- ResumeBatch：依据阻塞明细切出的可提交批次 / 依赖阻塞批次，批次内每个分区
  都带着重新取数所需的写入位置，执行时从该位置重新读数，而不是重放旧尝试。

成功的尝试默认不算待恢复；只有当其读取的上游版本确实被更正取代（含沿
"前一日累计结果" 链路的传递影响）时，才进入待恢复集合。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class AttemptStatus(str, Enum):
    SUCCESS = "成功"
    FAILED = "失败"
    BLOCKED = "依赖阻塞"


class BlockerType(str, Enum):
    DIRECT_FAILURE = "直接写入失败"
    UPSTREAM_CORRECTION = "上游数据更正"
    UPSTREAM_WRITE = "上游写入未完成"


class ResumeCategory(str, Enum):
    INDEPENDENT_FAILURE = "独立失败日"
    CORRECTION_GROUP = "更正续跑组"
    DEPENDENCY_GROUP = "依赖等待组"
    STABLE = "已稳定无需恢复"


class BatchState(str, Enum):
    SUBMITTABLE = "可提交"
    BLOCKED = "依赖阻塞"
    SUBMITTED = "已提交"


class ReceiptState(str, Enum):
    ACTIVE = "当前可读"
    STALE = "已被更正取代"
    BLOCKED = "上游未满足不可读"
    MISSING = "尚无成功写入"


@dataclass
class PartitionBlocker:
    """单次尝试（或当前回执）上的一条依赖阻塞明细。"""

    blocker_type: str
    source_partition: str
    source_table: str
    required_version: str
    detail: str
    confirmed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "blocker_type": self.blocker_type,
            "source_table": self.source_table,
            "source_partition": self.source_partition,
            "required_version": self.required_version,
            "confirmed": self.confirmed,
            "detail": self.detail,
        }


@dataclass
class UpstreamRead:
    """回执对一个上游（表, 分区, 版本）的读取关系。"""

    table: str
    partition: str
    version: str

    def key(self) -> tuple[str, str]:
        return self.table, self.partition

    def to_dict(self) -> dict[str, Any]:
        return {"table": self.table, "partition": self.partition, "version": self.version}


@dataclass
class WriteAttempt:
    """一次分区写入尝试：从哪个写入位置取数、被什么阻塞、产出了什么版本。"""

    attempt_no: int
    partition: str
    status: str
    write_position: dict[str, Any]
    blockers: list[PartitionBlocker] = field(default_factory=list)
    result_version: str | None = None
    rows_written: int = 0
    error_message: str = ""
    started_at: str = field(default_factory=_now)
    finished_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_no": self.attempt_no,
            "partition": self.partition,
            "status": self.status,
            "write_position": self.write_position,
            "blockers": [b.to_dict() for b in self.blockers],
            "result_version": self.result_version,
            "rows_written": self.rows_written,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


@dataclass
class VersionInfo:
    """版本看板上的一个可用版本。"""

    table: str
    partition: str
    version: str
    confirmed: bool
    origin: str  # 来源：例行产出 / 回填写入 / 上游更正

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "partition": self.partition,
            "version": self.version,
            "confirmed": self.confirmed,
            "origin": self.origin,
        }


@dataclass
class PartitionReceipt:
    """按分区写入回执：关联每次尝试与依赖阻塞明细。"""

    tracking_id: str
    target_table: str
    partition: str
    upstream_reads: list[UpstreamRead]
    attempts: list[WriteAttempt] = field(default_factory=list)
    current_result_version: str | None = None
    correction_version: str | None = None  # 本轮续跑需要消费的更正版本

    @property
    def latest_attempt(self) -> WriteAttempt | None:
        return self.attempts[-1] if self.attempts else None

    def last_write_position(self) -> dict[str, Any] | None:
        """最近一次尝试关联的写入位置，续跑从这里重新取数。"""
        latest = self.latest_attempt
        if latest is None:
            return None
        return dict(latest.write_position)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tracking_id": self.tracking_id,
            "target_table": self.target_table,
            "partition": self.partition,
            "upstream_reads": [u.to_dict() for u in self.upstream_reads],
            "attempts": [a.to_dict() for a in self.attempts],
            "current_result_version": self.current_result_version,
            "correction_version": self.correction_version,
            "last_write_position": self.last_write_position(),
        }


@dataclass
class ResumeItem:
    """续跑视图里单个分区的续跑结论。"""

    partition: str
    latest_attempt_status: str
    category: str
    submittable: bool
    current_result_version: str | None
    correction_version: str | None
    required_reads: list[dict[str, Any]]
    confirmed_prerequisites: list[dict[str, Any]]
    blockers: list[PartitionBlocker]
    receipt_state: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition": self.partition,
            "latest_attempt_status": self.latest_attempt_status,
            "category": self.category,
            "submittable": self.submittable,
            "current_result_version": self.current_result_version,
            "correction_version": self.correction_version,
            "required_reads": self.required_reads,
            "confirmed_prerequisites": self.confirmed_prerequisites,
            "blockers": [b.to_dict() for b in self.blockers],
            "receipt_state": self.receipt_state,
        }


@dataclass
class ResumeBatch:
    """据依赖阻塞明细切出的续跑批次。"""

    batch_id: int
    category: str
    state: str
    partitions: list[str]
    coverage_start: str
    coverage_end: str
    read_positions: dict[str, list[dict[str, Any]]]
    prerequisite_corrections: list[dict[str, Any]]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "category": self.category,
            "state": self.state,
            "partitions": list(self.partitions),
            "partition_count": len(self.partitions),
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "read_positions": self.read_positions,
            "prerequisite_corrections": self.prerequisite_corrections,
            "reason": self.reason,
        }


def _next_version(existing: list[VersionInfo] | None) -> str:
    if not existing:
        return "v1"
    max_no = 0
    for info in existing:
        if info.version.startswith("v") and info.version[1:].isdigit():
            max_no = max(max_no, int(info.version[1:]))
    return f"v{max_no + 1}"


class WriteReceiptEngine:
    """写入回执/依赖阻塞/续跑批次的纯领域逻辑，状态由服务层持有。"""

    # ---- 续跑视图：分类每个分区 ----

    def build_resume_view(
        self,
        receipts: list[PartitionReceipt],
        version_board: dict[tuple[str, str], list[VersionInfo]],
    ) -> dict[str, Any]:
        """根据回执与版本看板，定位每个分区的阻塞与续跑分类。

        规则：
        - 最近一次尝试直接失败、且上游没有未决更正 → 独立失败日，可立即恢复；
        - 最近一次尝试成功，但读取的上游版本已被确认的更正版本取代
          （含沿前一日累计链路传递）→ 更正续跑组，可按更正版本提交；
        - 所需上游更正版本尚未产出/确认 → 依赖等待组，阻塞不可提交；
        - 成功且上游无更正 → 已稳定，不进入待恢复集合。
        """
        receipts_by_partition = {r.partition: r for r in receipts}
        memo: dict[str, str] = {}

        def latest_confirmed(table: str, partition: str) -> VersionInfo | None:
            infos = version_board.get((table, partition), [])
            confirmed = [info for info in infos if info.confirmed]
            return confirmed[-1] if confirmed else None

        def latest_any(table: str, partition: str) -> VersionInfo | None:
            infos = version_board.get((table, partition), [])
            return infos[-1] if infos else None

        def partition_state(receipt: PartitionReceipt) -> str:
            """递归判定分区状态（含前一日累计链路的传递影响）。"""
            if receipt.partition in memo:
                return memo[receipt.partition]
            memo[receipt.partition] = ReceiptState.BLOCKED.value  # 防循环占位
            latest = receipt.latest_attempt
            if latest is None or latest.status == AttemptStatus.FAILED.value:
                memo[receipt.partition] = ReceiptState.MISSING.value
                return memo[receipt.partition]

            state = ReceiptState.ACTIVE.value
            for read in receipt.upstream_reads:
                board_latest = latest_confirmed(read.table, read.partition)
                if board_latest is None:
                    # 看板没有确认版本：若是同表前一日分区，交给回执递归判断
                    if read.table == receipt.target_table and read.partition in receipts_by_partition:
                        inner = partition_state(receipts_by_partition[read.partition])
                        if inner != ReceiptState.ACTIVE.value:
                            state = ReceiptState.BLOCKED.value
                    else:
                        state = ReceiptState.BLOCKED.value
                    continue
                if board_latest.version != read.version:
                    # 上游已有更新的确认版本 → 本分区结果被更正取代
                    state = ReceiptState.STALE.value
                elif read.table == receipt.target_table and read.partition in receipts_by_partition:
                    # 版本号相同但产出它的前一日分区本身已失效 → 传递性失效
                    inner = partition_state(receipts_by_partition[read.partition])
                    if inner in (ReceiptState.STALE.value, ReceiptState.BLOCKED.value,
                                 ReceiptState.MISSING.value):
                        state = (
                            ReceiptState.STALE.value
                            if inner == ReceiptState.STALE.value
                            else ReceiptState.BLOCKED.value
                        )
            memo[receipt.partition] = state
            return state

        items: list[ResumeItem] = []
        for receipt in sorted(receipts, key=lambda r: r.partition):
            latest = receipt.latest_attempt
            latest_status = latest.status if latest else "无尝试"
            blockers: list[PartitionBlocker] = []
            required_reads: list[dict[str, Any]] = []
            confirmed_prereq: list[dict[str, Any]] = []
            correction_version: str | None = None
            submittable = False

            for read in receipt.upstream_reads:
                board_latest = latest_confirmed(read.table, read.partition)
                required_version = board_latest.version if board_latest else read.version
                required_reads.append({
                    "table": read.table,
                    "partition": read.partition,
                    "read_version": read.version,
                    "required_version": required_version,
                    "available": board_latest is not None,
                    "confirmed": bool(board_latest and board_latest.confirmed),
                })
                if board_latest is not None and board_latest.version != read.version:
                    correction_version = board_latest.version
                    detail = (
                        f"上游 {read.table} 分区 {read.partition} "
                        f"已由 {read.version} 更正为 {board_latest.version}"
                    )
                    if board_latest.confirmed:
                        confirmed_prereq.append({
                            "table": read.table,
                            "partition": read.partition,
                            "version": board_latest.version,
                            "confirmed": True,
                        })
                    blockers.append(PartitionBlocker(
                        blocker_type=BlockerType.UPSTREAM_CORRECTION.value,
                        source_partition=read.partition,
                        source_table=read.table,
                        required_version=board_latest.version,
                        confirmed=board_latest.confirmed,
                        detail=detail,
                    ))
                elif read.table == receipt.target_table and read.partition in receipts_by_partition:
                    # 前一日累计链路：版本号相同，但前一日分区本身在等更正
                    inner_state = partition_state(receipts_by_partition[read.partition])
                    if inner_state != ReceiptState.ACTIVE.value:
                        inner_receipt = receipts_by_partition[read.partition]
                        needed = inner_receipt.correction_version or "下一确认版本"
                        blockers.append(PartitionBlocker(
                            blocker_type=BlockerType.UPSTREAM_CORRECTION.value,
                            source_partition=read.partition,
                            source_table=read.table,
                            required_version=needed,
                            confirmed=False,
                            detail=(
                                f"读取前一日累计结果 {read.table}/{read.partition}，"
                                f"该分区正在等待更正版本 {needed} 走完"
                            ),
                        ))

            if latest is not None and latest.status == AttemptStatus.FAILED.value:
                # 直接失败日：保留尝试上记录的直接写入失败阻塞明细
                direct = [b for b in latest.blockers
                          if b.blocker_type == BlockerType.DIRECT_FAILURE.value]
                blockers[:0] = direct
                unconfirmed = [b for b in blockers if not b.confirmed
                               and b.blocker_type != BlockerType.DIRECT_FAILURE.value]
                if unconfirmed:
                    category = ResumeCategory.DEPENDENCY_GROUP.value
                else:
                    category = ResumeCategory.INDEPENDENT_FAILURE.value
                    submittable = True
                receipt_state = ReceiptState.MISSING.value
            else:
                state = partition_state(receipt)
                receipt_state = state
                if state == ReceiptState.ACTIVE.value:
                    category = ResumeCategory.STABLE.value
                elif state == ReceiptState.STALE.value:
                    unconfirmed = [b for b in blockers if not b.confirmed]
                    if unconfirmed:
                        category = ResumeCategory.DEPENDENCY_GROUP.value
                    else:
                        category = ResumeCategory.CORRECTION_GROUP.value
                        submittable = True
                else:
                    category = ResumeCategory.DEPENDENCY_GROUP.value

            # 回填回执上记录本轮更正版本，供续跑页面保留展示
            receipt.correction_version = correction_version
            items.append(ResumeItem(
                partition=receipt.partition,
                latest_attempt_status=latest_status,
                category=category,
                submittable=submittable,
                current_result_version=receipt.current_result_version,
                correction_version=correction_version,
                required_reads=required_reads,
                confirmed_prerequisites=confirmed_prereq,
                blockers=blockers,
                receipt_state=receipt_state,
            ))

        pending = [i for i in items if i.category != ResumeCategory.STABLE.value]
        return {
            "partitions": [i.to_dict() for i in items],
            "pending_partitions": [i.partition for i in pending],
            "submittable_partitions": [i.partition for i in items if i.submittable],
            "blocked_partitions": [
                i.partition for i in items
                if i.category == ResumeCategory.DEPENDENCY_GROUP.value
            ],
            "stable_partitions": [
                i.partition for i in items
                if i.category == ResumeCategory.STABLE.value
            ],
            "summary": {
                "total": len(items),
                "pending": len(pending),
                "submittable": sum(1 for i in items if i.submittable),
                "blocked": sum(
                    1 for i in items
                    if i.category == ResumeCategory.DEPENDENCY_GROUP.value
                ),
                "stable": sum(
                    1 for i in items
                    if i.category == ResumeCategory.STABLE.value
                ),
            },
        }

    # ---- 批次切分 ----

    def cut_batches(self, view: dict[str, Any]) -> list[ResumeBatch]:
        """按依赖阻塞明细把待恢复分区切成可提交批次与依赖阻塞批次。"""
        items = {row["partition"]: row for row in view["partitions"]}
        submittable = list(view["submittable_partitions"])
        blocked = list(view["blocked_partitions"])
        batches: list[ResumeBatch] = []

        if submittable:
            batches.append(self._build_batch(1, submittable, items, True))
        if blocked:
            batches.append(self._build_batch(
                len(batches) + 1, blocked, items, False))
        return batches

    def _build_batch(
        self,
        batch_id: int,
        partitions: list[str],
        items: dict[str, dict[str, Any]],
        submittable: bool,
    ) -> ResumeBatch:
        ordered = sorted(partitions)
        read_positions: dict[str, list[dict[str, Any]]] = {}
        prereq: list[dict[str, Any]] = []
        for partition in ordered:
            row = items[partition]
            read_positions[partition] = row["required_reads"]
            for pre in row["confirmed_prerequisites"]:
                if pre not in prereq:
                    prereq.append(pre)
        if submittable:
            category = ResumeCategory.CORRECTION_GROUP.value if prereq else \
                ResumeCategory.INDEPENDENT_FAILURE.value
            reason = "直接失败日与已确认上游更正的分区可独立恢复，先切出提交"
        else:
            category = ResumeCategory.DEPENDENCY_GROUP.value
            reason = "读取前一日累计结果且上游更正未走完，等待依赖组解锁后再切批"
        return ResumeBatch(
            batch_id=batch_id,
            category=category,
            state=BatchState.SUBMITTABLE.value if submittable else BatchState.BLOCKED.value,
            partitions=ordered,
            coverage_start=ordered[0],
            coverage_end=ordered[-1],
            read_positions=read_positions,
            prerequisite_corrections=prereq,
            reason=reason,
        )

    # ---- 批次执行：从关联写入位置重新取数 ----

    def execute_batch(
        self,
        batch: ResumeBatch,
        receipts: list[PartitionReceipt],
        version_board: dict[tuple[str, str], list[VersionInfo]],
        rows_per_partition: int = 120000,
    ) -> dict[str, Any]:
        """执行续跑批次。

        每个分区从回执关联的最近写入位置出发，按批次携带的 required_reads
        重新指向上游确认版本后重新取数；已成功且无更正影响的分区不会出现在
        批次里，因此不会被重复恢复。
        """
        if batch.state != BatchState.SUBMITTABLE.value:
            raise ValueError(f"批次 {batch.batch_id} 当前为{batch.state}，不允许提交")

        receipts_by_partition = {r.partition: r for r in receipts}
        executed: list[dict[str, Any]] = []
        for partition in batch.partitions:
            receipt = receipts_by_partition.get(partition)
            if receipt is None:
                raise ValueError(f"分区 {partition} 缺少写入回执，无法续跑")
            base_position = receipt.last_write_position()
            if base_position is None:
                raise ValueError(f"分区 {partition} 没有可复用的写入位置")

            # 以批次切分时固化的 required_reads 为准，重新指向确认版本
            required = batch.read_positions.get(partition, [])
            for read in required:
                if not read.get("available") or not read.get("confirmed"):
                    raise ValueError(
                        f"分区 {partition} 的上游 {read['table']}/{read['partition']} "
                        f"所需版本 {read['required_version']} 尚未确认，不能取数"
                    )
            new_position = dict(base_position)
            new_position["upstream_reads"] = [
                {
                    "table": read["table"],
                    "partition": read["partition"],
                    "version": read["required_version"],
                    "confirmed": True,
                }
                for read in required
            ]
            new_position["resume_from"] = "receipt_last_write_position"

            target_key = (receipt.target_table, partition)
            new_version = _next_version(version_board.get(target_key))
            attempt = WriteAttempt(
                attempt_no=len(receipt.attempts) + 1,
                partition=partition,
                status=AttemptStatus.SUCCESS.value,
                write_position=new_position,
                blockers=[],
                result_version=new_version,
                rows_written=rows_per_partition,
                started_at=_now(),
                finished_at=_now(),
            )
            receipt.attempts.append(attempt)
            receipt.current_result_version = new_version
            receipt.correction_version = None
            # 回执顶层读取关系推进到本次实际消费的确认版本；
            # 历史读取位置完整保留在每次尝试的 write_position 中。
            receipt.upstream_reads = [
                UpstreamRead(
                    table=read["table"],
                    partition=read["partition"],
                    version=read["required_version"],
                )
                for read in required
            ]
            version_board.setdefault(target_key, []).append(VersionInfo(
                table=receipt.target_table,
                partition=partition,
                version=new_version,
                confirmed=True,
                origin="回填续跑写入",
            ))
            executed.append({
                "partition": partition,
                "attempt_no": attempt.attempt_no,
                "result_version": new_version,
                "rows_written": attempt.rows_written,
                "write_position": new_position,
            })

        batch.state = BatchState.SUBMITTED.value
        return {
            "batch_id": batch.batch_id,
            "executed_at": _now(),
            "executed_partitions": executed,
            "coverage_start": batch.coverage_start,
            "coverage_end": batch.coverage_end,
        }

    # ---- 下游可读性核对 ----

    def downstream_readable(
        self,
        receipts: list[PartitionReceipt],
        version_board: dict[tuple[str, str], list[VersionInfo]],
    ) -> dict[str, Any]:
        """核对下游实际能读取到的分区与版本。"""
        view = self.build_resume_view(receipts, version_board)
        state_by_partition = {
            row["partition"]: row for row in view["partitions"]
        }
        readable: list[dict[str, Any]] = []
        for receipt in sorted(receipts, key=lambda r: r.partition):
            key = (receipt.target_table, receipt.partition)
            infos = version_board.get(key, [])
            confirmed_versions = [info.version for info in infos if info.confirmed]
            latest_confirmed = confirmed_versions[-1] if confirmed_versions else None
            row = state_by_partition[receipt.partition]
            is_latest = (
                latest_confirmed is not None
                and receipt.current_result_version == latest_confirmed
            )
            readable.append({
                "partition": receipt.partition,
                "target_table": receipt.target_table,
                "current_result_version": receipt.current_result_version,
                "latest_confirmed_version": latest_confirmed,
                "downstream_can_read": latest_confirmed is not None,
                "is_latest": is_latest,
                "stale": row["receipt_state"] == ReceiptState.STALE.value,
                "receipt_state": row["receipt_state"],
                "blockers": row["blockers"],
                "readable_versions": confirmed_versions,
            })
        return {
            "partitions": readable,
            "actually_readable": [
                row["partition"] for row in readable if row["downstream_can_read"]
            ],
            "latest_and_confirmed": [
                row["partition"] for row in readable
                if row["downstream_can_read"] and row["is_latest"] and not row["stale"]
            ],
        }
