"""回填追踪领域服务：管理数据回填进度、写入量统计和完成状态追踪。

上游：rerun_compensation 产生回刷任务 → backfill_tracker 追踪进度
消费：alert_policy 监视回填状态触发完成和失败告警

本服务同时持有按分区写入回执（WriteReceiptEngine）：
- 每个分区一条回执，关联每次写入尝试及其依赖阻塞明细、写入位置与产出版本；
- 续跑视图区分独立失败日、更正续跑组和依赖等待组；
- 回刷补偿按阻塞明细切出可提交批次，执行时从回执关联的写入位置重新取数。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.engine.write_receipt import (
    AttemptStatus,
    BlockerType,
    PartitionBlocker,
    PartitionReceipt,
    ResumeBatch,
    UpstreamRead,
    VersionInfo,
    WriteAttempt,
    WriteReceiptEngine,
)
from app.services.base import BaseModuleService
from app.store import store

# 演示回填：同一份回填在两个日分区发生变动
DEMO_TRACKING_ID = "BK-20261120-001"
DEMO_TARGET_TABLE = "dw.dws_user_profile_daily"
DEMO_UPSTREAM_TABLE = "dw.dwd_user_behavior"

# 写入回执与版本看板（内存态，真实环境下沉到数据库）
RECEIPT_REGISTRY: dict[str, list[PartitionReceipt]] = {}
VERSION_BOARD: dict[tuple[str, str], list[VersionInfo]] = {}
BATCH_REGISTRY: dict[str, list[ResumeBatch]] = {}
_bootstrapped = False


def _bootstrap_demo_scenario() -> None:
    """构造三分区续跑场景：一个写失败日、一个已确认更正日、一个依赖等待日。

    - 2026-11-15：首次写入直接失败（存储写入冲突），上游无更正，可独立恢复；
    - 2026-11-16：首次写入成功且结果已确认，但上游 DWD 当日分区收到确认更正
      （v1 → v2），需要按更正版本重新取数，属于更正续跑组；
    - 2026-11-17：首次写入成功，但读取了前一日（11-16）累计结果，DWS 11-16
      的更正版本尚未产出，属于依赖等待组，必须等上游更正走完。
    """
    global _bootstrapped
    if _bootstrapped:
        return
    _bootstrapped = True

    board = VERSION_BOARD
    # 上游 DWD 版本看板
    board[(DEMO_UPSTREAM_TABLE, "2026-11-15")] = [
        VersionInfo(DEMO_UPSTREAM_TABLE, "2026-11-15", "v1", True, "例行产出"),
    ]
    board[(DEMO_UPSTREAM_TABLE, "2026-11-16")] = [
        VersionInfo(DEMO_UPSTREAM_TABLE, "2026-11-16", "v1", True, "例行产出"),
        VersionInfo(DEMO_UPSTREAM_TABLE, "2026-11-16", "v2", True, "上游数据更正"),
    ]
    board[(DEMO_UPSTREAM_TABLE, "2026-11-17")] = [
        VersionInfo(DEMO_UPSTREAM_TABLE, "2026-11-17", "v1", True, "例行产出"),
    ]
    # DWS 累计快照基线与首轮回填产物
    board[(DEMO_TARGET_TABLE, "2026-11-14")] = [
        VersionInfo(DEMO_TARGET_TABLE, "2026-11-14", "v1", True, "例行产出"),
    ]
    board[(DEMO_TARGET_TABLE, "2026-11-16")] = [
        VersionInfo(DEMO_TARGET_TABLE, "2026-11-16", "v1", True, "回填首轮写入"),
    ]
    board[(DEMO_TARGET_TABLE, "2026-11-17")] = [
        VersionInfo(DEMO_TARGET_TABLE, "2026-11-17", "v1", True, "回填首轮写入"),
    ]

    receipts: list[PartitionReceipt] = []

    # 11-15：直接写入失败
    r15 = PartitionReceipt(
        tracking_id=DEMO_TRACKING_ID,
        target_table=DEMO_TARGET_TABLE,
        partition="2026-11-15",
        upstream_reads=[UpstreamRead(DEMO_UPSTREAM_TABLE, "2026-11-15", "v1")],
        current_result_version=None,
    )
    r15.attempts.append(WriteAttempt(
        attempt_no=1,
        partition="2026-11-15",
        status=AttemptStatus.FAILED.value,
        write_position={
            "mode": "snapshot_cumulative",
            "target_table": DEMO_TARGET_TABLE,
            "target_partition": "2026-11-15",
            "upstream_reads": [
                {"table": DEMO_UPSTREAM_TABLE, "partition": "2026-11-15", "version": "v1"},
            ],
        },
        blockers=[PartitionBlocker(
            blocker_type=BlockerType.DIRECT_FAILURE.value,
            source_partition="2026-11-15",
            source_table=DEMO_TARGET_TABLE,
            required_version="v1",
            confirmed=True,
            detail="分区目录写入冲突（FileAlreadyExistsException），提交中断",
        )],
        result_version=None,
        rows_written=0,
        error_message="FileAlreadyExistsException: 分区目录已存在且未开启覆盖",
    ))
    receipts.append(r15)

    # 11-16：写入成功并确认，但上游 DWD 当日分区随后收到更正 v2
    r16 = PartitionReceipt(
        tracking_id=DEMO_TRACKING_ID,
        target_table=DEMO_TARGET_TABLE,
        partition="2026-11-16",
        upstream_reads=[UpstreamRead(DEMO_UPSTREAM_TABLE, "2026-11-16", "v1")],
        current_result_version="v1",
    )
    r16.attempts.append(WriteAttempt(
        attempt_no=1,
        partition="2026-11-16",
        status=AttemptStatus.SUCCESS.value,
        write_position={
            "mode": "snapshot_cumulative",
            "target_table": DEMO_TARGET_TABLE,
            "target_partition": "2026-11-16",
            "upstream_reads": [
                {"table": DEMO_UPSTREAM_TABLE, "partition": "2026-11-16", "version": "v1"},
            ],
        },
        blockers=[],
        result_version="v1",
        rows_written=118400,
    ))
    receipts.append(r16)

    # 11-17：写入成功，但读取前一日累计结果 DWS 11-16@v1，更正链尚未走完
    r17 = PartitionReceipt(
        tracking_id=DEMO_TRACKING_ID,
        target_table=DEMO_TARGET_TABLE,
        partition="2026-11-17",
        upstream_reads=[
            UpstreamRead(DEMO_UPSTREAM_TABLE, "2026-11-17", "v1"),
            UpstreamRead(DEMO_TARGET_TABLE, "2026-11-16", "v1"),
        ],
        current_result_version="v1",
    )
    r17.attempts.append(WriteAttempt(
        attempt_no=1,
        partition="2026-11-17",
        status=AttemptStatus.SUCCESS.value,
        write_position={
            "mode": "snapshot_cumulative",
            "target_table": DEMO_TARGET_TABLE,
            "target_partition": "2026-11-17",
            "upstream_reads": [
                {"table": DEMO_UPSTREAM_TABLE, "partition": "2026-11-17", "version": "v1"},
                {"table": DEMO_TARGET_TABLE, "partition": "2026-11-16", "version": "v1"},
            ],
        },
        blockers=[],
        result_version="v1",
        rows_written=121750,
    ))
    receipts.append(r17)

    RECEIPT_REGISTRY[DEMO_TRACKING_ID] = receipts


class BackfillTrackerService(BaseModuleService):
    MODULE = "backfill_tracker"
    ENTITY = "回填进度"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["追踪ID", "回填任务", "当前分区", "完成比例", "预估剩余", "数据写入量", "错误计数"]
    REQUIRED_FIELDS = ["追踪ID", "回填任务", "当前分区"]
    STATUS_ORDER = ["执行中", "已完成", "已暂停", "已失败"]
    ACTION_RULES = {"暂停回填": "执行中", "恢复回填": "已暂停", "标记完成": "执行中"}
    NEGATIVE_ACTIONS = []

    def update_progress(self, tracking_id: str, new_partition: str,
                        rows_written: int, errors: int = 0) -> dict[str, Any]:
        """更新回填进度。

        更新当前分区位置、数据写入量累计和错误计数，
        自动计算完成比例和预估剩余时间。
        """
        tracker = None
        for row in store.rows(self.MODULE):
            if row.get("追踪ID") == tracking_id:
                tracker = row
                break
        if tracker is None:
            return {"error": f"追踪记录 {tracking_id} 不存在"}

        previous_partition = tracker.get("当前分区", "")
        previous_written = int(tracker.get("数据写入量", "0"))
        previous_errors = int(tracker.get("错误计数", "0"))

        tracker["当前分区"] = new_partition
        tracker["数据写入量"] = str(previous_written + rows_written)
        tracker["错误计数"] = str(previous_errors + errors)

        try:
            current = int(new_partition)
            if len(new_partition) == 10:
                total_start_h = 2025010100
                total_end_h = 2025013123
            else:
                total_start_h = 0
                total_end_h = int(tracker.get("预估剩余", "100"))

            progress = min(round(current / max(total_end_h - total_start_h, 1) * 100, 1), 100)
            remaining = max(total_end_h - current, 0)
        except ValueError:
            progress = 0
            remaining = 100

        tracker["完成比例"] = str(progress)
        tracker["预估剩余"] = str(remaining)

        return {
            "tracking_id": tracking_id,
            "previous_partition": previous_partition,
            "current_partition": new_partition,
            "rows_written_this_batch": rows_written,
            "cumulative_rows": previous_written + rows_written,
            "cumulative_errors": previous_errors + errors,
            "progress_pct": progress,
            "remaining_partitions": remaining,
            "updated_at": datetime.now().isoformat(),
        }

    def query_by_task(self, task_name: str) -> list[dict[str, Any]]:
        """按回填任务查询追踪记录。

        从 store 中筛选指定任务的回填追踪数据，
        返回追踪列表供进度展示消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if task_name.casefold() in str(row.get("回填任务", "")).casefold():
                result.append(dict(row))
        return result

    def compute_throughput(self, task_name: str) -> dict[str, Any]:
        """计算回填吞吐量。

        统计指定任务所有追踪记录的写入量总和和错误率，
        产出吞吐量指标供容量规划消费。
        """
        trackers = self.query_by_task(task_name)
        total_rows = 0
        total_errors = 0
        completed = 0
        failed = 0

        for t in trackers:
            total_rows += int(t.get("数据写入量", "0"))
            total_errors += int(t.get("错误计数", "0"))
            status = t.get("status", "")
            if status == "已完成":
                completed += 1
            elif status == "已失败":
                failed += 1

        error_rate = round(total_errors / max(total_rows, 1) * 100, 2)
        return {
            "task": task_name,
            "total_trackers": len(trackers),
            "completed_trackers": completed,
            "failed_trackers": failed,
            "total_rows_written": total_rows,
            "total_errors": total_errors,
            "error_rate_pct": error_rate,
            "avg_rows_per_tracker": round(total_rows / max(len(trackers), 1), 0),
            "snapshot_at": datetime.now().isoformat(),
        }

    def check_completion_blockers(self, tracking_id: str) -> dict[str, Any]:
        """检查回填完成的阻塞因素。

        根据错误计数和当前状态判断回填是否被阻塞，
        产出阻塞分析供运维排障使用。
        """
        tracker = None
        for row in store.rows(self.MODULE):
            if row.get("追踪ID") == tracking_id:
                tracker = row
                break
        if tracker is None:
            return {"error": f"追踪记录 {tracking_id} 不存在"}

        errors = int(tracker.get("错误计数", "0"))
        status = tracker.get("status", "")
        progress = float(tracker.get("完成比例", "0"))
        written = int(tracker.get("数据写入量", "0"))

        blockers: list[str] = []
        if status == "已暂停":
            blockers.append("任务已被手动暂停")
        if errors > 100:
            blockers.append(f"错误次数过多({errors})")
        if progress > 80 and errors > 0 and written > 1000000:
            blockers.append("大数据量下的尾部错误风险")

        return {
            "tracking_id": tracking_id,
            "status": status,
            "progress_pct": progress,
            "errors": errors,
            "rows_written": written,
            "blocked": len(blockers) > 0,
            "blockers": blockers,
            "checked_at": datetime.now().isoformat(),
        }

    # ---- 写入回执与依赖阻塞续跑 ----

    def _load_receipts(self, tracking_id: str) -> list[PartitionReceipt]:
        _bootstrap_demo_scenario()
        receipts = RECEIPT_REGISTRY.get(tracking_id)
        if receipts is None:
            raise KeyError(f"追踪记录 {tracking_id} 没有按分区写入回执")
        return receipts

    def list_receipt_trackers(self) -> list[dict[str, Any]]:
        """列出已建立写入回执的回填追踪。"""
        _bootstrap_demo_scenario()
        result = []
        for tracking_id, receipts in RECEIPT_REGISTRY.items():
            result.append({
                "tracking_id": tracking_id,
                "target_table": receipts[0].target_table if receipts else "",
                "partition_count": len(receipts),
                "attempt_count": sum(len(r.attempts) for r in receipts),
            })
        return result

    def get_receipts(self, tracking_id: str) -> list[dict[str, Any]]:
        """读取某个回填的全部按分区写入回执。"""
        return [r.to_dict() for r in self._load_receipts(tracking_id)]

    def resume_view(self, tracking_id: str) -> dict[str, Any]:
        """生成续跑视图：定位阻塞、分类独立失败日与依赖等待组。

        页面列出准备继续的分区时，保留本轮更正版本和已确认的前置结果。
        """
        receipts = self._load_receipts(tracking_id)
        engine = WriteReceiptEngine()
        view = engine.build_resume_view(receipts, VERSION_BOARD)

        # 每次重算后刷新批次注册表，保证阻塞解除后能切出新的可提交批次
        batches = engine.cut_batches(view)
        BATCH_REGISTRY[tracking_id] = batches

        tracker_row = self._find_tracker_row(tracking_id)
        return {
            "tracking_id": tracking_id,
            "target_table": receipts[0].target_table if receipts else "",
            "backfill_task": tracker_row.get("回填任务", "") if tracker_row else "",
            "generated_at": datetime.now().isoformat(),
            "view": view,
            "receipts": [r.to_dict() for r in receipts],
            "batches": [b.to_dict() for b in batches],
        }

    def cut_resume_batches(self, tracking_id: str) -> dict[str, Any]:
        """回刷补偿依据依赖阻塞明细切出可提交批次与依赖阻塞批次。"""
        view = self.resume_view(tracking_id)
        return {
            "tracking_id": tracking_id,
            "batches": view["batches"],
            "submittable_partitions": view["view"]["submittable_partitions"],
            "blocked_partitions": view["view"]["blocked_partitions"],
            "cut_at": datetime.now().isoformat(),
        }

    def submit_resume_batch(self, tracking_id: str, batch_id: int) -> dict[str, Any]:
        """提交并执行续跑批次：从回执关联的写入位置重新取数。"""
        receipts = self._load_receipts(tracking_id)
        if tracking_id not in BATCH_REGISTRY:
            self.resume_view(tracking_id)
        target = next(
            (b for b in BATCH_REGISTRY[tracking_id] if b.batch_id == batch_id), None,
        )
        if target is None:
            raise KeyError(f"批次 {batch_id} 不存在，请重新切批")

        engine = WriteReceiptEngine()
        before = {
            "state": target.state,
            "coverage_start": target.coverage_start,
            "coverage_end": target.coverage_end,
            "partitions": list(target.partitions),
            "read_positions": {
                p: list(reads) for p, reads in target.read_positions.items()
            },
        }
        execution = engine.execute_batch(target, receipts, VERSION_BOARD)

        # 执行后重算视图与后续批次（依赖组可能已解锁）
        refreshed = engine.build_resume_view(receipts, VERSION_BOARD)
        BATCH_REGISTRY[tracking_id] = engine.cut_batches(refreshed)
        return {
            "tracking_id": tracking_id,
            "submitted_batch": before,
            "execution": execution,
            "next_view": refreshed,
            "next_batches": [b.to_dict() for b in BATCH_REGISTRY[tracking_id]],
            "readable": engine.downstream_readable(receipts, VERSION_BOARD),
        }

    def downstream_readable_partitions(self, tracking_id: str) -> dict[str, Any]:
        """核对下游实际能读取到的分区、版本以及是否为最新确认版本。"""
        receipts = self._load_receipts(tracking_id)
        engine = WriteReceiptEngine()
        result = engine.downstream_readable(receipts, VERSION_BOARD)
        result["tracking_id"] = tracking_id
        result["checked_at"] = datetime.now().isoformat()
        return result

    def _find_tracker_row(self, tracking_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("追踪ID") == tracking_id:
                return row
        return None

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg