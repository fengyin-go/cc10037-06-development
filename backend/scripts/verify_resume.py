"""回填续跑端到端断言：三分区写失败 + 已确认更正 + 依赖等待场景。

运行：.venv/bin/python scripts/verify_resume.py
"""
from __future__ import annotations

import sys

from app.engine.write_receipt import (
    BatchState,
    ReceiptState,
    ResumeCategory,
)
from app.services.backfill_tracker import DEMO_TRACKING_ID
from app.services.backfill_tracker import BackfillTrackerService


def assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: 期望 {expected!r}，实际 {actual!r}")
    print(f"  ✓ {label} = {actual!r}")


def main() -> int:
    svc = BackfillTrackerService()
    tid = DEMO_TRACKING_ID

    print("1) 打开续跑追踪视图，定位阻塞")
    view_payload = svc.resume_view(tid)
    view = view_payload["view"]
    rows = {row["partition"]: row for row in view["partitions"]}

    assert_eq(view["submittable_partitions"],
              ["2026-11-15", "2026-11-16"], "可提交分区")
    assert_eq(view["blocked_partitions"], ["2026-11-17"], "依赖阻塞分区")
    assert_eq(view["pending_partitions"],
              ["2026-11-15", "2026-11-16", "2026-11-17"], "待恢复分区（成功且无更正影响的不入选）")

    p15 = rows["2026-11-15"]
    assert_eq(p15["category"], ResumeCategory.INDEPENDENT_FAILURE.value, "11-15 分类")
    assert_eq(p15["latest_attempt_status"], "失败", "11-15 最近尝试状态")
    assert_eq(p15["blockers"][0]["blocker_type"], "直接写入失败", "11-15 直接失败阻塞明细")
    assert p15["submittable"] is True

    p16 = rows["2026-11-16"]
    assert_eq(p16["category"], ResumeCategory.CORRECTION_GROUP.value, "11-16 分类")
    assert_eq(p16["latest_attempt_status"], "成功", "11-16 最近尝试状态（首轮成功）")
    assert_eq(p16["correction_version"], "v2", "11-16 保留本轮更正版本")
    assert p16["submittable"] is True
    assert_eq(p16["confirmed_prerequisites"][0]["version"], "v2", "11-16 已确认前置结果")
    assert_eq(p16["receipt_state"], ReceiptState.STALE.value, "11-16 回执状态")

    p17 = rows["2026-11-17"]
    assert_eq(p17["category"], ResumeCategory.DEPENDENCY_GROUP.value, "11-17 分类")
    assert_eq(p17["latest_attempt_status"], "成功", "11-17 最近尝试状态（成功但受传递影响）")
    assert p17["submittable"] is False
    assert_eq(p17["blockers"][0]["source_partition"], "2026-11-16",
              "11-17 阻塞指向前一日累计结果分区")
    # 11-17 的 v1 仍可读但建立在已失效的前一日结果上，属于已被更正取代
    assert_eq(p17["receipt_state"], ReceiptState.STALE.value, "11-17 回执状态")

    print("2) 据依赖阻塞明细切批次")
    cut = svc.cut_resume_batches(tid)
    batches = cut["batches"]
    assert_eq(len(batches), 2, "批次数量")
    b1, b2 = batches
    assert_eq(b1["state"], BatchState.SUBMITTABLE.value, "批次1状态")
    assert_eq(b1["partitions"], ["2026-11-15", "2026-11-16"], "批次1覆盖日期")
    assert_eq(b1["coverage_start"], "2026-11-15", "批次1起始日")
    assert_eq(b1["coverage_end"], "2026-11-16", "批次1结束日")
    # 批次1中 11-16 的读取位置已重新指向更正版本 v2
    reads16 = b1["read_positions"]["2026-11-16"]
    assert_eq(reads16[0]["required_version"], "v2", "11-16 续跑读取位置指向 v2")
    assert_eq(b2["state"], BatchState.BLOCKED.value, "批次2状态")
    assert_eq(b2["partitions"], ["2026-11-17"], "批次2覆盖日期")

    print("3) 阻塞批次拒绝提交")
    try:
        svc.submit_resume_batch(tid, b2["batch_id"])
    except ValueError as exc:
        print(f"  ✓ 批次2提交被拒绝：{exc}")
    else:
        raise AssertionError("依赖阻塞批次不应允许提交")

    print("4) 提交可提交批次，从关联写入位置重新取数")
    result = svc.submit_resume_batch(tid, b1["batch_id"])
    exec_parts = {p["partition"]: p for p in result["execution"]["executed_partitions"]}
    assert_eq(sorted(exec_parts), ["2026-11-15", "2026-11-16"], "执行覆盖日期")
    wp16 = exec_parts["2026-11-16"]["write_position"]
    assert_eq(wp16["resume_from"], "receipt_last_write_position", "从回执写入位置续跑")
    assert_eq(wp16["upstream_reads"][0]["version"], "v2", "实际重新取数版本为更正版 v2")
    wp15 = exec_parts["2026-11-15"]["write_position"]
    assert_eq(wp15["upstream_reads"][0]["version"], "v1", "独立失败日按原位置 v1 重取")

    print("5) 依赖组解锁，切出新的可提交批次")
    next_view = result["next_view"]
    assert_eq(next_view["submittable_partitions"], ["2026-11-17"], "解锁后可提交分区")
    next_batches = result["next_batches"]
    assert_eq(len(next_batches), 1, "剩余批次数量")
    assert_eq(next_batches[0]["state"], BatchState.SUBMITTABLE.value, "依赖组批次已解锁")
    assert_eq(next_batches[0]["partitions"], ["2026-11-17"], "剩余批次覆盖 11-17")

    print("6) 提交依赖组批次并核对下游实际可读分区")
    result2 = svc.submit_resume_batch(tid, next_batches[0]["batch_id"])
    readable = result2["readable"]
    assert_eq(sorted(readable["actually_readable"]),
              ["2026-11-15", "2026-11-16", "2026-11-17"], "下游可读分区")
    assert_eq(sorted(readable["latest_and_confirmed"]),
              ["2026-11-15", "2026-11-16", "2026-11-17"], "全部为最新确认版本")
    for row in readable["partitions"]:
        assert row["downstream_can_read"] and not row["stale"], f"{row['partition']} 仍异常"

    print("7) 终态：无待恢复分区，成功尝试未被重复恢复")
    final = svc.resume_view(tid)["view"]
    assert_eq(final["pending_partitions"], [], "待恢复分区清空")
    assert_eq(final["stable_partitions"],
              ["2026-11-15", "2026-11-16", "2026-11-17"], "全部稳定")
    receipts = svc.get_receipts(tid)
    attempts = {r["partition"]: len(r["attempts"]) for r in receipts}
    assert_eq(attempts,
              {"2026-11-15": 2, "2026-11-16": 2, "2026-11-17": 2},
              "每分区恰好多一次续跑尝试（首轮成功未重复恢复）")

    print("\n全部断言通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
