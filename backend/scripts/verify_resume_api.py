"""HTTP 路由冒烟：通过 TestClient 走完整续跑 API。

运行：PYTHONPATH=. .venv/bin/python scripts/verify_resume_api.py
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.services.backfill_tracker import DEMO_TRACKING_ID

client = TestClient(app)
tid = DEMO_TRACKING_ID
base = f"/api/backfill-resume/{tid}"


def check(resp, expected=200):
    if resp.status_code != expected:
        raise AssertionError(
            f"{resp.request.method} {resp.request.url} 期望 {expected}，"
            f"实际 {resp.status_code}: {resp.text}"
        )
    return resp.json()


# 追踪列表
items = check(client.get("/api/backfill-resume/trackers"))["items"]
assert any(t["tracking_id"] == tid for t in items)
print("✓ GET /trackers")

# 续跑视图
view_payload = check(client.get(f"{base}/view"))
view = view_payload["view"]
assert view["submittable_partitions"] == ["2026-11-15", "2026-11-16"]
assert view["blocked_partitions"] == ["2026-11-17"]
print("✓ GET /view 阻塞定位正确")

# 回执
receipts = check(client.get(f"{base}/receipts"))["receipts"]
assert len(receipts) == 3
r16 = next(r for r in receipts if r["partition"] == "2026-11-16")
assert r16["attempts"][0]["result_version"] == "v1"
print("✓ GET /receipts 含每次尝试明细")

# 切批
batches = check(client.post(f"{base}/batches/cut"))["batches"]
assert [b["state"] for b in batches] == ["可提交", "依赖阻塞"]
blocked_id = next(b["batch_id"] for b in batches if b["state"] == "依赖阻塞")
print("✓ POST /batches/cut 切出可提交与依赖阻塞批次")

# 阻塞批次提交 → 409
err = check(client.post(f"{base}/batches/{blocked_id}/submit"), expected=409)
assert "依赖阻塞" in err["detail"]
print("✓ 阻塞批次提交返回 409")

# 提交可提交批次
submittable_id = next(b["batch_id"] for b in batches if b["state"] == "可提交")
submit1 = check(client.post(f"{base}/batches/{submittable_id}/submit"))
executed = sorted(p["partition"] for p in submit1["execution"]["executed_partitions"])
assert executed == ["2026-11-15", "2026-11-16"]
assert submit1["next_view"]["submittable_partitions"] == ["2026-11-17"]
print("✓ 提交批次1，覆盖 11-15~11-16，依赖组解锁")

# 下游核对（此刻 11-17 仍为旧版可读）
mid = check(client.get(f"{base}/downstream-readable"))
assert sorted(mid["actually_readable"]) == ["2026-11-15", "2026-11-16", "2026-11-17"]
p17 = next(p for p in mid["partitions"] if p["partition"] == "2026-11-17")
assert p17["stale"] is True
print("✓ 下游核对：11-17 旧版可读但已失效")

# 提交剩余批次
next_batch = submit1["next_batches"][0]
submit2 = check(client.post(f"{base}/batches/{next_batch['batch_id']}/submit"))
final_readable = submit2["readable"]
assert sorted(final_readable["latest_and_confirmed"]) == [
    "2026-11-15", "2026-11-16", "2026-11-17",
]
print("✓ 提交批次2，下游三分区全部最新确认可读")

# 终态视图
final_view = check(client.get(f"{base}/view"))["view"]
assert final_view["pending_partitions"] == []
print("✓ 终态无待恢复分区")

# 未知追踪 → 404
check(client.get("/api/backfill-resume/NO-SUCH/view"), expected=404)
print("✓ 未知追踪返回 404")

print("\nHTTP 全链路冒烟通过 ✅")
