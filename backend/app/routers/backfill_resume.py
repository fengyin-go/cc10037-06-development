"""回填续跑API：按分区写入回执、依赖阻塞视图、续跑批次切分与执行、下游可读性核对。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.services.backfill_tracker import BackfillTrackerService

router = APIRouter(prefix="/api/backfill-resume", tags=["回填续跑"])

_service = BackfillTrackerService()


@router.get("/trackers")
def list_resume_trackers() -> dict:
    """列出已建立按分区写入回执的回填追踪。"""
    return {"items": _service.list_receipt_trackers()}


@router.get("/{tracking_id}/view")
def resume_view(tracking_id: str) -> dict:
    """打开续跑追踪视图：直接失败日、更正续跑组与依赖等待组定位。"""
    try:
        return _service.resume_view(tracking_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/{tracking_id}/receipts")
def get_receipts(tracking_id: str) -> dict:
    """读取按分区写入回执（含每次尝试与依赖阻塞明细）。"""
    try:
        return {"tracking_id": tracking_id, "receipts": _service.get_receipts(tracking_id)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

@router.post("/{tracking_id}/batches/cut")
def cut_batches(tracking_id: str) -> dict:
    """依据依赖阻塞明细切出可提交批次与依赖阻塞批次。"""
    try:
        return _service.cut_resume_batches(tracking_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{tracking_id}/batches/{batch_id}/submit")
def submit_batch(tracking_id: str, batch_id: int) -> dict:
    """提交执行续跑批次，从回执关联的写入位置重新取数，并核对下游可读分区。"""
    try:
        return _service.submit_resume_batch(tracking_id, batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{tracking_id}/downstream-readable")
def downstream_readable(tracking_id: str) -> dict:
    """核对下游实际能读取的分区与版本。"""
    try:
        return _service.downstream_readable_partitions(tracking_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
