from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.database import get_db
from backend.responses import api_response
from backend.services.sync_service import prioritize_devices
from backend.utils import rows_to_list

router = APIRouter()


@router.get("/api/device-sync-state")
def get_device_sync_state():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT device_id, COUNT(*) AS anchor_count, MAX(updated_at) AS anchors_updated_at
            FROM device_sync_anchors
            GROUP BY device_id
            """
        )
        anchor_rows = {
            row["device_id"]: {
                "anchor_count": int(row["anchor_count"] or 0),
                "anchors_updated_at": row["anchors_updated_at"],
            }
            for row in cur.fetchall()
        }

        cur.execute(
            """
            SELECT device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
                   last_error_message, last_items_count, last_accepted_count, last_deduplicated_count, updated_at
            FROM device_sync_state
            ORDER BY updated_at DESC
            """
        )
        devices = prioritize_devices(rows_to_list(cur.fetchall()))

        cur.execute(
            """
            SELECT id, device_id, bundle_id, sent_at, received_at, item_count,
                   accepted_count, deduplicated_count, status, error_message
            FROM ingest_events
            ORDER BY id DESC
            LIMIT 10
            """
        )
        events = rows_to_list(cur.fetchall())

    for device in devices:
        device.update(anchor_rows.get(device["device_id"], {"anchor_count": 0, "anchors_updated_at": None}))

    return api_response({"devices": devices, "recent_events": events})


@router.get("/api/device-sync-state/anchors")
def get_device_sync_anchors(device_id: str = Query(...), bundle_id: Optional[str] = Query(None)):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
                   last_error_message, last_items_count, last_accepted_count, last_deduplicated_count, updated_at
            FROM device_sync_state
            WHERE device_id = %s
            """,
            (device_id,),
        )
        device = cur.fetchone()
        if not device:
            raise HTTPException(404, "未找到该 device_id 的同步状态")

        if bundle_id and device["bundle_id"] != bundle_id:
            raise HTTPException(404, "device_id 存在，但 bundle_id 不匹配")

        cur.execute(
            """
            SELECT record_type, anchor_value, updated_at
            FROM device_sync_anchors
            WHERE device_id = %s
            ORDER BY record_type
            """,
            (device_id,),
        )
        anchor_rows = rows_to_list(cur.fetchall())

    anchors = {row["record_type"]: row["anchor_value"] for row in anchor_rows}
    anchors_updated_at = max((row["updated_at"] for row in anchor_rows), default=None)
    device_payload = dict(device)
    device_payload["anchor_count"] = len(anchor_rows)
    device_payload["anchors_updated_at"] = anchors_updated_at
    return api_response({"device": device_payload, "anchors": anchors, "anchor_records": anchor_rows})
