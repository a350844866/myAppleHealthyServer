from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime

from fastapi import HTTPException

from backend.cache import dashboard_home_cache, overview_cache, record_types_cache
from backend.config import LOCAL_TIMEZONE
from backend.database import get_db
from backend.models import IngestPayload
from backend.utils import compact_dict, format_value_text, isoformat_z, normalize_ingest_datetime


def make_record_hash(payload: IngestPayload, item) -> str:
    base = f"bridge|{payload.device_id}|{payload.bundle_id}|{item.type}|{item.uuid}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def health_record_row_from_ingest(payload: IngestPayload, item) -> tuple:
    start_at = normalize_ingest_datetime(item.start_at)
    end_at = normalize_ingest_datetime(item.end_at)
    sent_at = normalize_ingest_datetime(payload.sent_at)
    metadata = {
        **item.metadata,
        "bridge_device_id": payload.device_id,
        "bridge_bundle_id": payload.bundle_id,
        "bridge_sent_at": isoformat_z(payload.sent_at),
        "bridge_kind": item.kind,
        "bridge_source": item.source,
    }
    value_text = (
        item.metadata.get("category_value_label")
        or item.metadata.get("category_value_raw")
        or format_value_text(item.value)
    )
    device_payload = compact_dict(
        {
            "device_id": payload.device_id,
            "bundle_id": payload.bundle_id,
            "product_type": item.metadata.get("product_type"),
            "source_bundle_id": item.metadata.get("source_bundle_id"),
        }
    )
    return (
        make_record_hash(payload, item),
        item.type,
        item.metadata.get("source_name") or payload.device_id,
        item.metadata.get("source_version"),
        json.dumps(device_payload, ensure_ascii=False, sort_keys=True) if device_payload else None,
        item.unit,
        value_text,
        item.value,
        sent_at,
        start_at,
        end_at,
        start_at.date(),
        json.dumps(metadata, ensure_ascii=False, sort_keys=True),
    )


def serialize_payload(payload: IngestPayload) -> str:
    return json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)


def require_ingest_token(authorization: str | None) -> None:
    expected = os.getenv("INGEST_API_TOKEN", "").strip()
    if not expected:
        return
    if authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="无效的 ingest token")


def upsert_device_sync_state(
    cur,
    *,
    payload: IngestPayload,
    status: str,
    accepted_count: int,
    deduplicated_count: int,
    error_message: str | None = None,
) -> None:
    sync_at = datetime.now(tz=LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)
    sent_at = normalize_ingest_datetime(payload.sent_at)
    cur.execute(
        """
        INSERT INTO device_sync_state (
            device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
            last_error_message, last_items_count, last_accepted_count, last_deduplicated_count
        )
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            bundle_id=VALUES(bundle_id),
            last_seen_at=CURRENT_TIMESTAMP,
            last_sent_at=VALUES(last_sent_at),
            last_sync_at=VALUES(last_sync_at),
            last_sync_status=VALUES(last_sync_status),
            last_error_message=VALUES(last_error_message),
            last_items_count=VALUES(last_items_count),
            last_accepted_count=VALUES(last_accepted_count),
            last_deduplicated_count=VALUES(last_deduplicated_count)
        """,
        (
            payload.device_id,
            payload.bundle_id,
            sent_at,
            sync_at,
            status,
            error_message,
            len(payload.items),
            accepted_count,
            deduplicated_count,
        ),
    )


def _clear_caches() -> None:
    dashboard_home_cache.clear()
    record_types_cache.clear()
    overview_cache.clear()
    with get_db() as db, db.cursor() as cur:
        cur.execute("DELETE FROM system_summary WHERE summary_key='overview'")
        cur.execute("DELETE FROM record_type_stats")


def ingest_samples(payload: IngestPayload, authorization: str | None) -> dict:
    require_ingest_token(authorization)

    unsupported_items = [item.kind for item in payload.items if item.kind != "sample"]
    if unsupported_items:
        raise HTTPException(400, f"暂不支持的 ingest kind: {', '.join(sorted(set(unsupported_items)))}")

    payload_json = serialize_payload(payload)
    accepted_count = len(payload.items)
    deduplicated_count = 0

    try:
        with get_db(autocommit=False) as db, db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_events (
                    device_id, bundle_id, sent_at, item_count, accepted_count,
                    deduplicated_count, status, payload_json
                )
                VALUES (%s, %s, %s, %s, 0, 0, 'received', %s)
                """,
                (
                    payload.device_id,
                    payload.bundle_id,
                    normalize_ingest_datetime(payload.sent_at),
                    accepted_count,
                    payload_json,
                ),
            )
            event_id = int(cur.lastrowid)

            rows = [health_record_row_from_ingest(payload, item) for item in payload.items]
            inserted_count = 0
            business_deduped_count = 0
            if rows:
                dedup_chunk = 200
                existing_keys: set = set()
                check_keys = [(row[1], row[9], row[10], row[2]) for row in rows]
                for i in range(0, len(check_keys), dedup_chunk):
                    chunk = check_keys[i : i + dedup_chunk]
                    placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(chunk))
                    flat_params = [v for key in chunk for v in key]
                    cur.execute(
                        f"""
                        SELECT type, start_at, end_at, source_name
                        FROM health_records
                        WHERE (type, start_at, end_at, source_name) IN ({placeholders})
                        """,
                        flat_params,
                    )
                    existing_keys.update(
                        (r["type"], r["start_at"], r["end_at"], r["source_name"])
                        for r in cur.fetchall()
                    )
                filtered_rows = [
                    row for row in rows if (row[1], row[9], row[10], row[2]) not in existing_keys
                ]
                business_deduped_count = len(rows) - len(filtered_rows)

                if filtered_rows:
                    cur.executemany(
                        """
                        INSERT IGNORE INTO health_records (
                            record_hash, type, source_name, source_version, device, unit,
                            value_text, value_num, creation_at, start_at, end_at, local_date, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        filtered_rows,
                    )
                    inserted_count = max(cur.rowcount, 0)

            deduplicated_count = accepted_count - inserted_count - business_deduped_count
            upsert_device_sync_state(
                cur,
                payload=payload,
                status="completed",
                accepted_count=accepted_count,
                deduplicated_count=deduplicated_count,
            )

            if payload.anchors:
                cur.executemany(
                    """
                    INSERT INTO device_sync_anchors (device_id, record_type, anchor_value)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        anchor_value=VALUES(anchor_value),
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    [(payload.device_id, record_type, anchor_value) for record_type, anchor_value in payload.anchors.items()],
                )

            cur.execute(
                """
                UPDATE ingest_events
                SET accepted_count=%s, deduplicated_count=%s, status='completed'
                WHERE id=%s
                """,
                (accepted_count, deduplicated_count, event_id),
            )
    except Exception as exc:
        error_message = str(exc)[:255]
        with get_db() as failed_db, failed_db.cursor() as failed_cur:
            upsert_device_sync_state(
                failed_cur,
                payload=payload,
                status="failed",
                accepted_count=0,
                deduplicated_count=0,
                error_message=error_message,
            )
            failed_cur.execute(
                """
                INSERT INTO ingest_events (
                    device_id, bundle_id, sent_at, item_count, accepted_count,
                    deduplicated_count, status, error_message, payload_json
                )
                VALUES (%s, %s, %s, %s, 0, 0, 'failed', %s, %s)
                """,
                (
                    payload.device_id,
                    payload.bundle_id,
                    normalize_ingest_datetime(payload.sent_at),
                    accepted_count,
                    error_message,
                    payload_json,
                ),
            )
        raise HTTPException(500, f"ingest failed: {error_message}") from exc

    _clear_caches()
    return {"ok": True, "accepted": accepted_count, "deduplicated": deduplicated_count}
