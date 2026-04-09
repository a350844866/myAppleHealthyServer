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

SUPPORTED_INGEST_KINDS = {"sample", "workout"}


def make_record_hash(payload: IngestPayload, item) -> str:
    base = f"bridge|{payload.device_id}|{payload.bundle_id}|{item.type}|{item.uuid}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def make_workout_hash(payload: IngestPayload, item) -> str:
    parts = json.dumps(
        [
            item.type,
            payload.device_id,
            item.metadata.get("source_name", ""),
            item.metadata.get("source_version", ""),
            str(item.start_at),
            str(item.end_at),
            item.metadata.get("duration", ""),
            item.metadata.get("total_distance", ""),
            item.metadata.get("total_energy_burned", ""),
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def _try_float(val: str | None) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def insert_workouts_from_ingest(cur, payload: IngestPayload, workout_items: list) -> int:
    if not workout_items:
        return 0
    inserted = 0
    for item in workout_items:
        start_at = normalize_ingest_datetime(item.start_at)
        end_at = normalize_ingest_datetime(item.end_at)
        workout_hash = make_workout_hash(payload, item)
        device_payload = compact_dict(
            {
                "device_id": payload.device_id,
                "bundle_id": payload.bundle_id,
                "device_name": item.metadata.get("device_name"),
                "device_model": item.metadata.get("device_model"),
            }
        )
        cur.execute(
            """
            INSERT INTO workouts
                (workout_hash, activity_type, duration, duration_unit,
                 total_distance, total_distance_unit, total_energy_burned,
                 total_energy_burned_unit, source_name, source_version,
                 device, creation_at, start_at, end_at, local_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_distance=COALESCE(VALUES(total_distance), total_distance),
                total_energy_burned=COALESCE(VALUES(total_energy_burned), total_energy_burned),
                duration=COALESCE(VALUES(duration), duration)
            """,
            (
                workout_hash,
                item.type,
                _try_float(item.metadata.get("duration")),
                item.metadata.get("duration_unit"),
                _try_float(item.metadata.get("total_distance")),
                item.metadata.get("total_distance_unit"),
                _try_float(item.metadata.get("total_energy_burned")),
                item.metadata.get("total_energy_burned_unit"),
                item.metadata.get("source_name") or payload.device_id,
                item.metadata.get("source_version"),
                json.dumps(device_payload, ensure_ascii=False, sort_keys=True) if device_payload else None,
                start_at,
                start_at,
                end_at,
                start_at.date(),
            ),
        )
        inserted += max(cur.rowcount, 0)
    return inserted


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
        raise HTTPException(status_code=503, detail="INGEST_API_TOKEN 未配置，拒绝写入")
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


def _insert_failed_ingest_event(cur, *, payload: IngestPayload, accepted_count: int, error_message: str, payload_json: str) -> None:
    cur.execute(
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


def ingest_samples(payload: IngestPayload, authorization: str | None) -> dict:
    require_ingest_token(authorization)

    unsupported_items = [item.kind for item in payload.items if item.kind not in SUPPORTED_INGEST_KINDS]
    if unsupported_items:
        raise HTTPException(400, f"暂不支持的 ingest kind: {', '.join(sorted(set(unsupported_items)))}")

    sample_items = [item for item in payload.items if item.kind == "sample"]
    workout_items = [item for item in payload.items if item.kind == "workout"]

    payload_json = serialize_payload(payload)
    accepted_count = len(payload.items)
    deduplicated_count = 0
    event_id: int | None = None

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

            # --- Insert sample items into health_records ---
            rows = [health_record_row_from_ingest(payload, item) for item in sample_items]
            sample_inserted = 0
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
                    sample_inserted = max(cur.rowcount, 0)

            # --- Insert workout items into workouts table ---
            workout_inserted = insert_workouts_from_ingest(cur, payload, workout_items)

            inserted_count = sample_inserted + workout_inserted
            deduplicated_count = accepted_count - inserted_count
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
            if event_id is not None:
                failed_cur.execute(
                    """
                    UPDATE ingest_events
                    SET accepted_count=0, deduplicated_count=0, status='failed', error_message=%s
                    WHERE id=%s
                    """,
                    (error_message, event_id),
                )
                if failed_cur.rowcount <= 0:
                    _insert_failed_ingest_event(
                        failed_cur,
                        payload=payload,
                        accepted_count=accepted_count,
                        error_message=error_message,
                        payload_json=payload_json,
                    )
            else:
                _insert_failed_ingest_event(
                    failed_cur,
                    payload=payload,
                    accepted_count=accepted_count,
                    error_message=error_message,
                    payload_json=payload_json,
                )
        raise HTTPException(500, f"ingest failed: {error_message}") from exc

    _clear_caches()
    return {"ok": True, "accepted": accepted_count, "deduplicated": deduplicated_count}
