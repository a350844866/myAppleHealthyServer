from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from backend.cache import overview_cache, record_types_cache
from backend.config import LOCAL_TIMEZONE, SUMMARY_STALE_SECONDS
from backend.database import get_db
from backend.services.schema_service import ensure_summary_tables
from backend.utils import rows_to_list


def _is_stale(refreshed_at, *, max_age_seconds: int) -> bool:
    if not refreshed_at:
        return True
    now = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None)
    return refreshed_at < now - timedelta(seconds=max_age_seconds)


def _ensure_tables() -> None:
    with get_db() as db, db.cursor() as cur:
        ensure_summary_tables(cur)


def _load_summary_row(summary_key: str) -> dict | None:
    _ensure_tables()
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT summary_key, summary_json, refreshed_at
            FROM system_summary
            WHERE summary_key=%s
            """,
            (summary_key,),
        )
        return cur.fetchone()


def _deserialize_summary_json(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return {}


def refresh_record_type_stats() -> dict[str, Any]:
    _ensure_tables()
    with get_db(autocommit=False) as db, db.cursor() as cur:
        cur.execute("DELETE FROM record_type_stats")
        cur.execute(
            """
            INSERT INTO record_type_stats (type, record_count, first_date, last_date, refreshed_at)
            SELECT type, COUNT(*) AS record_count, MIN(local_date) AS first_date, MAX(local_date) AS last_date, CURRENT_TIMESTAMP
            FROM health_records
            GROUP BY type
            """
        )
        inserted = cur.rowcount
    record_types_cache.clear()
    return {"ok": True, "table": "record_type_stats", "rows": int(inserted or 0)}


def refresh_system_summary() -> dict[str, Any]:
    _ensure_tables()
    with get_db(autocommit=False) as db, db.cursor() as cur:
        cur.execute("SELECT * FROM profile WHERE id=1")
        profile = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS total_records,
                   COUNT(DISTINCT type) AS distinct_types,
                   MIN(local_date) AS earliest_date,
                   MAX(local_date) AS latest_date
            FROM health_records
            """
        )
        record_stats = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS total_workouts,
                   ROUND(SUM(duration), 0) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            """
        )
        workout_stats = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(DISTINCT local_date) AS days
            FROM health_records
            WHERE type=%s
            """,
            ("HKCategoryTypeIdentifierSleepAnalysis",),
        )
        sleep_days = cur.fetchone()

        cur.execute(
            """
            SELECT SUM(value_num) AS steps
            FROM health_records
            WHERE type=%s
            """,
            ("HKQuantityTypeIdentifierStepCount",),
        )
        total_steps = cur.fetchone()

        cur.execute(
            """
            SELECT local_date AS date, SUM(value_num) AS steps
            FROM health_records
            WHERE type=%s AND local_date >= (CURDATE() - INTERVAL 7 DAY)
            GROUP BY local_date
            ORDER BY local_date
            """,
            ("HKQuantityTypeIdentifierStepCount",),
        )
        recent_steps = rows_to_list(cur.fetchall())

        payload = {
            "profile": profile,
            "records": record_stats,
            "workouts": workout_stats,
            "sleep_days": sleep_days["days"] if sleep_days else 0,
            "total_steps": int(total_steps["steps"] or 0) if total_steps else 0,
            "recent_steps": recent_steps,
        }

        cur.execute(
            """
            INSERT INTO system_summary (summary_key, summary_json)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                summary_json=VALUES(summary_json),
                refreshed_at=CURRENT_TIMESTAMP
            """,
            ("overview", json.dumps(payload, ensure_ascii=False, default=str)),
        )
    overview_cache.clear()
    return {"ok": True, "table": "system_summary", "summary_key": "overview"}


def refresh_all_summaries() -> dict[str, Any]:
    return {
        "record_types": refresh_record_type_stats(),
        "overview": refresh_system_summary(),
    }


def get_record_type_stats(*, force_refresh: bool = False) -> list[dict]:
    _ensure_tables()
    cached = None if force_refresh else record_types_cache.get("all")
    if cached is not None:
        return cached

    if force_refresh:
        refresh_record_type_stats()
    else:
        with get_db() as db, db.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(refreshed_at) AS refreshed_at, COUNT(*) AS total_rows
                FROM record_type_stats
                """
            )
            meta = cur.fetchone() or {}
        if not meta.get("total_rows") or _is_stale(meta.get("refreshed_at"), max_age_seconds=SUMMARY_STALE_SECONDS):
            refresh_record_type_stats()

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT type, record_count AS count, first_date, last_date
            FROM record_type_stats
            ORDER BY record_count DESC, last_date DESC
            """
        )
        rows = rows_to_list(cur.fetchall())
    return record_types_cache.set("all", rows)


def get_overview_summary(*, force_refresh: bool = False) -> dict:
    _ensure_tables()
    cached = None if force_refresh else overview_cache.get("overview")
    if cached is not None:
        return cached

    row = None if force_refresh else _load_summary_row("overview")
    if force_refresh or not row or _is_stale(row.get("refreshed_at"), max_age_seconds=SUMMARY_STALE_SECONDS):
        refresh_system_summary()
        row = _load_summary_row("overview")

    payload = _deserialize_summary_json(row["summary_json"]) if row else {
        "profile": None,
        "records": None,
        "workouts": None,
        "sleep_days": 0,
        "total_steps": 0,
        "recent_steps": [],
    }
    return overview_cache.set("overview", payload)
