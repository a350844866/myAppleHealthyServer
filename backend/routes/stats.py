from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from backend.database import get_db
from backend.responses import api_response, list_response
from backend.services.summary_service import get_overview_summary, refresh_all_summaries
from backend.utils import rows_to_list

router = APIRouter()


@router.get("/api/stats/overview")
def get_overview():
    return api_response(get_overview_summary())


@router.post("/api/system-summary/refresh")
def refresh_system_summary():
    return api_response(refresh_all_summaries())


@router.get("/api/stats/today")
def get_today_stats():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN type=%s AND local_date=CURDATE() THEN value_num END), 0) AS steps,
                COALESCE(SUM(CASE WHEN type=%s AND local_date=CURDATE() THEN value_num END), 0) AS active_calories,
                AVG(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_avg,
                MIN(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_min,
                MAX(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_max,
                COUNT(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN 1 END) AS hr_count,
                SUM(CASE WHEN local_date=CURDATE() THEN 1 ELSE 0 END) AS today_records,
                COUNT(DISTINCT CASE WHEN local_date=CURDATE() THEN type END) AS today_types
            FROM health_records
            WHERE local_date=CURDATE()
            """,
            [
                "HKQuantityTypeIdentifierStepCount",
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
            ],
        )
        today_row = cur.fetchone() or {}

        cur.execute("SELECT COUNT(*) AS cnt FROM workouts WHERE local_date=CURDATE()")
        workouts = cur.fetchone()["cnt"]

        cur.execute(
            """
            SELECT MAX(received_at) AS last_sync_at, COUNT(*) AS sync_count, SUM(accepted_count) AS total_accepted
            FROM ingest_events
            WHERE DATE(received_at)=CURDATE() AND status='completed'
            """
        )
        sync_row = cur.fetchone()

        cur.execute("SELECT MAX(received_at) AS last_sync_at FROM ingest_events WHERE status='completed'")
        last_sync = cur.fetchone()

    return api_response({
        "steps": int(today_row["steps"] or 0),
        "active_calories": round(today_row["active_calories"] or 0),
        "heart_rate": {
            "avg": round(today_row["hr_avg"], 1) if today_row["hr_avg"] else None,
            "min": round(today_row["hr_min"], 1) if today_row["hr_min"] else None,
            "max": round(today_row["hr_max"], 1) if today_row["hr_max"] else None,
            "count": today_row["hr_count"],
        },
        "workouts": workouts,
        "today_records": today_row["today_records"] or 0,
        "today_types": today_row["today_types"] or 0,
        "today_sync_count": sync_row["sync_count"] or 0,
        "today_sync_accepted": int(sync_row["total_accepted"] or 0),
        "today_last_sync_at": sync_row["last_sync_at"],
        "last_sync_at": last_sync["last_sync_at"] if last_sync else None,
    })


@router.get("/api/stats/monthly")
def get_monthly_stats(year: Optional[int] = Query(None)):
    conditions = ["local_date IS NOT NULL"]
    params: list = []
    if year:
        conditions.append("YEAR(local_date) = %s")
        params.append(year)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT DATE_FORMAT(local_date, '%%Y-%%m') AS month,
                   SUM(CASE WHEN type=%s THEN value_num END) AS steps,
                   AVG(CASE WHEN type=%s THEN value_num END) AS avg_heart_rate,
                   SUM(CASE WHEN type=%s THEN value_num END) AS active_calories,
                   AVG(CASE WHEN type=%s THEN value_num END) AS avg_spo2
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY DATE_FORMAT(local_date, '%%Y-%%m')
            ORDER BY month
            """,
            [
                "HKQuantityTypeIdentifierStepCount",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierOxygenSaturation",
                *params,
            ],
        )
        return list_response(rows_to_list(cur.fetchall()))
