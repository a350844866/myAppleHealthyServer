from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, Query

from backend.database import get_db
from backend.queries.heart_rate import query_daily_heart_rate_rows
from backend.queries.sleep import query_sleep_daily_rows, query_sleep_stage_rows
from backend.responses import api_response, list_response
from backend.services.summary_service import get_record_type_stats
from backend.services.step_service import (
    STEP_COUNT_TYPE,
    query_preferred_quantity_daily_rows,
    query_preferred_quantity_hourly_rows,
    query_preferred_step_daily_rows,
    query_preferred_step_hourly_rows,
    uses_preferred_source_resolution,
)
from backend.utils import build_date_filters, build_sample_anchor_sql, rows_to_list, stddev

router = APIRouter()


@router.get("/api/records/types")
def list_record_types():
    rows = get_record_type_stats()
    return list_response(rows)


@router.get("/api/records/recent")
def get_recent_records(
    device_id: str = Query(...),
    bundle_id: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
):
    conditions = ["JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_device_id')) = %s"]
    params: list = [device_id]
    if bundle_id:
        conditions.append("JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_bundle_id')) = %s")
        params.append(bundle_id)
    if type:
        conditions.append("type = %s")
        params.append(type)

    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    where = " AND ".join(conditions)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                id, type, source_name, source_version, unit, value_text, value_num,
                start_at, end_at, local_date, metadata,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_device_id')) AS bridge_device_id,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_bundle_id')) AS bridge_bundle_id,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_sent_at')) AS bridge_sent_at,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_kind')) AS bridge_kind,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_source')) AS bridge_source
            FROM health_records
            WHERE {where}
            ORDER BY start_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total FROM health_records WHERE {where}", params)
        total = cur.fetchone()["total"]
    return list_response(rows_to_list(rows), total=total, limit=limit, offset=offset)


@router.get("/api/records")
def get_records(
    type: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    limit: int = Query(1000, le=10000),
    offset: int = Query(0),
):
    conditions = ["type = %s"]
    params: list = [type]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    if source:
        conditions.append("source_name = %s")
        params.append(source)

    where = " AND ".join(conditions)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, type, source_name, unit, value_text, value_num,
                   start_at, end_at, local_date, metadata
            FROM health_records
            WHERE {where}
            ORDER BY start_at
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total FROM health_records WHERE {where}", params)
        total = cur.fetchone()["total"]
    return list_response(rows_to_list(rows), total=total, limit=limit, offset=offset)


@router.get("/api/records/by-source")
def get_records_by_source(
    type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(12, ge=1, le=50),
):
    conditions = []
    params: list = []
    if type:
        conditions.append("type = %s")
        params.append(type)

    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(NULLIF(source_name, ''), 'Unknown') AS source_name,
                COUNT(*) AS record_count,
                COUNT(DISTINCT type) AS distinct_types,
                MIN(local_date) AS first_date,
                MAX(local_date) AS last_date
            FROM health_records
            {where}
            GROUP BY COALESCE(NULLIF(source_name, ''), 'Unknown')
            ORDER BY record_count DESC, last_date DESC
            LIMIT %s
            """,
            params + [limit],
        )
        source_rows = rows_to_list(cur.fetchall())

        cur.execute(
            f"""
            SELECT
                COALESCE(NULLIF(source_name, ''), 'Unknown') AS source_name,
                type,
                COUNT(*) AS record_count
            FROM health_records
            {where}
            GROUP BY COALESCE(NULLIF(source_name, ''), 'Unknown'), type
            ORDER BY source_name, record_count DESC, type
            """,
            params,
        )
        type_rows = rows_to_list(cur.fetchall())

    top_types_by_source: dict[str, list[dict]] = {}
    for row in type_rows:
        source_name = row["source_name"]
        bucket = top_types_by_source.setdefault(source_name, [])
        if len(bucket) < 5:
            bucket.append({"type": row["type"], "record_count": int(row["record_count"] or 0)})

    for row in source_rows:
        row["record_count"] = int(row["record_count"] or 0)
        row["distinct_types"] = int(row["distinct_types"] or 0)
        row["top_types"] = top_types_by_source.get(row["source_name"], [])

    return list_response(source_rows, limit=limit, type=type, start=start, end=end)


@router.get("/api/records/daily")
def get_daily_records(
    type: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    agg: Literal["sum", "avg", "max", "min", "count"] = Query("sum"),
):
    if type == STEP_COUNT_TYPE and agg == "sum":
        with get_db() as db, db.cursor() as cur:
            rows = query_preferred_step_daily_rows(cur, start=start, end=end)
            return list_response(
                [{"date": row["date"], "value": row["steps"], "count": row["count"], "unit": row["unit"]} for row in rows],
                type=type,
                agg=agg,
            )

    if uses_preferred_source_resolution(type, agg=agg):
        with get_db() as db, db.cursor() as cur:
            rows = query_preferred_quantity_daily_rows(cur, metric_type=type, start=start, end=end)
            return list_response(
                [
                    {"date": row["date"], "value": row["value"], "count": row["count"], "unit": row["unit"]}
                    for row in rows
                ],
                type=type,
                agg=agg,
            )

    # Safe: agg_sql is selected from a fixed whitelist, never from raw user SQL.
    agg_sql = {
        "sum": "SUM(value_num)",
        "avg": "AVG(value_num)",
        "max": "MAX(value_num)",
        "min": "MIN(value_num)",
        "count": "COUNT(*)",
    }[agg]
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = [type]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date, {agg_sql} AS value, COUNT(*) AS count, MIN(unit) AS unit
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date
            ORDER BY local_date
            """,
            params,
        )
        return list_response(rows_to_list(cur.fetchall()), type=type, agg=agg)


@router.get("/api/records/hourly")
def get_hourly_records(
    type: str = Query(...),
    date: Optional[str] = Query(None, description="YYYY-MM-DD, defaults to today"),
    agg: Literal["sum", "avg", "max", "min", "count"] = Query("sum"),
):
    if type == STEP_COUNT_TYPE and agg == "sum":
        with get_db() as db, db.cursor() as cur:
            rows = query_preferred_step_hourly_rows(cur, date=date)
            return list_response(
                [{"hour": row["hour"], "value": row["value"], "count": row["count"], "unit": row["unit"]} for row in rows],
                type=type,
                agg=agg,
                date=date or "today",
            )

    if uses_preferred_source_resolution(type, agg=agg):
        with get_db() as db, db.cursor() as cur:
            rows = query_preferred_quantity_hourly_rows(cur, metric_type=type, date=date)
            return list_response(
                [{"hour": row["hour"], "value": row["value"], "count": row["count"], "unit": row["unit"]} for row in rows],
                type=type,
                agg=agg,
                date=date or "today",
            )

    # Safe: agg_sql is selected from a fixed whitelist, never from raw user SQL.
    agg_sql = {
        "sum": "SUM(value_num)",
        "avg": "AVG(value_num)",
        "max": "MAX(value_num)",
        "min": "MIN(value_num)",
        "count": "COUNT(*)",
    }[agg]
    sample_anchor_sql = build_sample_anchor_sql()
    date_filter = "local_date = CURDATE()" if not date else "local_date = %s"
    params: list = [type] + ([date] if date else [])

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT HOUR({sample_anchor_sql}) AS hour, {agg_sql} AS value, COUNT(*) AS count, MIN(unit) AS unit
            FROM health_records
            WHERE type = %s AND value_num IS NOT NULL AND {date_filter}
            GROUP BY HOUR({sample_anchor_sql})
            ORDER BY hour
            """,
            params,
        )
        return list_response(rows_to_list(cur.fetchall()), type=type, agg=agg, date=date or "today")


@router.get("/api/steps")
def get_steps(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records(STEP_COUNT_TYPE, start, end, "sum")


@router.get("/api/heart-rate")
def get_heart_rate(
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    granularity: Literal["raw", "hourly", "daily"] = Query("daily"),
):
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = ["HKQuantityTypeIdentifierHeartRate"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    where = " AND ".join(conditions)
    sample_anchor_sql = build_sample_anchor_sql()

    with get_db() as db, db.cursor() as cur:
        if granularity == "raw":
            cur.execute(
                f"""
                SELECT start_at, value_num AS bpm, source_name
                FROM health_records
                WHERE {where}
                ORDER BY start_at
                LIMIT 5000
                """,
                params,
            )
            return list_response(rows_to_list(cur.fetchall()), type="HKQuantityTypeIdentifierHeartRate", granularity=granularity)
        if granularity == "hourly":
            cur.execute(
                f"""
                SELECT DATE_FORMAT({sample_anchor_sql}, '%%Y-%%m-%%d %%H:00:00') AS hour,
                       AVG(value_num) AS avg_bpm,
                       MIN(value_num) AS min_bpm,
                       MAX(value_num) AS max_bpm,
                       COUNT(*) AS count
                FROM health_records
                WHERE {where}
                GROUP BY DATE_FORMAT({sample_anchor_sql}, '%%Y-%%m-%%d %%H')
                ORDER BY hour
                """,
                params,
            )
            return list_response(rows_to_list(cur.fetchall()), type="HKQuantityTypeIdentifierHeartRate", granularity=granularity)
        return list_response(query_daily_heart_rate_rows(cur, start=start, end=end), type="HKQuantityTypeIdentifierHeartRate", granularity=granularity)


@router.get("/api/heart-rate/variability")
def get_hrv(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", start, end, "avg")


@router.get("/api/sleep")
def get_sleep(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    conditions = ["type = %s"]
    params: list = ["HKCategoryTypeIdentifierSleepAnalysis"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date,
                   value_text AS value,
                   SUM(TIMESTAMPDIFF(SECOND, start_at, end_at)) / 60.0 AS minutes,
                   COUNT(*) AS segments
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date, value_text
            ORDER BY local_date, value_text
            """,
            params,
        )
        return list_response(rows_to_list(cur.fetchall()), type="HKCategoryTypeIdentifierSleepAnalysis")


@router.get("/api/sleep/daily")
def get_sleep_daily(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    with get_db() as db, db.cursor() as cur:
        return list_response(query_sleep_daily_rows(cur, start=start, end=end), type="HKCategoryTypeIdentifierSleepAnalysis", granularity="daily")


@router.get("/api/body-metrics")
def get_body_metrics(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    types = [
        "HKQuantityTypeIdentifierBodyMass",
        "HKQuantityTypeIdentifierBodyMassIndex",
        "HKQuantityTypeIdentifierBodyFatPercentage",
        "HKQuantityTypeIdentifierLeanBodyMass",
        "HKQuantityTypeIdentifierHeight",
    ]
    placeholders = ", ".join(["%s"] * len(types))
    conditions = [f"type IN ({placeholders})"]
    params: list = list(types)
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT type, unit, value_num AS value, start_at, local_date AS date, source_name
            FROM health_records
            WHERE {" AND ".join(conditions)}
            ORDER BY start_at
            """,
            params,
        )
        return list_response(rows_to_list(cur.fetchall()))


@router.get("/api/energy")
def get_energy(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    with get_db() as db, db.cursor() as cur:
        active_rows = query_preferred_quantity_daily_rows(
            cur,
            metric_type="HKQuantityTypeIdentifierActiveEnergyBurned",
            start=start,
            end=end,
        )
        basal_rows = query_preferred_quantity_daily_rows(
            cur,
            metric_type="HKQuantityTypeIdentifierBasalEnergyBurned",
            start=start,
            end=end,
        )

    by_date: dict[str, dict] = {}
    for row in active_rows:
        bucket = by_date.setdefault(row["date"], {"date": row["date"], "active_cal": 0.0, "basal_cal": 0.0})
        bucket["active_cal"] = row["value"]
    for row in basal_rows:
        bucket = by_date.setdefault(row["date"], {"date": row["date"], "active_cal": 0.0, "basal_cal": 0.0})
        bucket["basal_cal"] = row["value"]
    return list_response([by_date[key] for key in sorted(by_date)])


@router.get("/api/oxygen-saturation")
def get_spo2(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierOxygenSaturation", start, end, "avg")


@router.get("/api/respiratory-rate")
def get_respiratory_rate(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierRespiratoryRate", start, end, "avg")


@router.get("/api/vo2max")
def get_vo2max():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT local_date AS date, value_num AS value, unit, source_name
            FROM health_records
            WHERE type=%s
            ORDER BY local_date
            """,
            ("HKQuantityTypeIdentifierVO2Max",),
        )
        return list_response(rows_to_list(cur.fetchall()))


@router.get("/api/sleep/quality")
def get_sleep_quality(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    with get_db() as db, db.cursor() as cur:
        stage_rows = query_sleep_stage_rows(cur, start=start, end=end)
        nightly_rows = query_sleep_daily_rows(cur, start=start, end=end)

    stage_labels = {
        "HKCategoryValueSleepAnalysisAsleepCore": "core",
        "HKCategoryValueSleepAnalysisAsleepDeep": "deep",
        "HKCategoryValueSleepAnalysisAsleepREM": "rem",
        "HKCategoryValueSleepAnalysisAsleepUnspecified": "unspecified",
        "HKCategoryValueSleepAnalysisAwake": "awake",
    }
    per_day = {str(row["date"]): {
        "date": str(row["date"]),
        "sleep_start": row.get("sleep_start"),
        "sleep_end": row.get("sleep_end"),
        "total_hours": float(row.get("total_hours") or 0),
        "stages": {"core": 0.0, "deep": 0.0, "rem": 0.0, "unspecified": 0.0, "awake": 0.0},
        "score": None,
    } for row in nightly_rows}

    for row in stage_rows:
        date = str(row["date"])
        if date not in per_day:
            per_day[date] = {
                "date": date,
                "sleep_start": None,
                "sleep_end": None,
                "total_hours": 0.0,
                "stages": {"core": 0.0, "deep": 0.0, "rem": 0.0, "unspecified": 0.0, "awake": 0.0},
                "score": None,
            }
        key = stage_labels.get(row["stage"])
        if key:
            per_day[date]["stages"][key] = float(row.get("minutes") or 0)

    results = []
    sleep_starts = []
    sleep_ends = []
    for item in sorted(per_day.values(), key=lambda x: x["date"]):
        asleep_minutes = (
            item["stages"]["core"]
            + item["stages"]["deep"]
            + item["stages"]["rem"]
            + item["stages"]["unspecified"]
        )
        total_tracked = asleep_minutes + item["stages"]["awake"]
        efficiency = asleep_minutes / total_tracked if total_tracked > 0 else None
        deep_ratio = item["stages"]["deep"] / asleep_minutes if asleep_minutes > 0 else None
        rem_ratio = item["stages"]["rem"] / asleep_minutes if asleep_minutes > 0 else None
        score = 0.0
        if item["total_hours"] >= 7:
            score += 40
        elif item["total_hours"] >= 6:
            score += 28
        elif item["total_hours"] >= 5:
            score += 18
        if efficiency is not None:
            score += min(30, max(0, efficiency * 30))
        if deep_ratio is not None:
            score += min(15, max(0, deep_ratio * 100 * 0.8))
        if rem_ratio is not None:
            score += min(15, max(0, rem_ratio * 100 * 0.6))
        item["efficiency"] = round(efficiency, 3) if efficiency is not None else None
        item["deep_ratio"] = round(deep_ratio, 3) if deep_ratio is not None else None
        item["rem_ratio"] = round(rem_ratio, 3) if rem_ratio is not None else None
        item["score"] = round(min(score, 100), 1)
        results.append(item)
        if item["sleep_start"]:
            sleep_starts.append(item["sleep_start"])
        if item["sleep_end"]:
            sleep_ends.append(item["sleep_end"])

    def minutes_of_day(dt):
        return dt.hour * 60 + dt.minute if dt else None

    def bedtime_minutes(dt):
        minute = minutes_of_day(dt)
        if minute is None:
            return None
        return minute + 1440 if minute < 12 * 60 else minute

    start_minutes = [bedtime_minutes(item) for item in sleep_starts if item]
    end_minutes = [minutes_of_day(item) for item in sleep_ends if item]
    avg_score = round(sum(item["score"] for item in results) / len(results), 1) if results else None
    bedtime_std_minutes = stddev(start_minutes)
    wake_std_minutes = stddev(end_minutes)
    regularity_penalty = 0.0
    if bedtime_std_minutes is not None:
        regularity_penalty += min(35.0, bedtime_std_minutes / 2.0)
    if wake_std_minutes is not None:
        regularity_penalty += min(25.0, wake_std_minutes / 2.4)
    regularity_score = max(0.0, 100.0 - regularity_penalty) if results else None

    summary = {
        "nights": len(results),
        "avg_score": avg_score,
        "avg_total_hours": round(sum(item["total_hours"] for item in results) / len(results), 2) if results else None,
        "avg_bedtime_minutes": round(sum(start_minutes) / len(start_minutes), 1) % 1440 if start_minutes else None,
        "avg_wake_minutes": round(sum(end_minutes) / len(end_minutes), 1) if end_minutes else None,
        "bedtime_std_minutes": round(bedtime_std_minutes, 1) if bedtime_std_minutes is not None else None,
        "wake_std_minutes": round(wake_std_minutes, 1) if wake_std_minutes is not None else None,
        "regularity_score": round(regularity_score, 1) if regularity_score is not None else None,
    }
    return api_response({"summary": summary, "nights": results})
