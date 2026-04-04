from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from backend.config import LOCAL_TIMEZONE
from backend.utils import as_int, build_date_filters, rows_to_list


STEP_COUNT_TYPE = "HKQuantityTypeIdentifierStepCount"
PREFERRED_SOURCE_SUM_TYPES = {
    STEP_COUNT_TYPE,
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierBasalEnergyBurned",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKQuantityTypeIdentifierDistanceCycling",
    "HKQuantityTypeIdentifierDistanceSwimming",
    "HKQuantityTypeIdentifierDistanceWheelchair",
    "HKQuantityTypeIdentifierDistanceDownhillSnowSports",
    "HKQuantityTypeIdentifierFlightsClimbed",
    "HKQuantityTypeIdentifierAppleExerciseTime",
    "HKQuantityTypeIdentifierAppleStandTime",
    "HKQuantityTypeIdentifierSwimmingStrokeCount",
    "HKQuantityTypeIdentifierPushCount",
    "HKQuantityTypeIdentifierTimeInDaylight",
}


def _normalize_date(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _step_source_priority(row: dict[str, Any]) -> int:
    source_name = str(row.get("source_name") or "").lower()
    device_name = str(row.get("device_name") or "").lower()
    product_type = str(row.get("product_type") or "").lower()

    if "watch" in source_name or device_name == "apple watch" or product_type.startswith("watch"):
        return 0
    if "iphone" in source_name or device_name == "iphone" or product_type.startswith("iphone"):
        return 1
    return 2


def _pick_preferred_row(rows: list[dict[str, Any]], *, value_key: str) -> dict[str, Any] | None:
    if not rows:
        return None
    return min(
        rows,
        key=lambda row: (
            -float(row.get(value_key) or 0),
            _step_source_priority(row),
            str(row.get("source_name") or ""),
        ),
    )


def uses_preferred_source_resolution(metric_type: str, *, agg: str = "sum") -> bool:
    return agg == "sum" and metric_type in PREFERRED_SOURCE_SUM_TYPES


def query_preferred_quantity_daily_rows(
    cur,
    *,
    metric_type: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list[Any] = [metric_type]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    cur.execute(
        f"""
        SELECT
            local_date AS date,
            COALESCE(NULLIF(source_name, ''), 'Unknown') AS source_name,
            SUM(value_num) AS value,
            COUNT(*) AS count,
            MIN(unit) AS unit,
            MIN(JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.device_name'))) AS device_name,
            MIN(JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.product_type'))) AS product_type
        FROM health_records
        WHERE {" AND ".join(conditions)}
        GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')
        ORDER BY local_date, value DESC, source_name
        """,
        params,
    )
    grouped_rows = rows_to_list(cur.fetchall())

    rows_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in grouped_rows:
        rows_by_date[_normalize_date(row.get("date"))].append(row)

    preferred_rows: list[dict[str, Any]] = []
    for date_key in sorted(rows_by_date):
        chosen = _pick_preferred_row(rows_by_date[date_key], value_key="value")
        if chosen is None:
            continue
        preferred_rows.append({
            "date": date_key,
            "value": float(chosen.get("value") or 0),
            "count": as_int(chosen.get("count")),
            "unit": chosen.get("unit"),
            "source_name": chosen.get("source_name"),
        })
    return preferred_rows


def query_preferred_quantity_total(cur, *, metric_type: str, date: str | None = None) -> float:
    target_date = date or datetime.now(LOCAL_TIMEZONE).date().isoformat()
    rows = query_preferred_quantity_daily_rows(cur, metric_type=metric_type, start=target_date, end=target_date)
    if not rows:
        return 0.0
    return float(rows[0].get("value") or 0)


def _sample_anchor(start_at: datetime, end_at: datetime) -> datetime:
    if end_at > start_at:
        return start_at + timedelta(seconds=int((end_at - start_at).total_seconds() // 2))
    return start_at


def _split_value_across_hours(
    *,
    start_at: datetime,
    end_at: datetime,
    value: float,
    window_start: datetime,
    window_end: datetime,
) -> dict[int, float]:
    if end_at <= start_at:
        if window_start <= start_at < window_end:
            return {start_at.hour: value}
        return {}

    total_seconds = (end_at - start_at).total_seconds()
    clipped_start = max(start_at, window_start)
    clipped_end = min(end_at, window_end)
    if clipped_end <= clipped_start or total_seconds <= 0:
        return {}

    distributed: dict[int, float] = defaultdict(float)
    current = clipped_start
    while current < clipped_end:
        next_hour = current.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        chunk_end = min(next_hour, clipped_end)
        overlap_seconds = (chunk_end - current).total_seconds()
        distributed[current.hour] += value * overlap_seconds / total_seconds
        current = chunk_end
    return dict(distributed)


def query_preferred_quantity_hourly_rows(cur, *, metric_type: str, date: str | None = None) -> list[dict[str, Any]]:
    target_date = date or datetime.now(LOCAL_TIMEZONE).date().isoformat()
    preferred_rows = query_preferred_quantity_daily_rows(cur, metric_type=metric_type, start=target_date, end=target_date)
    if not preferred_rows:
        return []
    preferred_source = preferred_rows[0].get("source_name")
    window_start = datetime.fromisoformat(target_date)
    window_end = window_start + timedelta(days=1)

    cur.execute(
        """
        SELECT
            start_at,
            end_at,
            value_num AS value,
            unit
        FROM health_records
        WHERE type = %s
          AND value_num IS NOT NULL
          AND local_date = %s
          AND COALESCE(NULLIF(source_name, ''), 'Unknown') = %s
        ORDER BY start_at, end_at, id
        """,
        [metric_type, target_date, preferred_source],
    )
    source_rows = rows_to_list(cur.fetchall())

    hourly_values: dict[int, float] = defaultdict(float)
    hourly_counts: dict[int, int] = defaultdict(int)
    unit = None
    for row in source_rows:
        start_at = row.get("start_at")
        end_at = row.get("end_at") or start_at
        if not isinstance(start_at, datetime) or not isinstance(end_at, datetime):
            continue
        if unit is None and row.get("unit"):
            unit = row.get("unit")
        value = float(row.get("value") or 0)
        for hour, portion in _split_value_across_hours(
            start_at=start_at,
            end_at=end_at,
            value=value,
            window_start=window_start,
            window_end=window_end,
        ).items():
            hourly_values[hour] += portion

        anchor = _sample_anchor(start_at, end_at)
        if anchor < window_start:
            count_hour = 0
        elif anchor >= window_end:
            count_hour = 23
        else:
            count_hour = anchor.hour
        hourly_counts[count_hour] += 1

    return [
        {
            "hour": hour,
            "value": hourly_values[hour],
            "count": hourly_counts.get(hour, 0),
            "unit": unit,
            "source_name": preferred_source,
        }
        for hour in sorted(hourly_values)
        if hourly_values[hour] > 0
    ]


def rollup_quantity_monthly(daily_rows: list[dict[str, Any]]) -> dict[str, float]:
    monthly_totals: dict[str, float] = defaultdict(float)
    for row in daily_rows:
        month_key = str(row.get("date"))[:7]
        monthly_totals[month_key] += float(row.get("value") or 0)
    return dict(monthly_totals)


def query_preferred_step_daily_rows(cur, *, start: str | None = None, end: str | None = None) -> list[dict[str, Any]]:
    rows = query_preferred_quantity_daily_rows(cur, metric_type=STEP_COUNT_TYPE, start=start, end=end)
    return [
        {
            "date": row["date"],
            "steps": as_int(row.get("value")),
            "count": as_int(row.get("count")),
            "unit": row.get("unit"),
            "source_name": row.get("source_name"),
        }
        for row in rows
    ]


def query_preferred_step_total(cur, *, date: str | None = None) -> int:
    return as_int(query_preferred_quantity_total(cur, metric_type=STEP_COUNT_TYPE, date=date))


def query_preferred_step_hourly_rows(cur, *, date: str | None = None) -> list[dict[str, Any]]:
    rows = query_preferred_quantity_hourly_rows(cur, metric_type=STEP_COUNT_TYPE, date=date)
    return [
        {
            "hour": row.get("hour"),
            "value": float(row.get("value") or 0),
            "count": as_int(row.get("count")),
            "unit": row.get("unit"),
            "source_name": row.get("source_name"),
        }
        for row in rows
    ]


def rollup_step_monthly(daily_rows: list[dict[str, Any]]) -> dict[str, int]:
    monthly_totals = rollup_quantity_monthly([
        {"date": row.get("date"), "value": row.get("steps")}
        for row in daily_rows
    ])
    return {month: as_int(value) for month, value in monthly_totals.items()}
