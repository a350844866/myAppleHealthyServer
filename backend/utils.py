from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from .config import LOCAL_TIMEZONE


def rows_to_list(rows) -> list[dict]:
    return [dict(row) for row in rows]


def row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def round_or_none(value: Any, digits: int = 1) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def as_int(value: Any) -> int:
    return int(value or 0)


def percent_change(current: float | int | None, previous: float | int | None, digits: int = 1) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((float(current) - float(previous)) / float(previous)) * 100, digits)


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    avg = mean(values)
    if avg is None:
        return None
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return variance ** 0.5


def build_date_filters(column: str, start: Optional[str], end: Optional[str]) -> tuple[list[str], list]:
    conditions = []
    params: list = []
    if start:
        conditions.append(f"{column} >= %s")
        params.append(start)
    if end:
        conditions.append(f"{column} <= %s")
        params.append(end)
    return conditions, params


def build_sample_anchor_sql(start_column: str = "start_at", end_column: str = "end_at") -> str:
    return (
        f"CASE WHEN {end_column} > {start_column} "
        f"THEN TIMESTAMPADD(SECOND, TIMESTAMPDIFF(SECOND, {start_column}, {end_column}) DIV 2, {start_column}) "
        f"ELSE {start_column} END"
    )


def compact_dict(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def normalize_ingest_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)


def isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(microsecond=0).isoformat()
    return value.replace(microsecond=0).isoformat()


def format_value_text(value: float | None) -> str | None:
    if value is None:
        return None
    if float(value).is_integer():
        return str(int(value))
    return format(value, ".15g")


def deserialize_json_field(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback
