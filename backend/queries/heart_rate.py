from __future__ import annotations

from typing import Optional

from backend.utils import build_date_filters, rows_to_list


def query_daily_heart_rate_rows(cur, *, start: Optional[str] = None, end: Optional[str] = None) -> list[dict]:
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = ["HKQuantityTypeIdentifierHeartRate"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    cur.execute(
        f"""
        SELECT local_date AS date,
               AVG(value_num) AS avg_bpm,
               MIN(value_num) AS min_bpm,
               MAX(value_num) AS max_bpm,
               COUNT(*) AS count
        FROM health_records
        WHERE {" AND ".join(conditions)}
        GROUP BY local_date
        ORDER BY local_date
        """,
        params,
    )
    return rows_to_list(cur.fetchall())
