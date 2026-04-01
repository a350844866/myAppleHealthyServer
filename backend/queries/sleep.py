from __future__ import annotations

from typing import Optional

from backend.utils import build_date_filters, rows_to_list


def query_sleep_stage_rows(cur, *, start: Optional[str] = None, end: Optional[str] = None) -> list[dict]:
    date_conditions, date_params = build_date_filters("local_date", start, end)
    extra_where = (" AND " + " AND ".join(date_conditions)) if date_conditions else ""
    cur.execute(
        f"""
        WITH sleep_rows AS (
            SELECT local_date, value_text, start_at, end_at
            FROM health_records
            WHERE type = %s
              {extra_where}
        ),
        stage_presence AS (
            SELECT local_date,
                   MAX(value_text IN (%s, %s, %s)) AS has_staged_sleep
            FROM sleep_rows
            GROUP BY local_date
        )
        SELECT sr.local_date AS date,
               sr.value_text AS stage,
               ROUND(SUM(TIMESTAMPDIFF(SECOND, sr.start_at, sr.end_at)) / 60.0, 1) AS minutes
        FROM sleep_rows sr
        JOIN stage_presence sp ON sp.local_date = sr.local_date
        WHERE sr.value_text IN (%s, %s, %s, %s, %s)
          AND (
              sr.value_text <> %s
              OR sp.has_staged_sleep = 0
          )
        GROUP BY sr.local_date, sr.value_text
        ORDER BY sr.local_date
        """,
        [
            "HKCategoryTypeIdentifierSleepAnalysis",
            *date_params,
            "HKCategoryValueSleepAnalysisAsleepCore",
            "HKCategoryValueSleepAnalysisAsleepDeep",
            "HKCategoryValueSleepAnalysisAsleepREM",
            "HKCategoryValueSleepAnalysisAsleepCore",
            "HKCategoryValueSleepAnalysisAsleepDeep",
            "HKCategoryValueSleepAnalysisAsleepREM",
            "HKCategoryValueSleepAnalysisAsleepUnspecified",
            "HKCategoryValueSleepAnalysisAwake",
            "HKCategoryValueSleepAnalysisAsleepUnspecified",
        ],
    )
    return rows_to_list(cur.fetchall())


def query_sleep_daily_rows(cur, *, start: Optional[str] = None, end: Optional[str] = None) -> list[dict]:
    date_conditions, date_params = build_date_filters("local_date", start, end)
    extra_where = (" AND " + " AND ".join(date_conditions)) if date_conditions else ""
    cur.execute(
        f"""
        WITH sleep_rows AS (
            SELECT local_date, value_text, start_at, end_at
            FROM health_records
            WHERE type = %s
              {extra_where}
        ),
        stage_presence AS (
            SELECT local_date,
                   MAX(value_text IN (%s, %s, %s)) AS has_staged_sleep
            FROM sleep_rows
            GROUP BY local_date
        )
        SELECT sr.local_date AS date,
               SUM(TIMESTAMPDIFF(SECOND, sr.start_at, sr.end_at)) / 3600.0 AS total_hours,
               MIN(sr.start_at) AS sleep_start,
               MAX(sr.end_at) AS sleep_end
        FROM sleep_rows sr
        JOIN stage_presence sp ON sp.local_date = sr.local_date
        WHERE sr.value_text NOT IN (%s, %s)
          AND (
              sr.value_text <> %s
              OR sp.has_staged_sleep = 0
          )
        GROUP BY sr.local_date
        ORDER BY sr.local_date
        """,
        [
            "HKCategoryTypeIdentifierSleepAnalysis",
            *date_params,
            "HKCategoryValueSleepAnalysisAsleepCore",
            "HKCategoryValueSleepAnalysisAsleepDeep",
            "HKCategoryValueSleepAnalysisAsleepREM",
            "HKCategoryValueSleepAnalysisInBed",
            "HKCategoryValueSleepAnalysisAwake",
            "HKCategoryValueSleepAnalysisAsleepUnspecified",
        ],
    )
    return rows_to_list(cur.fetchall())
