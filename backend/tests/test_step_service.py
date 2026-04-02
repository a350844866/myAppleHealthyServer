from __future__ import annotations

from datetime import date

from backend.services.step_service import (
    query_preferred_quantity_daily_rows,
    query_preferred_quantity_hourly_rows,
    query_preferred_quantity_total,
    query_preferred_step_daily_rows,
    query_preferred_step_hourly_rows,
    query_preferred_step_total,
    rollup_quantity_monthly,
    rollup_step_monthly,
)
from backend.tests.conftest import ScriptedCursor


def test_query_preferred_step_daily_rows_prefers_watch_source():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 3587,
                    "count": 27,
                    "unit": "count",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "value": 2992,
                    "count": 12,
                    "unit": "count",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
                {
                    "date": date(2026, 4, 3),
                    "source_name": "尊贵的华为三联屏",
                    "value": 4123,
                    "count": 13,
                    "unit": "count",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
            ],
        }
    ])

    rows = query_preferred_step_daily_rows(cursor, start="2026-04-02", end="2026-04-03")

    assert rows == [
        {"date": "2026-04-02", "steps": 3587, "count": 27, "unit": "count", "source_name": "贾诩的Apple Watch"},
        {"date": "2026-04-03", "steps": 4123, "count": 13, "unit": "count", "source_name": "尊贵的华为三联屏"},
    ]
    cursor.assert_finished()


def test_query_preferred_step_total_returns_preferred_daily_total():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 3587,
                    "count": 27,
                    "unit": "count",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "value": 2992,
                    "count": 12,
                    "unit": "count",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
            ],
        }
    ])

    assert query_preferred_step_total(cursor, date="2026-04-02") == 3587
    cursor.assert_finished()


def test_query_preferred_step_hourly_rows_filters_to_preferred_source():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "steps": 3587,
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "steps": 2992,
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
            ],
        },
        {
            "match": "GROUP BY HOUR(start_at), COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {"hour": 7, "source_name": "贾诩的Apple Watch", "value": 897, "count": 3, "unit": "count"},
                {"hour": 7, "source_name": "尊贵的华为三联屏", "value": 821, "count": 2, "unit": "count"},
                {"hour": 13, "source_name": "尊贵的华为三联屏", "value": 16, "count": 1, "unit": "count"},
            ],
        },
    ])

    rows = query_preferred_step_hourly_rows(cursor, date="2026-04-02")

    assert rows == [
        {"hour": 7, "value": 897.0, "count": 3, "unit": "count", "source_name": "贾诩的Apple Watch"},
    ]
    cursor.assert_finished()


def test_query_preferred_quantity_daily_rows_prefers_watch_for_distance():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 2694.8745909542777,
                    "count": 27,
                    "unit": "m",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "value": 1908.8599999989383,
                    "count": 12,
                    "unit": "m",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
            ],
        }
    ])

    rows = query_preferred_quantity_daily_rows(
        cursor,
        metric_type="HKQuantityTypeIdentifierDistanceWalkingRunning",
        start="2026-04-02",
        end="2026-04-02",
    )

    assert rows == [
        {
            "date": "2026-04-02",
            "value": 2694.8745909542777,
            "count": 27,
            "unit": "m",
            "source_name": "贾诩的Apple Watch",
        }
    ]
    cursor.assert_finished()


def test_query_preferred_quantity_total_uses_preferred_source():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 246.192,
                    "count": 101,
                    "unit": "kcal",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "value": 190.0,
                    "count": 80,
                    "unit": "kcal",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
            ],
        }
    ])

    assert query_preferred_quantity_total(
        cursor,
        metric_type="HKQuantityTypeIdentifierActiveEnergyBurned",
        date="2026-04-02",
    ) == 246.192
    cursor.assert_finished()


def test_query_preferred_quantity_hourly_rows_filters_to_preferred_source():
    cursor = ScriptedCursor([
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "尊贵的华为三联屏",
                    "value": 4,
                    "count": 2,
                    "unit": "count",
                    "device_name": "iPhone",
                    "product_type": "iPhone17,1",
                },
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 3,
                    "count": 2,
                    "unit": "count",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                },
            ],
        },
        {
            "match": "GROUP BY HOUR(start_at), COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {"hour": 9, "source_name": "尊贵的华为三联屏", "value": 2, "count": 1, "unit": "count"},
                {"hour": 9, "source_name": "贾诩的Apple Watch", "value": 1, "count": 1, "unit": "count"},
                {"hour": 14, "source_name": "尊贵的华为三联屏", "value": 2, "count": 1, "unit": "count"},
            ],
        },
    ])

    rows = query_preferred_quantity_hourly_rows(
        cursor,
        metric_type="HKQuantityTypeIdentifierFlightsClimbed",
        date="2026-04-02",
    )

    assert rows == [
        {"hour": 9, "value": 2.0, "count": 1, "unit": "count", "source_name": "尊贵的华为三联屏"},
        {"hour": 14, "value": 2.0, "count": 1, "unit": "count", "source_name": "尊贵的华为三联屏"},
    ]
    cursor.assert_finished()


def test_rollup_step_monthly_sums_daily_rows():
    daily_rows = [
        {"date": "2026-04-01", "steps": 1000},
        {"date": "2026-04-02", "steps": 2000},
        {"date": "2026-05-01", "steps": 3000},
    ]

    assert rollup_step_monthly(daily_rows) == {
        "2026-04": 3000,
        "2026-05": 3000,
    }


def test_rollup_quantity_monthly_sums_daily_rows():
    daily_rows = [
        {"date": "2026-04-01", "value": 100.5},
        {"date": "2026-04-02", "value": 200.25},
        {"date": "2026-05-01", "value": 300.0},
    ]

    assert rollup_quantity_monthly(daily_rows) == {
        "2026-04": 300.75,
        "2026-05": 300.0,
    }
