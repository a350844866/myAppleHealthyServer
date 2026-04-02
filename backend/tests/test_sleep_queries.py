from __future__ import annotations

from backend.queries.sleep import query_sleep_daily_rows, query_sleep_stage_rows
from backend.tests.conftest import ScriptedCursor


def test_query_sleep_daily_rows_limits_outside_window_unspecified_to_staged_source():
    cursor = ScriptedCursor([{"match": "WITH sleep_rows AS", "fetchall": []}])

    rows = query_sleep_daily_rows(cursor, start="2026-04-01", end="2026-04-02")

    assert rows == []
    cursor.assert_finished()
    executed = cursor.executed[0]
    sql = executed["sql"]
    assert "MIN(source_name) AS staged_source" in sql
    assert "sr.source_name = st.staged_source" in sql
    assert "OR NOT EXISTS (" in sql
    assert "hr2.source_name = st.staged_source" in sql
    assert "hr2.local_date = sr.local_date" in sql
    assert executed["params"][:3] == [
        "HKCategoryTypeIdentifierSleepAnalysis",
        "2026-04-01",
        "2026-04-02",
    ]
    assert executed["params"][-2:] == [
        "HKCategoryTypeIdentifierSleepAnalysis",
        "HKCategoryValueSleepAnalysisAsleepUnspecified",
    ]


def test_query_sleep_stage_rows_limits_outside_window_unspecified_to_staged_source():
    cursor = ScriptedCursor([{"match": "WITH sleep_rows AS", "fetchall": []}])

    rows = query_sleep_stage_rows(cursor, start="2026-04-01", end="2026-04-02")

    assert rows == []
    cursor.assert_finished()
    executed = cursor.executed[0]
    sql = executed["sql"]
    assert "MIN(source_name) AS staged_source" in sql
    assert "sr.source_name = st.staged_source" in sql
    assert "OR NOT EXISTS (" in sql
    assert "hr2.source_name = st.staged_source" in sql
    assert "hr2.local_date = sr.local_date" in sql
    assert executed["params"][:3] == [
        "HKCategoryTypeIdentifierSleepAnalysis",
        "2026-04-01",
        "2026-04-02",
    ]
    assert executed["params"][-2:] == [
        "HKCategoryTypeIdentifierSleepAnalysis",
        "HKCategoryValueSleepAnalysisAsleepUnspecified",
    ]
