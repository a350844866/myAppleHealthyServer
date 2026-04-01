from __future__ import annotations

from datetime import datetime

import pytest
from fastapi import HTTPException

from backend.models import IngestItem, IngestPayload
from backend.services import ingest_service
from backend.tests.conftest import FakeConnection, ScriptedCursor, build_get_db


def make_payload() -> IngestPayload:
    items = [
        IngestItem(
            source="bridge",
            kind="sample",
            type="HKQuantityTypeIdentifierStepCount",
            uuid="a-1",
            start_at=datetime(2026, 4, 1, 8, 0, 0),
            end_at=datetime(2026, 4, 1, 8, 5, 0),
            value=120,
            unit="count",
            metadata={"source_name": "jiaxu-iphone"},
        ),
        IngestItem(
            source="bridge",
            kind="sample",
            type="HKQuantityTypeIdentifierStepCount",
            uuid="a-2",
            start_at=datetime(2026, 4, 1, 9, 0, 0),
            end_at=datetime(2026, 4, 1, 9, 5, 0),
            value=150,
            unit="count",
            metadata={"source_name": "jiaxu-iphone"},
        ),
        IngestItem(
            source="bridge",
            kind="sample",
            type="HKQuantityTypeIdentifierStepCount",
            uuid="a-3",
            start_at=datetime(2026, 4, 1, 10, 0, 0),
            end_at=datetime(2026, 4, 1, 10, 5, 0),
            value=180,
            unit="count",
            metadata={"source_name": "jiaxu-iphone"},
        ),
    ]
    return IngestPayload(
        device_id="jiaxu-iphone",
        bundle_id="com.example.healthbridge",
        sent_at=datetime(2026, 4, 1, 12, 0, 0),
        items=items,
        anchors={"HKQuantityTypeIdentifierStepCount": "anchor-123"},
    )


def test_ingest_samples_counts_prechecked_and_insert_ignore_dedup(monkeypatch):
    payload = make_payload()
    success_cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO ingest_events", "lastrowid": 91},
            {
                "match": "SELECT type, start_at, end_at, source_name",
                "fetchall": [
                    {
                        "type": "HKQuantityTypeIdentifierStepCount",
                        "start_at": datetime(2026, 4, 1, 8, 0, 0),
                        "end_at": datetime(2026, 4, 1, 8, 5, 0),
                        "source_name": "jiaxu-iphone",
                    }
                ],
            },
            {"method": "executemany", "match": "INSERT IGNORE INTO health_records", "rowcount": 1},
            {"match": "INSERT INTO device_sync_state"},
            {"method": "executemany", "match": "INSERT INTO device_sync_anchors"},
            {"match": "UPDATE ingest_events"},
        ]
    )

    monkeypatch.setattr(
        ingest_service,
        "get_db",
        build_get_db(FakeConnection(success_cursor)),
    )
    monkeypatch.setattr(ingest_service, "_clear_caches", lambda: None)

    result = ingest_service.ingest_samples(payload, authorization=None)

    assert result == {"ok": True, "accepted": 3, "deduplicated": 2}
    success_cursor.assert_finished()


def test_ingest_samples_updates_existing_event_on_failure(monkeypatch):
    payload = make_payload()
    failing_cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO ingest_events", "lastrowid": 55},
            {
                "match": "SELECT type, start_at, end_at, source_name",
                "raise": RuntimeError("boom"),
            },
        ]
    )
    failed_cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO device_sync_state"},
            {"match": "UPDATE ingest_events", "rowcount": 1},
        ]
    )

    monkeypatch.setattr(
        ingest_service,
        "get_db",
        build_get_db(FakeConnection(failing_cursor), FakeConnection(failed_cursor)),
    )
    monkeypatch.setattr(ingest_service, "_clear_caches", lambda: None)

    with pytest.raises(HTTPException) as exc_info:
        ingest_service.ingest_samples(payload, authorization=None)

    assert exc_info.value.status_code == 500
    assert "ingest failed: boom" in exc_info.value.detail
    failed_sql = [entry["sql"] for entry in failed_cursor.executed]
    assert any("UPDATE ingest_events" in sql for sql in failed_sql)
    assert not any(
        "INSERT INTO ingest_events" in sql and "status, error_message" in sql
        for sql in failed_sql
    )
    failing_cursor.assert_finished()
    failed_cursor.assert_finished()


def test_ingest_samples_inserts_failed_event_when_rolled_back_event_missing(monkeypatch):
    payload = make_payload()
    failing_cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO ingest_events", "lastrowid": 77},
            {
                "match": "SELECT type, start_at, end_at, source_name",
                "raise": RuntimeError("boom"),
            },
        ]
    )
    failed_cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO device_sync_state"},
            {"match": "UPDATE ingest_events", "rowcount": 0},
            {"match": "INSERT INTO ingest_events"},
        ]
    )

    monkeypatch.setattr(
        ingest_service,
        "get_db",
        build_get_db(FakeConnection(failing_cursor), FakeConnection(failed_cursor)),
    )
    monkeypatch.setattr(ingest_service, "_clear_caches", lambda: None)

    with pytest.raises(HTTPException) as exc_info:
        ingest_service.ingest_samples(payload, authorization=None)

    assert exc_info.value.status_code == 500
    failed_sql = [entry["sql"] for entry in failed_cursor.executed]
    assert any("UPDATE ingest_events" in sql for sql in failed_sql)
    assert any(
        "INSERT INTO ingest_events" in sql and "status, error_message" in sql
        for sql in failed_sql
    )
    failing_cursor.assert_finished()
    failed_cursor.assert_finished()
