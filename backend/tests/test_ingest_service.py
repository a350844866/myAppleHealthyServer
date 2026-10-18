from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException

from backend import utils as backend_utils
from backend.models import IngestItem, IngestPayload
from backend.services import ingest_service
from backend.tests.conftest import FakeConnection, ScriptedCursor, build_get_db

TEST_INGEST_TOKEN = "test-ingest-token"
TEST_AUTHORIZATION = f"Bearer {TEST_INGEST_TOKEN}"

# Positional index of creation_at in the INSERT INTO workouts params tuple.
WORKOUT_CREATION_AT_PARAM = 11


@pytest.fixture(autouse=True)
def ingest_token(monkeypatch):
    monkeypatch.setenv("INGEST_API_TOKEN", TEST_INGEST_TOKEN)


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

    result = ingest_service.ingest_samples(payload, authorization=TEST_AUTHORIZATION)

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
        ingest_service.ingest_samples(payload, authorization=TEST_AUTHORIZATION)

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
        ingest_service.ingest_samples(payload, authorization=TEST_AUTHORIZATION)

    assert exc_info.value.status_code == 500
    failed_sql = [entry["sql"] for entry in failed_cursor.executed]
    assert any("UPDATE ingest_events" in sql for sql in failed_sql)
    assert any(
        "INSERT INTO ingest_events" in sql and "status, error_message" in sql
        for sql in failed_sql
    )
    failing_cursor.assert_finished()
    failed_cursor.assert_finished()


def make_workout_payload(creation_date: str | None = None) -> IngestPayload:
    metadata = {
        "source_name": "jiaxu-watch",
        "duration": "1800",
        "duration_unit": "s",
    }
    if creation_date is not None:
        metadata["creation_date"] = creation_date
    return IngestPayload(
        device_id="jiaxu-iphone",
        bundle_id="com.example.healthbridge",
        sent_at=datetime(2026, 4, 1, 12, 0, 0),
        items=[
            IngestItem(
                source="healthkit",
                kind="workout",
                type="HKWorkoutActivityTypeRunning",
                uuid="w-1",
                start_at=datetime(2026, 4, 1, 7, 0, 0),
                end_at=datetime(2026, 4, 1, 7, 30, 0),
                value=1800,
                unit="s",
                metadata=metadata,
            )
        ],
    )


def test_insert_workouts_uses_creation_date_from_metadata():
    payload = make_workout_payload(creation_date="2026-04-01T07:31:05.250")
    cursor = ScriptedCursor([{"match": "INSERT INTO workouts", "rowcount": 1}])

    inserted = ingest_service.insert_workouts_from_ingest(cursor, payload, payload.items)

    assert inserted == 1
    params = cursor.executed[0]["params"]
    assert params[WORKOUT_CREATION_AT_PARAM] == datetime(2026, 4, 1, 7, 31, 5)
    cursor.assert_finished()


def test_insert_workouts_falls_back_to_start_at_without_creation_date():
    payload = make_workout_payload()
    cursor = ScriptedCursor([{"match": "INSERT INTO workouts", "rowcount": 1}])

    ingest_service.insert_workouts_from_ingest(cursor, payload, payload.items)

    params = cursor.executed[0]["params"]
    assert params[WORKOUT_CREATION_AT_PARAM] == datetime(2026, 4, 1, 7, 0, 0)
    cursor.assert_finished()


def test_insert_workouts_converts_aware_creation_date_to_local(monkeypatch):
    monkeypatch.setattr(backend_utils, "LOCAL_TIMEZONE", ZoneInfo("Asia/Shanghai"))
    payload = make_workout_payload(creation_date="2026-03-31T23:31:05Z")
    cursor = ScriptedCursor([{"match": "INSERT INTO workouts", "rowcount": 1}])

    ingest_service.insert_workouts_from_ingest(cursor, payload, payload.items)

    params = cursor.executed[0]["params"]
    assert params[WORKOUT_CREATION_AT_PARAM] == datetime(2026, 4, 1, 7, 31, 5)
    cursor.assert_finished()


@pytest.mark.parametrize(
    "creation_date",
    [
        "not-a-date",
        # Parses fine but overflows when converted to local time.
        "9999-12-31T23:59:59+00:00",
        "0001-01-01T00:00:00+09:00",
    ],
)
def test_ingest_samples_malformed_creation_date_falls_back_to_start_at(monkeypatch, creation_date):
    monkeypatch.setattr(backend_utils, "LOCAL_TIMEZONE", ZoneInfo("Asia/Shanghai"))
    payload = make_workout_payload(creation_date=creation_date)
    cursor = ScriptedCursor(
        [
            {"match": "INSERT INTO ingest_events", "lastrowid": 12},
            {"match": "INSERT INTO workouts", "rowcount": 1},
            {"match": "INSERT INTO device_sync_state"},
            {"match": "UPDATE ingest_events"},
        ]
    )

    monkeypatch.setattr(
        ingest_service,
        "get_db",
        build_get_db(FakeConnection(cursor)),
    )
    monkeypatch.setattr(ingest_service, "_clear_caches", lambda: None)

    result = ingest_service.ingest_samples(payload, authorization=TEST_AUTHORIZATION)

    assert result == {"ok": True, "accepted": 1, "deduplicated": 0}
    workout_params = next(
        entry["params"] for entry in cursor.executed if "INSERT INTO workouts" in entry["sql"]
    )
    assert workout_params[WORKOUT_CREATION_AT_PARAM] == datetime(2026, 4, 1, 7, 0, 0)
    cursor.assert_finished()


def test_workout_hash_ignores_creation_date():
    without_creation = make_workout_payload()
    with_creation = make_workout_payload(creation_date="2026-04-01T07:31:05")

    assert ingest_service.make_workout_hash(
        without_creation, without_creation.items[0]
    ) == ingest_service.make_workout_hash(with_creation, with_creation.items[0])
