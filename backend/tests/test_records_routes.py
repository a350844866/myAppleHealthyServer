from __future__ import annotations

from datetime import date

from backend.routes import records
from backend.tests.conftest import FakeConnection, ScriptedCursor, build_get_db, make_test_client


def test_records_daily_prefers_single_source_for_distance(monkeypatch):
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
    monkeypatch.setattr(records, "get_db", build_get_db(FakeConnection(cursor)))
    client = make_test_client(records.router)

    response = client.get(
        "/api/records/daily",
        params={
            "type": "HKQuantityTypeIdentifierDistanceWalkingRunning",
            "agg": "sum",
            "start": "2026-04-02",
            "end": "2026-04-02",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == [
        {"date": "2026-04-02", "value": 2694.8745909542777, "count": 27, "unit": "m"}
    ]
    cursor.assert_finished()


def test_energy_route_prefers_single_source_per_metric(monkeypatch):
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
        },
        {
            "match": "GROUP BY local_date, COALESCE(NULLIF(source_name, ''), 'Unknown')",
            "fetchall": [
                {
                    "date": date(2026, 4, 2),
                    "source_name": "贾诩的Apple Watch",
                    "value": 1229.661,
                    "count": 73,
                    "unit": "kcal",
                    "device_name": "Apple Watch",
                    "product_type": "Watch7,9",
                }
            ],
        },
    ])
    monkeypatch.setattr(records, "get_db", build_get_db(FakeConnection(cursor)))
    client = make_test_client(records.router)

    response = client.get("/api/energy", params={"start": "2026-04-02", "end": "2026-04-02"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == [
        {"date": "2026-04-02", "active_cal": 246.192, "basal_cal": 1229.661}
    ]
    cursor.assert_finished()


def test_records_hourly_uses_sample_anchor(monkeypatch):
    cursor = ScriptedCursor([
        {
            "match": "TIMESTAMPADD(SECOND, TIMESTAMPDIFF(SECOND, start_at, end_at) DIV 2, start_at)",
            "fetchall": [
                {"hour": 10, "value": 80.7, "count": 18, "unit": "count/min"},
                {"hour": 11, "value": 83.2, "count": 9, "unit": "count/min"},
            ],
        }
    ])
    monkeypatch.setattr(records, "get_db", build_get_db(FakeConnection(cursor)))
    client = make_test_client(records.router)

    response = client.get(
        "/api/records/hourly",
        params={
            "type": "HKQuantityTypeIdentifierHeartRate",
            "agg": "avg",
            "date": "2026-04-04",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == [
        {"hour": 10, "value": 80.7, "count": 18, "unit": "count/min"},
        {"hour": 11, "value": 83.2, "count": 9, "unit": "count/min"},
    ]
    cursor.assert_finished()


def test_heart_rate_hourly_uses_sample_anchor(monkeypatch):
    cursor = ScriptedCursor([
        {
            "match": "TIMESTAMPADD(SECOND, TIMESTAMPDIFF(SECOND, start_at, end_at) DIV 2, start_at)",
            "fetchall": [
                {
                    "hour": "2026-04-04 10:00:00",
                    "avg_bpm": 80.7,
                    "min_bpm": 73.0,
                    "max_bpm": 95.0,
                    "count": 18,
                }
            ],
        }
    ])
    monkeypatch.setattr(records, "get_db", build_get_db(FakeConnection(cursor)))
    client = make_test_client(records.router)

    response = client.get(
        "/api/heart-rate",
        params={
            "granularity": "hourly",
            "start": "2026-04-04",
            "end": "2026-04-04",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == [
        {
            "hour": "2026-04-04 10:00:00",
            "avg_bpm": 80.7,
            "min_bpm": 73.0,
            "max_bpm": 95.0,
            "count": 18,
        }
    ]
    cursor.assert_finished()
