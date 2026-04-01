from __future__ import annotations

from datetime import date, datetime

from backend.routes import workouts
from backend.tests.conftest import FakeConnection, ScriptedCursor, build_get_db, make_test_client


def test_workout_routes_heatmap_returns_sampled_points(monkeypatch):
    cursor = ScriptedCursor(
        [
            {
                "match": "SELECT wr.id AS route_id",
                "fetchall": [
                    {
                        "route_id": 7,
                        "workout_id": 101,
                        "activity_type": "Running",
                        "date": date(2026, 3, 31),
                        "start_at": datetime(2026, 3, 31, 6, 30, 0),
                    },
                    {
                        "route_id": 8,
                        "workout_id": 102,
                        "activity_type": "Walking",
                        "date": date(2026, 3, 30),
                        "start_at": datetime(2026, 3, 30, 7, 15, 0),
                    },
                ],
            },
            {
                "match": "SELECT COUNT(*) AS total_points",
                "fetchone": {
                    "total_points": 2500,
                    "min_lat": 31.21,
                    "max_lat": 31.25,
                    "min_lng": 121.44,
                    "max_lng": 121.49,
                },
            },
            {
                "match": "SELECT route_id, latitude, longitude",
                "fetchall": [
                    {"route_id": 7, "latitude": 31.21, "longitude": 121.44},
                    {"route_id": 7, "latitude": 31.22, "longitude": 121.45},
                    {"route_id": 8, "latitude": 31.24, "longitude": 121.48},
                ],
            },
        ]
    )
    monkeypatch.setattr(workouts, "get_db", build_get_db(FakeConnection(cursor)))
    client = make_test_client(workouts.router)

    response = client.get("/api/workouts/routes/heatmap?route_limit=4&max_points=1000")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["summary"]["routes"] == 2
    assert payload["data"]["summary"]["sample_step"] == 3
    assert payload["data"]["summary"]["returned_points"] == 3
    assert payload["data"]["bounds"] == [[31.21, 121.44], [31.25, 121.49]]
    assert payload["data"]["points"][0]["workout_id"] == 101
    assert payload["meta"]["route_limit"] == 4
    cursor.assert_finished()


def test_workout_route_sampling_keeps_last_point(monkeypatch):
    points = [
        {
            "latitude": 31.20 + index * 0.001,
            "longitude": 121.40 + index * 0.001,
            "elevation": 5 + index,
            "timestamp": datetime(2026, 3, 31, 6, index % 60, 0),
            "speed": None,
            "course": None,
            "point_index": index,
        }
        for index in range(401)
    ]
    cursor = ScriptedCursor(
        [
            {
                "match": "SELECT w.id, w.activity_type",
                "fetchone": {
                    "id": 101,
                    "activity_type": "Running",
                    "duration": 48.0,
                    "duration_unit": "min",
                    "total_distance": 10.2,
                    "total_distance_unit": "km",
                    "total_energy_burned": 620,
                    "total_energy_burned_unit": "kcal",
                    "source_name": "Apple Watch",
                    "start_at": datetime(2026, 3, 31, 6, 0, 0),
                    "end_at": datetime(2026, 3, 31, 6, 48, 0),
                    "date": date(2026, 3, 31),
                    "route_file": "route.gpx",
                    "route_id": 7,
                    "creation_at": datetime(2026, 3, 31, 7, 0, 0),
                    "device": "watch",
                    "point_count": len(points),
                    "min_lat": 31.20,
                    "max_lat": 31.60,
                    "min_lng": 121.40,
                    "max_lng": 121.80,
                },
            },
            {
                "match": "SELECT latitude, longitude, elevation, recorded_at AS timestamp",
                "fetchall": points,
            },
        ]
    )
    monkeypatch.setattr(workouts, "get_db", build_get_db(FakeConnection(cursor)))

    payload = workouts.get_workout_route(101, max_points=200)

    assert payload["data"]["sample_step"] == 3
    assert payload["meta"]["total_points"] == 401
    assert payload["meta"]["returned_points"] == 135
    assert payload["data"]["sampled_points"][-1]["point_index"] == 400
    cursor.assert_finished()
