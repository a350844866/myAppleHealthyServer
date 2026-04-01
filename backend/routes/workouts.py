from __future__ import annotations

import json
import math
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.database import get_db
from backend.responses import api_response, list_response
from backend.utils import build_date_filters, rows_to_list

router = APIRouter()


@router.get("/api/workouts")
def get_workouts(
    activity_type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
):
    conditions = []
    params: list = []
    if activity_type:
        conditions.append("activity_type = %s")
        params.append(activity_type)
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, activity_type, duration, duration_unit, total_distance,
                   total_distance_unit, total_energy_burned, total_energy_burned_unit,
                   source_name, start_at, end_at, local_date AS date, route_file
            FROM workouts
            {where}
            ORDER BY start_at DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total FROM workouts {where}", params)
        total = cur.fetchone()["total"]
    return list_response(rows_to_list(rows), total=total, limit=limit, offset=offset)


@router.get("/api/workouts/summary")
def get_workouts_summary():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT activity_type,
                   COUNT(*) AS count,
                   ROUND(SUM(duration), 1) AS total_minutes,
                   ROUND(AVG(duration), 1) AS avg_minutes,
                   ROUND(SUM(total_distance), 2) AS total_distance,
                   MIN(total_distance_unit) AS distance_unit,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories,
                   MIN(local_date) AS first_date,
                   MAX(local_date) AS last_date
            FROM workouts
            GROUP BY activity_type
            ORDER BY count DESC
            """
        )
        return list_response(rows_to_list(cur.fetchall()))


@router.get("/api/workouts/weekly-summary")
def get_workouts_weekly_summary(weeks: int = Query(12, ge=4, le=52)):
    start_date = (datetime.now().date() - timedelta(days=weeks * 7 - 1)).isoformat()
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT local_date AS date,
                   activity_type,
                   duration,
                   total_energy_burned
            FROM workouts
            WHERE local_date >= %s
            ORDER BY local_date, start_at
            """,
            (start_date,),
        )
        rows = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT activity_type,
                   COUNT(*) AS count,
                   ROUND(SUM(duration), 1) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            WHERE local_date >= %s
            GROUP BY activity_type
            ORDER BY count DESC, total_minutes DESC
            LIMIT 8
            """,
            (start_date,),
        )
        top_types = rows_to_list(cur.fetchall())

    by_date: dict[str, dict] = {}
    for row in rows:
        date = str(row["date"])
        bucket = by_date.setdefault(date, {
            "date": date,
            "count": 0,
            "minutes": 0.0,
            "calories": 0.0,
            "types": {},
        })
        bucket["count"] += 1
        bucket["minutes"] += float(row.get("duration") or 0)
        bucket["calories"] += float(row.get("total_energy_burned") or 0)
        activity = row.get("activity_type") or "Other"
        bucket["types"][activity] = bucket["types"].get(activity, 0) + 1

    daily = []
    day = datetime.fromisoformat(start_date).date()
    end_day = datetime.now().date()
    while day <= end_day:
        key = day.isoformat()
        value = by_date.get(key, {
            "date": key,
            "count": 0,
            "minutes": 0.0,
            "calories": 0.0,
            "types": {},
        })
        value["minutes"] = round(float(value["minutes"]), 1)
        value["calories"] = round(float(value["calories"]), 0)
        value["intensity"] = (
            "high" if value["minutes"] >= 60 or value["calories"] >= 500 else
            "medium" if value["minutes"] >= 25 or value["calories"] >= 180 else
            "low" if value["count"] > 0 else
            "none"
        )
        daily.append(value)
        day += timedelta(days=1)

    weekly = []
    for index in range(0, len(daily), 7):
        chunk = daily[index:index + 7]
        if not chunk:
            continue
        weekly.append({
            "week_start": chunk[0]["date"],
            "week_end": chunk[-1]["date"],
            "count": sum(item["count"] for item in chunk),
            "minutes": round(sum(item["minutes"] for item in chunk), 1),
            "calories": round(sum(item["calories"] for item in chunk), 0),
        })

    summary = {
        "weeks": weeks,
        "total_workouts": sum(item["count"] for item in daily),
        "total_minutes": round(sum(item["minutes"] for item in daily), 1),
        "total_calories": round(sum(item["calories"] for item in daily), 0),
        "active_days": sum(1 for item in daily if item["count"] > 0),
    }
    return api_response({
        "summary": summary,
        "weekly": weekly,
        "daily": daily,
        "top_types": top_types,
    })


@router.get("/api/workouts/routes")
def get_workout_routes(limit: int = Query(12, ge=1, le=50)):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT
                w.id,
                w.activity_type,
                w.duration,
                w.duration_unit,
                w.total_distance,
                w.total_distance_unit,
                w.total_energy_burned,
                w.total_energy_burned_unit,
                w.source_name,
                w.start_at,
                w.end_at,
                w.local_date AS date,
                w.route_file,
                wr.id AS route_id,
                COUNT(rp.id) AS point_count,
                MIN(rp.latitude) AS min_lat,
                MAX(rp.latitude) AS max_lat,
                MIN(rp.longitude) AS min_lng,
                MAX(rp.longitude) AS max_lng
            FROM workouts w
            JOIN workout_routes wr ON wr.file_path = w.route_file
            LEFT JOIN route_points rp ON rp.route_id = wr.id
            WHERE w.route_file IS NOT NULL
              AND w.route_file <> ''
            GROUP BY
                w.id, w.activity_type, w.duration, w.duration_unit,
                w.total_distance, w.total_distance_unit,
                w.total_energy_burned, w.total_energy_burned_unit,
                w.source_name, w.start_at, w.end_at, w.local_date,
                w.route_file, wr.id
            ORDER BY w.start_at DESC, w.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = rows_to_list(cur.fetchall())

    for row in rows:
        row["point_count"] = int(row.get("point_count") or 0)
        row["bounds"] = (
            [[float(row["min_lat"]), float(row["min_lng"])], [float(row["max_lat"]), float(row["max_lng"])]]
            if row.get("min_lat") is not None and row.get("min_lng") is not None and row.get("max_lat") is not None and row.get("max_lng") is not None
            else None
        )
    return list_response(rows, limit=limit)


@router.get("/api/workouts/{workout_id}/route")
def get_workout_route(workout_id: int, max_points: int = Query(2500, ge=200, le=10000)):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT
                w.id,
                w.activity_type,
                w.duration,
                w.duration_unit,
                w.total_distance,
                w.total_distance_unit,
                w.total_energy_burned,
                w.total_energy_burned_unit,
                w.source_name,
                w.start_at,
                w.end_at,
                w.local_date AS date,
                w.route_file,
                wr.id AS route_id,
                wr.creation_at,
                wr.device,
                COUNT(rp.id) AS point_count,
                MIN(rp.latitude) AS min_lat,
                MAX(rp.latitude) AS max_lat,
                MIN(rp.longitude) AS min_lng,
                MAX(rp.longitude) AS max_lng
            FROM workouts w
            JOIN workout_routes wr ON wr.file_path = w.route_file
            LEFT JOIN route_points rp ON rp.route_id = wr.id
            WHERE w.id = %s
            GROUP BY
                w.id, w.activity_type, w.duration, w.duration_unit,
                w.total_distance, w.total_distance_unit,
                w.total_energy_burned, w.total_energy_burned_unit,
                w.source_name, w.start_at, w.end_at, w.local_date,
                w.route_file, wr.id, wr.creation_at, wr.device
            """,
            (workout_id,),
        )
        workout = cur.fetchone()
        if not workout:
            raise HTTPException(404, "未找到该运动路线")

        cur.execute(
            """
            SELECT latitude, longitude, elevation, recorded_at AS timestamp, speed, course, point_index
            FROM route_points
            WHERE route_id = %s
            ORDER BY point_index
            """,
            (workout["route_id"],),
        )
        points = rows_to_list(cur.fetchall())

    if not points:
        raise HTTPException(404, "该路线没有坐标点")

    sample_step = max(1, math.ceil(len(points) / max_points))
    sampled_points = points[::sample_step]
    if sampled_points[-1] != points[-1]:
        sampled_points.append(points[-1])

    payload = dict(workout)
    payload["point_count"] = int(payload.get("point_count") or 0)
    payload["bounds"] = (
        [[float(payload["min_lat"]), float(payload["min_lng"])], [float(payload["max_lat"]), float(payload["max_lng"])]]
        if payload.get("min_lat") is not None and payload.get("min_lng") is not None and payload.get("max_lat") is not None and payload.get("max_lng") is not None
        else None
    )
    payload["sample_step"] = sample_step
    payload["sampled_points"] = sampled_points
    return api_response(payload, total_points=len(points), returned_points=len(sampled_points), max_points=max_points)


@router.get("/api/workouts/{workout_id}")
def get_workout_detail(workout_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM workouts WHERE id=%s", (workout_id,))
        workout = cur.fetchone()
        if not workout:
            raise HTTPException(404, "运动记录不存在")

        cur.execute(
            """
            SELECT id, type, start_at, end_at, average_value, minimum_value, maximum_value, sum_value, unit
            FROM workout_statistics
            WHERE workout_id=%s
            ORDER BY type, start_at
            """,
            (workout_id,),
        )
        workout["statistics"] = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT id, type, event_at, duration, duration_unit
            FROM workout_events
            WHERE workout_id=%s
            ORDER BY event_at
            """,
            (workout_id,),
        )
        workout["events"] = rows_to_list(cur.fetchall())

        if workout.get("route_file"):
            cur.execute(
                """
                SELECT rp.latitude, rp.longitude, rp.elevation, rp.recorded_at AS timestamp,
                       rp.speed, rp.course, rp.h_acc, rp.v_acc
                FROM workout_routes wr
                JOIN route_points rp ON rp.route_id = wr.id
                WHERE wr.file_path=%s
                ORDER BY rp.point_index
                """,
                (workout["route_file"],),
            )
            workout["route_points"] = rows_to_list(cur.fetchall())
        else:
            workout["route_points"] = []
        return api_response(workout)


@router.get("/api/activity-summaries")
def get_activity_summaries(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    conditions, params = build_date_filters("summary_date", start, end)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT summary_date AS date_components, active_energy_burned, active_energy_burned_goal,
                   active_energy_burned_unit, apple_move_time, apple_move_time_goal,
                   apple_exercise_time, apple_exercise_time_goal,
                   apple_stand_hours, apple_stand_hours_goal
            FROM activity_summaries
            {where}
            ORDER BY summary_date
            """,
            params,
        )
        return list_response(rows_to_list(cur.fetchall()))


@router.get("/api/ecg")
def list_ecg():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT id, file_name, record_at, classification, symptoms,
                   software_version, device, sample_rate, lead_name, unit
            FROM ecg_readings
            ORDER BY record_at
            """
        )
        return list_response(rows_to_list(cur.fetchall()))


@router.get("/api/ecg/{ecg_id}")
def get_ecg_detail(ecg_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM ecg_readings WHERE id=%s", (ecg_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "ECG 记录不存在")
        if row.get("voltage_data"):
            row["voltage_data"] = json.loads(row["voltage_data"])
        return api_response(row)
