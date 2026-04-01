from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from backend.cache import dashboard_home_cache
from backend.config import LOCAL_TIMEZONE
from backend.database import get_db
from backend.queries.heart_rate import query_daily_heart_rate_rows
from backend.queries.sleep import query_sleep_daily_rows
from backend.services.ai_service import get_ai_config
from backend.services.sync_service import prioritize_devices
from backend.utils import as_int, mean, percent_change, round_or_none, rows_to_list


def _clamp_score(value: float, *, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def _compute_sleep_score(last_night_hours: float | None, avg_7d: float | None) -> float:
    baseline = avg_7d if avg_7d is not None else last_night_hours
    if baseline is None:
        return 55.0
    score = 35.0
    if baseline >= 8:
        score += 45.0
    elif baseline >= 7:
        score += 38.0
    elif baseline >= 6:
        score += 28.0
    elif baseline >= 5:
        score += 18.0
    else:
        score += 8.0
    if last_night_hours is not None and avg_7d is not None:
        score += max(-12.0, 12.0 - abs(last_night_hours - avg_7d) * 10.0)
    return _clamp_score(score)


def _compute_activity_score(steps_avg_7d: float | None, workout_count_7d: int, workout_minutes_7d: float | None) -> float:
    score = 20.0
    if steps_avg_7d is not None:
        score += min(55.0, (steps_avg_7d / 9000.0) * 55.0)
    else:
        score += 18.0
    score += min(15.0, workout_count_7d * 4.0)
    if workout_minutes_7d is not None:
        score += min(10.0, workout_minutes_7d / 18.0)
    return _clamp_score(score)


def _compute_heart_score(hr_avg_7d: float | None, today_hr_avg: float | None, today_hr_max: float | None) -> float:
    score = 72.0
    reference = hr_avg_7d if hr_avg_7d is not None else today_hr_avg
    if reference is None:
        return 60.0
    if reference < 48 or reference > 95:
        score -= 12.0
    elif reference < 55 or reference > 88:
        score -= 6.0
    else:
        score += 8.0
    if today_hr_avg is not None:
        score -= min(10.0, abs(today_hr_avg - reference) * 1.5)
    if today_hr_max is not None and today_hr_max >= 165:
        score -= 10.0
    elif today_hr_max is not None and today_hr_max >= 150:
        score -= 5.0
    return _clamp_score(score)


def _compute_sync_score(hours_since_last_sync: float | None) -> float:
    if hours_since_last_sync is None:
        return 25.0
    if hours_since_last_sync <= 2:
        return 100.0
    if hours_since_last_sync <= 8:
        return 90.0
    if hours_since_last_sync <= 24:
        return 72.0
    if hours_since_last_sync <= 48:
        return 50.0
    return 30.0


def _score_label(score: float) -> str:
    if score >= 85:
        return "状态稳"
    if score >= 70:
        return "基本在线"
    if score >= 55:
        return "需要留意"
    return "先保守一点"


def get_dashboard_home_payload(*, force_refresh: bool = False) -> dict[str, Any]:
    cache_key = "dashboard-home"
    cached = None if force_refresh else dashboard_home_cache.get(cache_key)
    if cached is not None:
        return cached

    now = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)
    today = now.date()
    fourteen_days_ago = (today - timedelta(days=13)).isoformat()
    thirty_days_ago = (today - timedelta(days=29)).isoformat()

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN type=%s AND local_date=CURDATE() THEN value_num END), 0) AS steps_today,
                COALESCE(SUM(CASE WHEN type=%s AND local_date=CURDATE() THEN value_num END), 0) AS active_calories_today,
                AVG(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_avg,
                MIN(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_min,
                MAX(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN value_num END) AS hr_max,
                COUNT(CASE WHEN type=%s AND local_date=CURDATE() AND value_num IS NOT NULL THEN 1 END) AS hr_count
            FROM health_records
            WHERE (type IN (%s, %s, %s) AND local_date=CURDATE())
            """,
            [
                "HKQuantityTypeIdentifierStepCount",
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierStepCount",
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierHeartRate",
            ],
        )
        today_row = cur.fetchone() or {}

        cur.execute(
            """
            SELECT local_date AS date, SUM(value_num) AS steps
            FROM health_records
            WHERE type=%s AND local_date >= %s
            GROUP BY local_date
            ORDER BY local_date
            """,
            ("HKQuantityTypeIdentifierStepCount", fourteen_days_ago),
        )
        step_rows = rows_to_list(cur.fetchall())

        sleep_rows = query_sleep_daily_rows(cur, start=fourteen_days_ago)
        hr_rows = query_daily_heart_rate_rows(cur, start=thirty_days_ago)

        cur.execute(
            """
            SELECT
                SUM(CASE WHEN start_at >= (CURDATE() - INTERVAL 6 DAY) THEN 1 ELSE 0 END) AS count_7d,
                ROUND(SUM(CASE WHEN start_at >= (CURDATE() - INTERVAL 6 DAY) THEN duration ELSE 0 END), 1) AS total_minutes_7d,
                ROUND(SUM(CASE WHEN start_at >= (CURDATE() - INTERVAL 29 DAY) THEN total_energy_burned ELSE 0 END), 0) AS total_calories_30d,
                SUM(CASE WHEN start_at >= (CURDATE() - INTERVAL 29 DAY) THEN 1 ELSE 0 END) AS count_30d,
                ROUND(SUM(CASE WHEN start_at >= (CURDATE() - INTERVAL 29 DAY) THEN duration ELSE 0 END), 1) AS total_minutes_30d
            FROM workouts
            WHERE start_at >= (CURDATE() - INTERVAL 29 DAY)
            """
        )
        workout_window = cur.fetchone() or {}

        cur.execute(
            """
            SELECT id, activity_type, duration, duration_unit, total_distance,
                   total_distance_unit, total_energy_burned, total_energy_burned_unit,
                   source_name, start_at, end_at, local_date AS date, route_file
            FROM workouts
            WHERE start_at >= (CURDATE() - INTERVAL 29 DAY)
            ORDER BY start_at DESC
            LIMIT 6
            """
        )
        recent_workouts = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT activity_type,
                   COUNT(*) AS count,
                   ROUND(SUM(duration), 1) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            WHERE start_at >= (CURDATE() - INTERVAL 29 DAY)
            GROUP BY activity_type
            ORDER BY count DESC, total_minutes DESC
            LIMIT 5
            """
        )
        workout_mix = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT type, COUNT(*) AS count_7d, MAX(start_at) AS last_at
            FROM health_records
            WHERE start_at >= (NOW() - INTERVAL 7 DAY)
            GROUP BY type
            ORDER BY count_7d DESC, last_at DESC
            LIMIT 8
            """
        )
        recent_types = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT MAX(received_at) AS last_sync_at,
                   COUNT(*) AS today_sync_count,
                   SUM(accepted_count) AS today_sync_accepted
            FROM ingest_events
            WHERE status='completed'
              AND received_at >= CURDATE()
              AND received_at < (CURDATE() + INTERVAL 1 DAY)
            """
        )
        today_sync = cur.fetchone() or {}

        cur.execute(
            """
            SELECT MAX(received_at) AS last_sync_at
            FROM ingest_events
            WHERE status='completed'
            """
        )
        sync_overall = cur.fetchone() or {}

        cur.execute(
            """
            SELECT device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
                   last_error_message, last_items_count, last_accepted_count, last_deduplicated_count, updated_at
            FROM device_sync_state
            ORDER BY updated_at DESC
            LIMIT 5
            """
        )
        devices = prioritize_devices(rows_to_list(cur.fetchall()))

        cur.execute(
            """
            SELECT local_date AS date,
                   value_text AS stage,
                   ROUND(SUM(TIMESTAMPDIFF(SECOND, start_at, end_at)) / 60.0, 1) AS minutes
            FROM health_records
            WHERE type=%s
              AND local_date >= %s
              AND value_text IN (%s, %s, %s, %s, %s)
            GROUP BY local_date, value_text
            ORDER BY local_date, value_text
            """,
            (
                "HKCategoryTypeIdentifierSleepAnalysis",
                fourteen_days_ago,
                "HKCategoryValueSleepAnalysisAsleepCore",
                "HKCategoryValueSleepAnalysisAsleepDeep",
                "HKCategoryValueSleepAnalysisAsleepREM",
                "HKCategoryValueSleepAnalysisAsleepUnspecified",
                "HKCategoryValueSleepAnalysisAwake",
            ),
        )
        sleep_stage_rows = rows_to_list(cur.fetchall())

    step_map = {row["date"].isoformat(): as_int(row.get("steps")) for row in step_rows}
    steps_last_14_days = []
    for offset in range(13, -1, -1):
        date_key = (today - timedelta(days=offset)).isoformat()
        steps_last_14_days.append({"date": date_key, "steps": step_map.get(date_key, 0)})
    steps_last_7_days = steps_last_14_days[-7:]
    steps_prev_7_days = steps_last_14_days[:7]
    steps_total_7d = sum(item["steps"] for item in steps_last_7_days)
    steps_avg_7d = steps_total_7d / 7 if steps_last_7_days else None
    steps_avg_prev_7d = sum(item["steps"] for item in steps_prev_7_days) / 7 if steps_prev_7_days else None

    sleep_last_7 = [round_or_none(row.get("total_hours"), 2) for row in sleep_rows[-7:]]
    sleep_last_7 = [value for value in sleep_last_7 if value is not None]
    sleep_prev_7 = [round_or_none(row.get("total_hours"), 2) for row in sleep_rows[:-7]]
    sleep_prev_7 = [value for value in sleep_prev_7 if value is not None]
    last_sleep = sleep_rows[-1] if sleep_rows else None

    hr_last_7_avg = mean([float(row["avg_bpm"]) for row in hr_rows[-7:] if row.get("avg_bpm") is not None])
    hr_prev_7_avg = mean([float(row["avg_bpm"]) for row in hr_rows[-14:-7] if row.get("avg_bpm") is not None])

    last_sync_at = sync_overall.get("last_sync_at")
    hours_since_last_sync = None
    if last_sync_at:
        hours_since_last_sync = round((now - last_sync_at).total_seconds() / 3600, 1)

    insights: list[dict[str, Any]] = []
    if hours_since_last_sync is None:
        insights.append(
            {
                "level": "warn",
                "title": "还没有同步记录",
                "detail": "首页先聚焦近期状态，但当前服务端还没有收到 bridge 的完成同步事件。",
            }
        )
    elif hours_since_last_sync >= 24:
        insights.append(
            {
                "level": "warn",
                "title": "最近 24 小时没有新同步",
                "detail": f"距离上次完成同步已过去 {hours_since_last_sync} 小时，近期数据可能不是最新。",
            }
        )

    sleep_avg_7d = mean(sleep_last_7)
    sleep_avg_prev_7d = mean(sleep_prev_7)
    if sleep_avg_7d is not None and sleep_avg_7d < 7:
        insights.append(
            {
                "level": "notice",
                "title": "近 7 晚平均睡眠偏少",
                "detail": f"最近 7 晚平均约 {round(sleep_avg_7d, 1)} 小时，可以单独追踪晚睡和补觉波动。",
                "raw_type": "HKCategoryTypeIdentifierSleepAnalysis",
            }
        )

    if steps_avg_7d is not None and steps_avg_prev_7d and steps_avg_7d < steps_avg_prev_7d * 0.8:
        insights.append(
            {
                "level": "notice",
                "title": "最近一周活动量下降",
                "detail": f"近 7 天日均步数 {round(steps_avg_7d):,}，低于前一周的 {round(steps_avg_prev_7d):,}。",
                "raw_type": "HKQuantityTypeIdentifierStepCount",
            }
        )
    elif steps_avg_7d is not None and steps_avg_7d >= 8000:
        insights.append(
            {
                "level": "good",
                "title": "最近一周步数保持不错",
                "detail": f"近 7 天日均步数约 {round(steps_avg_7d):,}，首页可以继续把步数作为主维度展示。",
                "raw_type": "HKQuantityTypeIdentifierStepCount",
            }
        )

    if as_int(workout_window.get("count_7d")) == 0:
        insights.append(
            {
                "level": "notice",
                "title": "最近 7 天没有运动记录",
                "detail": "可以把首页的训练卡片作为提醒入口，而不是放全历史运动总量。",
            }
        )
    elif recent_workouts:
        last_workout = recent_workouts[0]
        insights.append(
            {
                "level": "good",
                "title": "最近训练还在持续",
                "detail": f"最近一次训练是 {last_workout.get('date')} 的 {last_workout.get('activity_type') or '运动'}。",
            }
        )

    score_sleep = _compute_sleep_score(
        round_or_none(last_sleep.get("total_hours"), 2) if last_sleep else None,
        sleep_avg_7d,
    )
    score_activity = _compute_activity_score(
        steps_avg_7d,
        as_int(workout_window.get("count_7d")),
        float(workout_window.get("total_minutes_7d") or 0),
    )
    score_heart = _compute_heart_score(
        hr_last_7_avg,
        round_or_none(today_row.get("hr_avg"), 1),
        round_or_none(today_row.get("hr_max"), 1),
    )
    score_sync = _compute_sync_score(hours_since_last_sync)
    overall_score = round(
        score_sleep * 0.35 + score_activity * 0.3 + score_heart * 0.2 + score_sync * 0.15,
        1,
    )

    payload = {
        "generated_at": now,
        "ai": get_ai_config(),
        "score": {
            "overall": overall_score,
            "label": _score_label(overall_score),
            "components": {
                "sleep": round(score_sleep, 1),
                "activity": round(score_activity, 1),
                "heart": round(score_heart, 1),
                "sync": round(score_sync, 1),
            },
        },
        "today": {
            "steps": as_int(today_row.get("steps_today")),
            "active_calories": as_int(today_row.get("active_calories_today")),
            "heart_rate": {
                "avg": round_or_none(today_row.get("hr_avg"), 1),
                "min": round_or_none(today_row.get("hr_min"), 1),
                "max": round_or_none(today_row.get("hr_max"), 1),
                "count": as_int(today_row.get("hr_count")),
            },
        },
        "steps": {
            "today": as_int(today_row.get("steps_today")),
            "last_7_days": steps_last_7_days,
            "total_7d": steps_total_7d,
            "avg_7d": round_or_none(steps_avg_7d, 1),
            "avg_prev_7d": round_or_none(steps_avg_prev_7d, 1),
            "delta_vs_prev_7d": percent_change(steps_avg_7d, steps_avg_prev_7d),
        },
        "sleep": {
            "last_14_days": sleep_rows,
            "last_14_days_stages": sleep_stage_rows,
            "last_night_hours": round_or_none(last_sleep.get("total_hours"), 2) if last_sleep else None,
            "last_sleep_start": last_sleep.get("sleep_start") if last_sleep else None,
            "last_sleep_end": last_sleep.get("sleep_end") if last_sleep else None,
            "avg_7d": round_or_none(sleep_avg_7d, 2),
            "avg_prev_7d": round_or_none(sleep_avg_prev_7d, 2),
            "delta_vs_prev_7d": percent_change(sleep_avg_7d, sleep_avg_prev_7d),
        },
        "heart_rate": {
            "last_30_days": hr_rows,
            "avg_7d": round_or_none(hr_last_7_avg, 1),
            "avg_prev_7d": round_or_none(hr_prev_7_avg, 1),
            "delta_vs_prev_7d": percent_change(hr_last_7_avg, hr_prev_7_avg),
        },
        "workouts": {
            "count_7d": as_int(workout_window.get("count_7d")),
            "count_30d": as_int(workout_window.get("count_30d")),
            "total_minutes_7d": round_or_none(workout_window.get("total_minutes_7d"), 1),
            "total_minutes_30d": round_or_none(workout_window.get("total_minutes_30d"), 1),
            "total_calories_30d": as_int(workout_window.get("total_calories_30d")),
            "last_workout": recent_workouts[0] if recent_workouts else None,
            "recent": recent_workouts,
            "summary_30d": workout_mix,
        },
        "recent_types": recent_types,
        "sync": {
            "last_sync_at": last_sync_at,
            "hours_since_last_sync": hours_since_last_sync,
            "today_sync_count": as_int(today_sync.get("today_sync_count")),
            "today_sync_accepted": as_int(today_sync.get("today_sync_accepted")),
            "devices": devices,
        },
        "insights": insights[:4],
    }
    return dashboard_home_cache.set(cache_key, payload)
