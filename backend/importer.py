"""
Apple Health Data Importer for MySQL 8

用法:
    export HEALTH_DB_PASSWORD='your-password'
    python importer.py
    python importer.py --force
    python importer.py --xml-only
    python importer.py --gpx-only
    python importer.py --ecg-only
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

BASE_DIR = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "apple_health_export"
XML_PATH = EXPORT_DIR / "导出.xml"
ECG_DIR = EXPORT_DIR / "electrocardiograms"
ROUTES_DIR = EXPORT_DIR / "workout-routes"
SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"
BATCH_SIZE = 5000
PROGRESS_UPDATE_INTERVAL = 5000


def db_config(include_database: bool = True) -> dict:
    config = {
        "host": os.getenv("HEALTH_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("HEALTH_DB_PORT", "3306")),
        "user": os.getenv("HEALTH_DB_USER", "root"),
        "password": os.getenv("HEALTH_DB_PASSWORD", ""),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": False,
    }
    if include_database:
        config["database"] = os.getenv("HEALTH_DB_NAME", "apple_health")
    return config


def ensure_password() -> None:
    if db_config(False)["password"]:
        return
    raise SystemExit("缺少 HEALTH_DB_PASSWORD 环境变量，无法连接 MySQL。")


def get_conn(include_database: bool = True):
    return pymysql.connect(**db_config(include_database))


def ensure_database() -> None:
    db_name = os.getenv("HEALTH_DB_NAME", "apple_health")
    conn = get_conn(include_database=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
        conn.commit()
    finally:
        conn.close()


def init_schema(conn) -> None:
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()
    print("✓ MySQL schema 初始化完成")


def ensure_runtime_schema(conn) -> None:
    with conn.cursor() as cur:
        runtime_columns = {
            "last_progress_at": "ADD COLUMN last_progress_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
            "run_started_records_seen": "ADD COLUMN run_started_records_seen BIGINT NULL",
            "run_started_records_inserted": "ADD COLUMN run_started_records_inserted BIGINT NULL",
            "run_started_at": "ADD COLUMN run_started_at TIMESTAMP NULL DEFAULT NULL",
        }
        for column_name, ddl in runtime_columns.items():
            cur.execute(
                """
                SELECT COUNT(*) AS count
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'import_files'
                  AND column_name = %s
                """,
                (column_name,),
            )
            if cur.fetchone()["count"]:
                continue
            cur.execute(f"ALTER TABLE import_files {ddl}")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS import_progress_samples (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                import_file_id BIGINT NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                records_seen BIGINT NOT NULL DEFAULT 0,
                records_inserted BIGINT NOT NULL DEFAULT 0,
                KEY idx_progress_samples_file_time (import_file_id, recorded_at),
                CONSTRAINT fk_progress_samples_file FOREIGN KEY (import_file_id) REFERENCES import_files(id) ON DELETE CASCADE
            )
            """
        )
    conn.commit()


def parse_health_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S.%f %z"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=None)
        except ValueError:
            continue
    return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=None)
    except ValueError:
        return None


def parse_date(value: str | None):
    dt = parse_health_datetime(value)
    if dt is not None:
        return dt.date()
    if value:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def try_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def file_mtime(path: Path) -> int:
    return int(path.stat().st_mtime)


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def hash_payload(*parts) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_import_file_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(EXPORT_DIR.resolve())
        return str(Path("apple_health_export") / relative)
    except ValueError:
        pass
    try:
        return str(resolved.relative_to(BASE_DIR.resolve()))
    except ValueError:
        return str(resolved)


def import_file_lookup_paths(path: Path) -> list[str]:
    resolved = str(path.resolve())
    canonical = canonical_import_file_path(path)
    values = [canonical]
    if resolved != canonical:
        values.append(resolved)
    original = str(path)
    if original not in values:
        values.append(original)
    return values


def import_file_legacy_suffix(path: Path) -> str:
    return f"%/{canonical_import_file_path(path)}"


def file_already_imported(conn, path: Path, import_type: str) -> bool:
    lookup_paths = import_file_lookup_paths(path)
    legacy_suffix = import_file_legacy_suffix(path)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id
            FROM import_files
            WHERE (
                file_path IN ({", ".join(["%s"] * len(lookup_paths))})
                OR file_path LIKE %s
            )
              AND file_mtime=%s
              AND import_type=%s
              AND status='completed'
            LIMIT 1
            """,
            (*lookup_paths, legacy_suffix, file_mtime(path), import_type),
        )
        return cur.fetchone() is not None


def create_import_batch(conn, batch_type: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO import_batches (batch_type, status) VALUES (%s, 'running')",
            (batch_type,),
        )
        batch_id = cur.lastrowid
    conn.commit()
    return batch_id


def mark_stale_running_jobs(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_batches
            SET status='failed',
                completed_at=NOW(),
                note=CASE
                    WHEN note IS NULL OR note='' THEN 'stale run recovered before new importer start'
                    ELSE CONCAT(note, ' | stale run recovered before new importer start')
                END
            WHERE status='running'
            """
        )
        stale_batches = cur.rowcount
        cur.execute(
            """
            UPDATE import_files
            SET status='failed',
                completed_at=NOW()
            WHERE status='running'
            """
        )
        stale_files = cur.rowcount
    conn.commit()
    return stale_batches, stale_files


def finish_import_batch(conn, batch_id: int, status: str = "completed") -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE import_batches SET status=%s, completed_at=NOW() WHERE id=%s",
            (status, batch_id),
        )
    conn.commit()


def create_import_file(conn, batch_id: int, path: Path, import_type: str, *, resume: bool = False) -> dict:
    canonical_path = canonical_import_file_path(path)
    lookup_paths = import_file_lookup_paths(path)
    legacy_suffix = import_file_legacy_suffix(path)
    existing_rows = []
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, file_path, status, records_seen, records_inserted,
                   run_started_records_seen, run_started_records_inserted
            FROM import_files
            WHERE (
                file_path IN ({", ".join(["%s"] * len(lookup_paths))})
                OR file_path LIKE %s
            )
              AND file_mtime=%s
              AND import_type=%s
            ORDER BY records_seen DESC, records_inserted DESC, id DESC
            """,
            (*lookup_paths, legacy_suffix, file_mtime(path), import_type),
        )
        existing_rows = cur.fetchall()

    with conn.cursor() as cur:
        if not existing_rows:
            cur.execute(
                """
                INSERT INTO import_files
                    (
                        batch_id, file_path, file_size, file_mtime, import_type, status,
                        last_progress_at, run_started_records_seen, run_started_records_inserted, run_started_at
                    )
                VALUES (%s, %s, %s, %s, %s, 'running', NOW(), %s, %s, NOW())
                """,
                (batch_id, canonical_path, path.stat().st_size, file_mtime(path), import_type, 0, 0),
            )
            import_file_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO import_progress_samples (import_file_id, records_seen, records_inserted)
                VALUES (%s, %s, %s)
                """,
                (import_file_id, 0, 0),
            )
            conn.commit()
            return {"id": import_file_id, "resume_records_seen": 0, "resume_records_inserted": 0}

        existing = next(
            (row for row in existing_rows if row["file_path"] == canonical_path),
            existing_rows[0],
        )

        resume_records_seen = 0
        resume_records_inserted = 0
        if resume:
            resumable_rows = [row for row in existing_rows if row["status"] == "failed"]
            if resumable_rows:
                resume_records_seen = max(
                    max(int(row["records_seen"] or 0), int(row["run_started_records_seen"] or 0))
                    for row in resumable_rows
                )
                resume_records_inserted = max(
                    max(int(row["records_inserted"] or 0), int(row["run_started_records_inserted"] or 0))
                    for row in resumable_rows
                )

        cur.execute(
            """
            UPDATE import_files
            SET batch_id=%s,
                file_path=%s,
                file_size=%s,
                status='running',
                records_seen=%s,
                records_inserted=%s,
                run_started_records_seen=%s,
                run_started_records_inserted=%s,
                run_started_at=NOW(),
                completed_at=NULL,
                last_progress_at=NOW()
            WHERE id=%s
            """,
            (
                batch_id,
                canonical_path,
                path.stat().st_size,
                resume_records_seen,
                resume_records_inserted,
                resume_records_seen,
                resume_records_inserted,
                existing["id"],
            ),
        )
        cur.execute(
            """
            INSERT INTO import_progress_samples (import_file_id, records_seen, records_inserted)
            VALUES (%s, %s, %s)
            """,
            (existing["id"], resume_records_seen, resume_records_inserted),
        )
    conn.commit()
    return {
        "id": existing["id"],
        "resume_records_seen": resume_records_seen,
        "resume_records_inserted": resume_records_inserted,
    }


def finish_import_file(conn, import_file_id: int, records_seen: int, records_inserted: int, status: str = "completed") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_files
            SET records_seen=%s,
                records_inserted=%s,
                status=%s,
                completed_at=NOW(),
                last_progress_at=NOW()
            WHERE id=%s
            """,
            (records_seen, records_inserted, status, import_file_id),
        )
        cur.execute(
            """
            INSERT INTO import_progress_samples (import_file_id, records_seen, records_inserted)
            VALUES (%s, %s, %s)
            """,
            (import_file_id, records_seen, records_inserted),
        )
    conn.commit()


def fail_import_file(conn, import_file_id: int, records_seen: int, records_inserted: int) -> None:
    finish_import_file(conn, import_file_id, records_seen, records_inserted, status="failed")


def update_import_progress(conn, import_file_id: int, records_seen: int, records_inserted: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_files
            SET records_seen=%s,
                records_inserted=%s,
                last_progress_at=NOW()
            WHERE id=%s
            """,
            (records_seen, records_inserted, import_file_id),
        )
        cur.execute(
            """
            INSERT INTO import_progress_samples (import_file_id, records_seen, records_inserted)
            VALUES (%s, %s, %s)
            """,
            (import_file_id, records_seen, records_inserted),
        )
    conn.commit()


def upsert_profile(conn, attributes: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profile
                (id, date_of_birth, biological_sex, blood_type, skin_type, cardio_meds_use)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                date_of_birth=VALUES(date_of_birth),
                biological_sex=VALUES(biological_sex),
                blood_type=VALUES(blood_type),
                skin_type=VALUES(skin_type),
                cardio_meds_use=VALUES(cardio_meds_use)
            """,
            (
                1,
                parse_date(attributes.get("HKCharacteristicTypeIdentifierDateOfBirth")),
                attributes.get("HKCharacteristicTypeIdentifierBiologicalSex"),
                attributes.get("HKCharacteristicTypeIdentifierBloodType"),
                attributes.get("HKCharacteristicTypeIdentifierFitzpatrickSkinType"),
                attributes.get("HKCharacteristicTypeIdentifierCardioFitnessMedicationsUse"),
            ),
        )
    conn.commit()


def update_export_date(conn, value: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO profile (id, export_date_raw, exported_at)
            VALUES (1, %s, %s)
            ON DUPLICATE KEY UPDATE
                export_date_raw=VALUES(export_date_raw),
                exported_at=VALUES(exported_at)
            """,
            (value, parse_health_datetime(value)),
        )
    conn.commit()


def flush_records(conn, batch: list[tuple]) -> int:
    if not batch:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT IGNORE INTO health_records
                (record_hash, type, source_name, source_version, device, unit,
                 value_text, value_num, creation_at, start_at, end_at, local_date, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            batch,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def flush_activity_summaries(conn, batch: list[tuple]) -> int:
    if not batch:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO activity_summaries
                (summary_date, active_energy_burned, active_energy_burned_goal,
                 active_energy_burned_unit, apple_move_time, apple_move_time_goal,
                 apple_exercise_time, apple_exercise_time_goal,
                 apple_stand_hours, apple_stand_hours_goal)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                active_energy_burned=VALUES(active_energy_burned),
                active_energy_burned_goal=VALUES(active_energy_burned_goal),
                active_energy_burned_unit=VALUES(active_energy_burned_unit),
                apple_move_time=VALUES(apple_move_time),
                apple_move_time_goal=VALUES(apple_move_time_goal),
                apple_exercise_time=VALUES(apple_exercise_time),
                apple_exercise_time_goal=VALUES(apple_exercise_time_goal),
                apple_stand_hours=VALUES(apple_stand_hours),
                apple_stand_hours_goal=VALUES(apple_stand_hours_goal)
            """,
            batch,
        )
        affected = cur.rowcount
    conn.commit()
    return affected


def save_workout(conn, workout: dict, stats: list[dict], events: list[dict], route: dict | None) -> int:
    workout_hash = hash_payload(
        workout.get("workoutActivityType"),
        workout.get("sourceName"),
        workout.get("sourceVersion"),
        workout.get("device"),
        workout.get("startDate"),
        workout.get("endDate"),
        workout.get("duration"),
        workout.get("totalDistance"),
        workout.get("totalEnergyBurned"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO workouts
                (workout_hash, activity_type, duration, duration_unit,
                 total_distance, total_distance_unit, total_energy_burned,
                 total_energy_burned_unit, source_name, source_version,
                 device, creation_at, start_at, end_at, local_date, route_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                route_file=COALESCE(VALUES(route_file), route_file),
                total_distance=COALESCE(VALUES(total_distance), total_distance),
                total_energy_burned=COALESCE(VALUES(total_energy_burned), total_energy_burned),
                duration=COALESCE(VALUES(duration), duration)
            """,
            (
                workout_hash,
                workout.get("workoutActivityType"),
                try_float(workout.get("duration")),
                workout.get("durationUnit"),
                try_float(workout.get("totalDistance")),
                workout.get("totalDistanceUnit"),
                try_float(workout.get("totalEnergyBurned")),
                workout.get("totalEnergyBurnedUnit"),
                workout.get("sourceName"),
                workout.get("sourceVersion"),
                workout.get("device"),
                parse_health_datetime(workout.get("creationDate")),
                parse_health_datetime(workout.get("startDate")),
                parse_health_datetime(workout.get("endDate")),
                parse_date(workout.get("startDate")),
                route.get("file_path") if route else None,
            ),
        )
        cur.execute("SELECT id FROM workouts WHERE workout_hash=%s", (workout_hash,))
        workout_id = cur.fetchone()["id"]

        if stats:
            rows = []
            for stat in stats:
                stat_hash = hash_payload(workout_hash, stat)
                rows.append(
                    (
                        stat_hash,
                        workout_id,
                        stat.get("type"),
                        parse_health_datetime(stat.get("startDate")),
                        parse_health_datetime(stat.get("endDate")),
                        try_float(stat.get("average")),
                        try_float(stat.get("minimum")),
                        try_float(stat.get("maximum")),
                        try_float(stat.get("sum")),
                        stat.get("unit"),
                    )
                )
            cur.executemany(
                """
                INSERT IGNORE INTO workout_statistics
                    (statistic_hash, workout_id, type, start_at, end_at,
                     average_value, minimum_value, maximum_value, sum_value, unit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

        if events:
            rows = []
            for event in events:
                event_hash = hash_payload(workout_hash, event)
                rows.append(
                    (
                        event_hash,
                        workout_id,
                        event.get("type"),
                        parse_health_datetime(event.get("date")),
                        try_float(event.get("duration")),
                        event.get("durationUnit"),
                    )
                )
            cur.executemany(
                """
                INSERT IGNORE INTO workout_events
                    (event_hash, workout_id, type, event_at, duration, duration_unit)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

        if route:
            route_hash = hash_payload(
                route.get("file_path"),
                route.get("startDate"),
                route.get("endDate"),
                route.get("sourceName"),
                route.get("sourceVersion"),
            )
            cur.execute(
                """
                INSERT INTO workout_routes
                    (route_hash, file_path, source_name, source_version, device,
                     creation_at, start_at, end_at, workout_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    workout_id=VALUES(workout_id),
                    source_name=COALESCE(VALUES(source_name), source_name),
                    source_version=COALESCE(VALUES(source_version), source_version),
                    device=COALESCE(VALUES(device), device),
                    creation_at=COALESCE(VALUES(creation_at), creation_at),
                    start_at=COALESCE(VALUES(start_at), start_at),
                    end_at=COALESCE(VALUES(end_at), end_at)
                """,
                (
                    route_hash,
                    route.get("file_path"),
                    route.get("sourceName"),
                    route.get("sourceVersion"),
                    route.get("device"),
                    parse_health_datetime(route.get("creationDate")),
                    parse_health_datetime(route.get("startDate")),
                    parse_health_datetime(route.get("endDate")),
                    workout_id,
                ),
            )
    conn.commit()
    return workout_id


def import_xml(conn, batch_id: int, force: bool = False) -> None:
    if not XML_PATH.exists():
        print(f"✗ 找不到 XML: {XML_PATH}")
        return
    if not force and file_already_imported(conn, XML_PATH, "xml"):
        print("⚡ 主 XML 未变化，跳过")
        return

    import_file = create_import_file(conn, batch_id, XML_PATH, "xml", resume=True)
    import_file_id = import_file["id"]
    resume_record_seen = int(import_file["resume_records_seen"] or 0)
    resume_record_inserted = int(import_file["resume_records_inserted"] or 0)
    print(f"📂 开始导入 XML: {XML_PATH}")
    if resume_record_seen:
        print(
            f"↻ 从上次断点恢复：跳过前 {resume_record_seen:,} 条 Record，"
            f"沿用已写入 {resume_record_inserted:,}",
            flush=True,
        )
    started = time.time()

    record_batch: list[tuple] = []
    activity_batch: list[tuple] = []
    record_seen = 0
    record_inserted = resume_record_inserted
    activity_seen = 0
    activity_affected = 0
    workout_seen = 0

    current_workout = None
    current_stats: list[dict] = []
    current_events: list[dict] = []
    current_route = None

    def effective_record_seen() -> int:
        return max(record_seen, resume_record_seen)

    try:
        for event, elem in ET.iterparse(str(XML_PATH), events=("start", "end")):
            tag = elem.tag

            if event == "start":
                if tag == "Workout":
                    current_workout = dict(elem.attrib)
                    current_stats = []
                    current_events = []
                    current_route = None
                elif tag == "WorkoutRoute":
                    current_route = dict(elem.attrib)
                continue

            if tag == "Me":
                upsert_profile(conn, elem.attrib)
                elem.clear()
                continue

            if tag == "ExportDate":
                update_export_date(conn, elem.attrib.get("value"))
                elem.clear()
                continue

            if tag == "Record":
                record_seen += 1
                if record_seen <= resume_record_seen:
                    if record_seen % PROGRESS_UPDATE_INTERVAL == 0:
                        update_import_progress(conn, import_file_id, effective_record_seen(), record_inserted)
                    if record_seen % 200000 == 0:
                        print(
                            f"  … 快速跳过 {effective_record_seen():,} 条已扫描 Record，已写入 {record_inserted:,}",
                            flush=True,
                        )
                    elem.clear()
                    continue

                attrs = dict(elem.attrib)
                metadata = {}
                hrv_samples = []
                for child in elem:
                    if child.tag == "MetadataEntry":
                        key = child.attrib.get("key")
                        if key:
                            metadata[key] = child.attrib.get("value")
                    elif child.tag == "HeartRateVariabilityMetadataList":
                        for bpm in child:
                            hrv_samples.append(
                                {"bpm": bpm.attrib.get("bpm"), "time": bpm.attrib.get("time")}
                            )
                if hrv_samples:
                    metadata["hrv_samples"] = hrv_samples

                record_hash = hash_payload(attrs, metadata)
                record_batch.append(
                    (
                        record_hash,
                        attrs.get("type"),
                        attrs.get("sourceName"),
                        attrs.get("sourceVersion"),
                        attrs.get("device"),
                        attrs.get("unit"),
                        attrs.get("value"),
                        try_float(attrs.get("value")),
                        parse_health_datetime(attrs.get("creationDate")),
                        parse_health_datetime(attrs.get("startDate")),
                        parse_health_datetime(attrs.get("endDate")),
                        parse_date(attrs.get("startDate")),
                        json.dumps(metadata, ensure_ascii=False) if metadata else None,
                    )
                )
                if len(record_batch) >= BATCH_SIZE:
                    record_inserted += flush_records(conn, record_batch)
                    record_batch.clear()
                    if record_seen % PROGRESS_UPDATE_INTERVAL == 0:
                        update_import_progress(conn, import_file_id, effective_record_seen(), record_inserted)
                    if record_seen % 200000 == 0:
                        print(
                            f"  … 已扫描 {effective_record_seen():,} 条 Record，已写入 {record_inserted:,}",
                            flush=True,
                        )
                elem.clear()
                continue

            if tag == "WorkoutStatistics" and current_workout is not None:
                current_stats.append(dict(elem.attrib))
                elem.clear()
                continue

            if tag == "WorkoutEvent" and current_workout is not None:
                current_events.append(dict(elem.attrib))
                elem.clear()
                continue

            if tag == "FileReference" and current_route is not None:
                current_route["file_path"] = elem.attrib.get("path")
                elem.clear()
                continue

            if tag == "Workout":
                if current_workout is not None:
                    save_workout(conn, current_workout, current_stats, current_events, current_route)
                    workout_seen += 1
                current_workout = None
                current_stats = []
                current_events = []
                current_route = None
                elem.clear()
                continue

            if tag == "ActivitySummary":
                attrs = dict(elem.attrib)
                activity_batch.append(
                    (
                        parse_date(attrs.get("dateComponents")),
                        try_float(attrs.get("activeEnergyBurned")),
                        try_float(attrs.get("activeEnergyBurnedGoal")),
                        attrs.get("activeEnergyBurnedUnit"),
                        try_float(attrs.get("appleMoveTime")),
                        try_float(attrs.get("appleMoveTimeGoal")),
                        try_float(attrs.get("appleExerciseTime")),
                        try_float(attrs.get("appleExerciseTimeGoal")),
                        try_float(attrs.get("appleStandHours")),
                        try_float(attrs.get("appleStandHoursGoal")),
                    )
                )
                activity_seen += 1
                if len(activity_batch) >= BATCH_SIZE:
                    activity_affected += flush_activity_summaries(conn, activity_batch)
                    activity_batch.clear()
                elem.clear()
                continue

            elem.clear()

        if record_batch:
            record_inserted += flush_records(conn, record_batch)
        if activity_batch:
            activity_affected += flush_activity_summaries(conn, activity_batch)

        finish_import_file(
            conn,
            import_file_id,
            effective_record_seen() + activity_seen + workout_seen,
            record_inserted,
        )
        elapsed = time.time() - started
        print(
            f"✓ XML 导入完成: scanned Record={effective_record_seen():,}, inserted Record={record_inserted:,}, "
            f"Workout={workout_seen}, ActivitySummary={activity_seen} ({elapsed:.1f}s)"
        )
    except Exception:
        fail_import_file(
            conn,
            import_file_id,
            effective_record_seen() + activity_seen + workout_seen,
            record_inserted,
        )
        raise


def parse_gpx_points(gpx_file: Path) -> list[tuple]:
    points = []
    ns = {"gpx": "http://www.topografix.com/GPX/1/1"}
    try:
        tree = ET.parse(str(gpx_file))
    except ET.ParseError as exc:
        print(f"  ✗ GPX 解析失败 {gpx_file.name}: {exc}")
        return points

    root = tree.getroot()
    for index, trkpt in enumerate(root.findall(".//gpx:trkpt", ns)):
        lon = try_float(trkpt.get("lon"))
        lat = try_float(trkpt.get("lat"))
        if lon is None or lat is None:
            continue

        ele = trkpt.findtext("gpx:ele", default=None, namespaces=ns)
        recorded_at = trkpt.findtext("gpx:time", default=None, namespaces=ns)
        ext = trkpt.find("gpx:extensions", ns)

        speed = course = h_acc = v_acc = None
        if ext is not None:
            for child in ext:
                name = child.tag.split("}", 1)[-1]
                if name == "speed":
                    speed = try_float(child.text)
                elif name == "course":
                    course = try_float(child.text)
                elif name == "hAcc":
                    h_acc = try_float(child.text)
                elif name == "vAcc":
                    v_acc = try_float(child.text)

        points.append(
            (
                index,
                lon,
                lat,
                try_float(ele),
                parse_iso_datetime(recorded_at),
                speed,
                course,
                h_acc,
                v_acc,
            )
        )
    return points


def import_gpx_files(conn, batch_id: int, force: bool = False) -> None:
    gpx_files = sorted(ROUTES_DIR.glob("*.gpx"))
    if not gpx_files:
        print("⚡ 没有找到 GPX 文件")
        return

    pending = [path for path in gpx_files if force or not file_already_imported(conn, path, "gpx")]
    print(f"📍 GPX 文件总数 {len(gpx_files)}，待导入 {len(pending)}")

    for gpx_file in pending:
        import_file_id = create_import_file(conn, batch_id, gpx_file, "gpx")["id"]
        points: list[tuple] = []
        try:
            route_path = f"/workout-routes/{gpx_file.name}"
            route_hash = hash_payload(route_path)
            points = parse_gpx_points(gpx_file)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workout_routes (route_hash, file_path)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE route_hash=VALUES(route_hash)
                    """,
                    (route_hash, route_path),
                )
                cur.execute("SELECT id FROM workout_routes WHERE file_path=%s", (route_path,))
                route_id = cur.fetchone()["id"]
                cur.execute("DELETE FROM route_points WHERE route_id=%s", (route_id,))
                if points:
                    cur.executemany(
                        """
                        INSERT INTO route_points
                            (route_id, point_index, longitude, latitude, elevation,
                             recorded_at, speed, course, h_acc, v_acc)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [(route_id,) + point for point in points],
                    )
            conn.commit()
            finish_import_file(conn, import_file_id, len(points), len(points))
            print(f"  ✓ {gpx_file.name}: {len(points):,} 个轨迹点")
        except Exception:
            fail_import_file(conn, import_file_id, len(points), 0)
            raise


def parse_ecg_file(ecg_file: Path) -> tuple[dict, list[float]]:
    metadata = {}
    voltages: list[float] = []

    with ecg_file.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            if len(row) == 1:
                value = row[0].strip()
                if not value:
                    continue
                try:
                    voltages.append(float(value))
                except ValueError:
                    continue
                continue

            key = row[0].strip()
            value = row[1].strip().strip('"') if len(row) > 1 else ""
            metadata[key] = value

    sample_rate = None
    if metadata.get("采样率"):
        match = re.search(r"\d+", metadata["采样率"])
        sample_rate = int(match.group()) if match else None

    result = {
        "record_at": parse_health_datetime(metadata.get("记录日期")),
        "classification": metadata.get("分类"),
        "symptoms": metadata.get("症状"),
        "software_version": metadata.get("软件版本"),
        "device": metadata.get("设备"),
        "sample_rate": sample_rate,
        "lead_name": metadata.get("导联"),
        "unit": metadata.get("单位"),
    }
    return result, voltages


def import_ecg_files(conn, batch_id: int, force: bool = False) -> None:
    ecg_files = sorted(ECG_DIR.glob("*.csv"))
    if not ecg_files:
        print("⚡ 没有找到 ECG 文件")
        return

    pending = [path for path in ecg_files if force or not file_already_imported(conn, path, "ecg")]
    print(f"🫀 ECG 文件总数 {len(ecg_files)}，待导入 {len(pending)}")

    for ecg_file in pending:
        import_file_id = create_import_file(conn, batch_id, ecg_file, "ecg")["id"]
        voltages: list[float] = []
        try:
            metadata, voltages = parse_ecg_file(ecg_file)
            ecg_hash = hash_payload(ecg_file.name, metadata, len(voltages))
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ecg_readings
                        (ecg_hash, file_name, record_at, classification, symptoms,
                         software_version, device, sample_rate, lead_name, unit, voltage_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        ecg_hash=VALUES(ecg_hash),
                        record_at=VALUES(record_at),
                        classification=VALUES(classification),
                        symptoms=VALUES(symptoms),
                        software_version=VALUES(software_version),
                        device=VALUES(device),
                        sample_rate=VALUES(sample_rate),
                        lead_name=VALUES(lead_name),
                        unit=VALUES(unit),
                        voltage_data=VALUES(voltage_data)
                    """,
                    (
                        ecg_hash,
                        ecg_file.name,
                        metadata["record_at"],
                        metadata["classification"],
                        metadata["symptoms"],
                        metadata["software_version"],
                        metadata["device"],
                        metadata["sample_rate"],
                        metadata["lead_name"],
                        metadata["unit"],
                        json.dumps(voltages),
                    ),
                )
            conn.commit()
            finish_import_file(conn, import_file_id, len(voltages), 1)
            print(f"  ✓ {ecg_file.name}: {len(voltages):,} 个采样点")
        except Exception:
            fail_import_file(conn, import_file_id, len(voltages), 0)
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Apple Health export into MySQL")
    parser.add_argument("--force", action="store_true", help="强制重扫所有文件")
    parser.add_argument("--xml-only", action="store_true", help="仅导入导出.xml")
    parser.add_argument("--gpx-only", action="store_true", help="仅导入 GPX")
    parser.add_argument("--ecg-only", action="store_true", help="仅导入 ECG")
    args = parser.parse_args()

    ensure_password()
    ensure_database()

    conn = get_conn()
    try:
        init_schema(conn)
        ensure_runtime_schema(conn)
        stale_batches, stale_files = mark_stale_running_jobs(conn)
        if stale_batches or stale_files:
            print(f"⚠ 发现残留运行状态，已修正 batch={stale_batches}，files={stale_files}")
        batch_id = create_import_batch(conn, "full" if args.force else "incremental")
        if not args.gpx_only and not args.ecg_only:
            import_xml(conn, batch_id, force=args.force)
        if not args.xml_only and not args.ecg_only:
            import_gpx_files(conn, batch_id, force=args.force)
        if not args.xml_only and not args.gpx_only:
            import_ecg_files(conn, batch_id, force=args.force)
        finish_import_batch(conn, batch_id, "completed")
    except Exception:
        if "batch_id" in locals():
            finish_import_batch(conn, batch_id, "failed")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
