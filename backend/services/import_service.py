from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timedelta

from backend.config import IMPORT_RATE_WINDOW_MINUTES, IMPORT_STALE_SECONDS, LOCAL_TIMEZONE, XML_PATH
from backend.database import get_db
from backend.utils import row_to_dict, rows_to_list

_xml_record_total_cache: int | None = None


def get_xml_record_total() -> int | None:
    global _xml_record_total_cache
    if _xml_record_total_cache is not None:
        return _xml_record_total_cache
    if not XML_PATH.exists():
        return None

    rg = shutil.which("rg")
    if rg:
        try:
            result = subprocess.run(
                [rg, "-c", "<Record ", str(XML_PATH)],
                check=True,
                capture_output=True,
                text=True,
            )
            _xml_record_total_cache = int(result.stdout.strip())
            return _xml_record_total_cache
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass

    marker = b"<Record "
    overlap = len(marker) - 1
    total = 0
    trailing = b""

    try:
        with XML_PATH.open("rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                data = trailing + chunk
                total += data.count(marker)
                trailing = data[-overlap:] if len(data) >= overlap else data
    except OSError:
        return None

    _xml_record_total_cache = total
    return _xml_record_total_cache


def get_import_status_payload() -> dict:
    now = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT id, batch_type, status, note, started_at, completed_at
            FROM import_batches
            ORDER BY id DESC
            LIMIT 1
            """
        )
        batch = cur.fetchone()
        if not batch:
            return {
                "batch": None,
                "files": {
                    "total": 0,
                    "completed": 0,
                    "running": 0,
                    "failed": 0,
                    "records_seen": 0,
                    "records_inserted": 0,
                    "progress_percent": None,
                    "record_progress_percent": None,
                    "estimated_total_records": get_xml_record_total(),
                    "scan_rate_per_minute": None,
                    "eta_minutes": None,
                    "estimated_completion_at": None,
                    "speed_window_minutes": IMPORT_RATE_WINDOW_MINUTES,
                    "last_progress_at": None,
                    "seconds_since_progress": None,
                    "stalled": False,
                },
                "tables": {
                    "profile_ready": False,
                    "health_records_max_id": 0,
                    "workouts": 0,
                    "ecg_readings": 0,
                },
                "recent_files": [],
            }

        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running,
                   SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                   SUM(records_seen) AS records_seen,
                   SUM(records_inserted) AS records_inserted,
                   SUM(COALESCE(run_started_records_seen, 0)) AS run_started_records_seen,
                   SUM(COALESCE(run_started_records_inserted, 0)) AS run_started_records_inserted,
                   MAX(last_progress_at) AS last_progress_at
            FROM import_files
            WHERE batch_id=%s
            """,
            (batch["id"],),
        )
        file_summary = cur.fetchone() or {}

        cur.execute(
            """
            SELECT id, file_path, import_type, status, records_seen, records_inserted, created_at, completed_at, last_progress_at
            FROM import_files
            WHERE batch_id=%s
            ORDER BY id DESC
            LIMIT 8
            """,
            (batch["id"],),
        )
        recent_files = cur.fetchall()

        cur.execute(
            """
            SELECT id, file_path, import_type, status, records_seen, records_inserted,
                   run_started_records_seen, run_started_records_inserted,
                   created_at, run_started_at, completed_at, last_progress_at
            FROM import_files
            WHERE batch_id=%s AND import_type='xml'
            ORDER BY CASE WHEN status='running' THEN 0 ELSE 1 END, id DESC
            LIMIT 1
            """,
            (batch["id"],),
        )
        xml_file = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS count FROM profile")
        profile_count = cur.fetchone()["count"]

        cur.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM health_records")
        health_records_max_id = cur.fetchone()["max_id"]

        cur.execute("SELECT COUNT(*) AS count FROM workouts")
        workout_count = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) AS count FROM ecg_readings")
        ecg_count = cur.fetchone()["count"]

    total_files = int(file_summary.get("total") or 0)
    completed_files = int(file_summary.get("completed") or 0)
    progress_percent = round((completed_files / total_files) * 100, 1) if total_files else None
    records_seen = int(file_summary.get("records_seen") or 0)
    records_inserted = int(file_summary.get("records_inserted") or 0)
    estimated_total_records = get_xml_record_total()
    xml_records_seen = int(xml_file.get("records_seen") or 0) if xml_file else records_seen
    xml_records_inserted = int(xml_file.get("records_inserted") or 0) if xml_file else records_inserted
    record_progress_percent = (
        round((xml_records_seen / estimated_total_records) * 100, 1)
        if estimated_total_records and xml_records_seen
        else None
    )
    last_progress_at = file_summary.get("last_progress_at")
    seconds_since_progress = None
    stalled = False
    if last_progress_at:
        seconds_since_progress = max(int((now - last_progress_at).total_seconds()), 0)
        stalled = batch["status"] == "running" and seconds_since_progress > IMPORT_STALE_SECONDS

    scan_rate_per_minute = None
    insert_rate_per_minute = None
    eta_minutes = None
    estimated_completion_at = None
    current_run_records_seen = None
    current_run_records_inserted = None
    has_current_run_baseline = False

    if xml_file:
        xml_run_started_records_seen = int(xml_file.get("run_started_records_seen") or 0)
        xml_run_started_records_inserted = int(xml_file.get("run_started_records_inserted") or 0)
        has_current_run_baseline = xml_file.get("run_started_records_seen") is not None
        current_run_records_seen = max(xml_records_seen - xml_run_started_records_seen, 0)
        current_run_records_inserted = max(xml_records_inserted - xml_run_started_records_inserted, 0)

        cutoff = now - timedelta(minutes=IMPORT_RATE_WINDOW_MINUTES)
        latest_sample = None
        baseline_sample = None
        rate_reference_at = xml_file.get("last_progress_at") or now

        with get_db() as db, db.cursor() as cur:
            cur.execute(
                """
                SELECT id, recorded_at, records_seen, records_inserted
                FROM import_progress_samples
                WHERE import_file_id=%s
                ORDER BY recorded_at DESC, id DESC
                LIMIT 1
                """,
                (xml_file["id"],),
            )
            latest_sample = cur.fetchone()
            if batch["status"] == "running" and xml_file["status"] == "running":
                should_append_sample = not latest_sample
                if latest_sample and not should_append_sample:
                    latest_seen = int(latest_sample.get("records_seen") or 0)
                    latest_inserted = int(latest_sample.get("records_inserted") or 0)
                    latest_recorded_at = latest_sample.get("recorded_at")
                    should_append_sample = (
                        latest_seen != xml_records_seen
                        or latest_inserted != xml_records_inserted
                        or not latest_recorded_at
                        or (now - latest_recorded_at).total_seconds() >= 60
                    )
                if should_append_sample:
                    cur.execute(
                        """
                        INSERT INTO import_progress_samples (import_file_id, records_seen, records_inserted)
                        VALUES (%s, %s, %s)
                        """,
                        (xml_file["id"], xml_records_seen, xml_records_inserted),
                    )
                    db.commit()
                    latest_sample = {
                        "recorded_at": now,
                        "records_seen": xml_records_seen,
                        "records_inserted": xml_records_inserted,
                    }

            cur.execute(
                """
                SELECT recorded_at, records_seen, records_inserted
                FROM import_progress_samples
                WHERE import_file_id=%s AND recorded_at <= %s
                ORDER BY recorded_at DESC, id DESC
                LIMIT 1
                """,
                (xml_file["id"], cutoff),
            )
            baseline_sample = cur.fetchone()
            if not baseline_sample:
                cur.execute(
                    """
                    SELECT recorded_at, records_seen, records_inserted
                    FROM import_progress_samples
                    WHERE import_file_id=%s AND recorded_at >= %s
                    ORDER BY recorded_at ASC, id ASC
                    LIMIT 1
                    """,
                    (xml_file["id"], cutoff),
                )
                baseline_sample = cur.fetchone()

        if latest_sample:
            rate_reference_at = latest_sample["recorded_at"] or rate_reference_at

        if (
            latest_sample
            and baseline_sample
            and latest_sample["recorded_at"]
            and baseline_sample["recorded_at"]
            and latest_sample["recorded_at"] > baseline_sample["recorded_at"]
        ):
            elapsed_seconds = max(
                int((latest_sample["recorded_at"] - baseline_sample["recorded_at"]).total_seconds()),
                1,
            )
            recent_records_seen = max(
                int(latest_sample.get("records_seen") or 0) - int(baseline_sample.get("records_seen") or 0),
                0,
            )
            recent_records_inserted = max(
                int(latest_sample.get("records_inserted") or 0) - int(baseline_sample.get("records_inserted") or 0),
                0,
            )
            if recent_records_seen > 0:
                scan_rate_per_minute = round(recent_records_seen / elapsed_seconds * 60, 1)
            if recent_records_inserted > 0:
                insert_rate_per_minute = round(recent_records_inserted / elapsed_seconds * 60, 1)

        if scan_rate_per_minute is None and batch.get("started_at") and current_run_records_seen and has_current_run_baseline:
            elapsed_seconds = max(int((rate_reference_at - batch["started_at"]).total_seconds()), 1)
            scan_rate_per_minute = round(current_run_records_seen / elapsed_seconds * 60, 1)
            if current_run_records_inserted and current_run_records_inserted > 0:
                insert_rate_per_minute = round(current_run_records_inserted / elapsed_seconds * 60, 1)

        if estimated_total_records and scan_rate_per_minute and scan_rate_per_minute > 0 and not stalled:
            remaining_records = max(estimated_total_records - xml_records_seen, 0)
            eta_minutes = round(remaining_records / scan_rate_per_minute, 1)
            estimated_completion_at = rate_reference_at + timedelta(minutes=eta_minutes)

    return {
        "batch": row_to_dict(batch),
        "files": {
            "total": total_files,
            "completed": completed_files,
            "running": int(file_summary.get("running") or 0),
            "failed": int(file_summary.get("failed") or 0),
            "records_seen": records_seen,
            "records_inserted": records_inserted,
            "xml_records_seen": xml_records_seen,
            "xml_records_inserted": xml_records_inserted,
            "current_run_records_seen": current_run_records_seen if has_current_run_baseline else None,
            "current_run_records_inserted": current_run_records_inserted if has_current_run_baseline else None,
            "progress_percent": progress_percent,
            "record_progress_percent": record_progress_percent,
            "estimated_total_records": estimated_total_records,
            "scan_rate_per_minute": scan_rate_per_minute,
            "insert_rate_per_minute": insert_rate_per_minute,
            "eta_minutes": eta_minutes,
            "estimated_completion_at": estimated_completion_at,
            "speed_window_minutes": IMPORT_RATE_WINDOW_MINUTES,
            "last_progress_at": last_progress_at,
            "seconds_since_progress": seconds_since_progress,
            "stalled": stalled,
        },
        "tables": {
            "profile_ready": profile_count > 0,
            "health_records_max_id": int(health_records_max_id or 0),
            "workouts": int(workout_count or 0),
            "ecg_readings": int(ecg_count or 0),
        },
        "recent_files": rows_to_list(recent_files),
    }
