"""
Apple Health Personal API for MySQL 8

运行:
    export HEALTH_DB_PASSWORD='your-password'
    uvicorn main:app --host 0.0.0.0 --port 18000
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

import pymysql
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pymysql.cursors import DictCursor

app = FastAPI(title="Apple Health Personal API", version="2.0")
BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
LOCAL_TIMEZONE = ZoneInfo(os.getenv("HEALTH_LOCAL_TZ", "Asia/Shanghai"))
IMPORT_STALE_SECONDS = int(os.getenv("IMPORT_STALE_SECONDS", "300"))
IMPORT_RATE_WINDOW_MINUTES = int(os.getenv("IMPORT_RATE_WINDOW_MINUTES", "10"))
XML_PATH = BASE_DIR / "apple_health_export" / "导出.xml"
_xml_record_total_cache: int | None = None
INGEST_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS ingest_events (
        id                  BIGINT PRIMARY KEY AUTO_INCREMENT,
        device_id           VARCHAR(128) NOT NULL,
        bundle_id           VARCHAR(255) NOT NULL,
        sent_at             DATETIME NOT NULL,
        received_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        item_count          INT NOT NULL DEFAULT 0,
        accepted_count      INT NOT NULL DEFAULT 0,
        deduplicated_count  INT NOT NULL DEFAULT 0,
        status              VARCHAR(32) NOT NULL DEFAULT 'received',
        error_message       VARCHAR(255) NULL,
        payload_json        JSON NOT NULL,
        KEY idx_ingest_events_device (device_id, received_at),
        KEY idx_ingest_events_status (status, received_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS device_sync_state (
        device_id               VARCHAR(128) PRIMARY KEY,
        bundle_id               VARCHAR(255) NOT NULL,
        last_seen_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_sent_at            DATETIME NULL,
        last_sync_at            DATETIME NULL,
        last_sync_status        VARCHAR(32) NOT NULL DEFAULT 'pending',
        last_error_message      VARCHAR(255) NULL,
        last_items_count        INT NOT NULL DEFAULT 0,
        last_accepted_count     INT NOT NULL DEFAULT 0,
        last_deduplicated_count INT NOT NULL DEFAULT 0,
        updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_device_sync_updated (updated_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS device_sync_anchors (
        device_id       VARCHAR(128) NOT NULL,
        record_type     VARCHAR(128) NOT NULL,
        anchor_value    MEDIUMTEXT NOT NULL,
        updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (device_id, record_type),
        CONSTRAINT fk_device_sync_anchors_state FOREIGN KEY (device_id) REFERENCES device_sync_state(device_id) ON DELETE CASCADE
    )
    """,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIR.exists():
    app.mount("/dashboard", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="dashboard")


def db_config(*, autocommit: bool = True) -> dict:
    password = os.getenv("HEALTH_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("缺少 HEALTH_DB_PASSWORD 环境变量。")
    return {
        "host": os.getenv("HEALTH_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("HEALTH_DB_PORT", "3306")),
        "user": os.getenv("HEALTH_DB_USER", "root"),
        "password": password,
        "database": os.getenv("HEALTH_DB_NAME", "apple_health"),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": autocommit,
    }


@contextmanager
def get_db(*, autocommit: bool = True):
    conn = pymysql.connect(**db_config(autocommit=autocommit))
    try:
        yield conn
    finally:
        conn.close()


def rows_to_list(rows) -> list[dict]:
    return [dict(row) for row in rows]


def row_to_dict(row) -> dict | None:
    return dict(row) if row else None


def build_date_filters(column: str, start: Optional[str], end: Optional[str]) -> tuple[list[str], list]:
    conditions = []
    params: list = []
    if start:
        conditions.append(f"{column} >= %s")
        params.append(start)
    if end:
        conditions.append(f"{column} <= %s")
        params.append(end)
    return conditions, params


def ensure_ingest_tables(cur) -> None:
    for statement in INGEST_TABLE_STATEMENTS:
        cur.execute(statement)


def ensure_import_status_schema(cur) -> None:
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


def compact_dict(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def normalize_ingest_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)


def isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(microsecond=0).isoformat()
    return value.replace(microsecond=0).isoformat()


def format_value_text(value: float | None) -> str | None:
    if value is None:
        return None
    if float(value).is_integer():
        return str(int(value))
    return format(value, ".15g")


def make_record_hash(payload: "IngestPayload", item: "IngestItem") -> str:
    base = f"bridge|{payload.device_id}|{payload.bundle_id}|{item.type}|{item.uuid}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def health_record_row_from_ingest(payload: "IngestPayload", item: "IngestItem") -> tuple:
    start_at = normalize_ingest_datetime(item.start_at)
    end_at = normalize_ingest_datetime(item.end_at)
    sent_at = normalize_ingest_datetime(payload.sent_at)
    metadata = {
        **item.metadata,
        "bridge_device_id": payload.device_id,
        "bridge_bundle_id": payload.bundle_id,
        "bridge_sent_at": isoformat_z(payload.sent_at),
        "bridge_kind": item.kind,
        "bridge_source": item.source,
    }
    value_text = (
        item.metadata.get("category_value_label")
        or item.metadata.get("category_value_raw")
        or format_value_text(item.value)
    )
    device_payload = compact_dict(
        {
            "device_id": payload.device_id,
            "bundle_id": payload.bundle_id,
            "product_type": item.metadata.get("product_type"),
            "source_bundle_id": item.metadata.get("source_bundle_id"),
        }
    )
    return (
        make_record_hash(payload, item),
        item.type,
        item.metadata.get("source_name") or payload.device_id,
        item.metadata.get("source_version"),
        json.dumps(device_payload, ensure_ascii=False, sort_keys=True) if device_payload else None,
        item.unit,
        value_text,
        item.value,
        sent_at,
        start_at,
        end_at,
        start_at.date(),
        json.dumps(metadata, ensure_ascii=False, sort_keys=True),
    )


def serialize_payload(payload: "IngestPayload") -> str:
    return json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)


def require_ingest_token(authorization: str | None) -> None:
    expected = os.getenv("INGEST_API_TOKEN", "").strip()
    if not expected:
        return

    expected_header = f"Bearer {expected}"
    if authorization != expected_header:
        raise HTTPException(status_code=401, detail="无效的 ingest token")


def upsert_device_sync_state(
    cur,
    *,
    payload: "IngestPayload",
    status: str,
    accepted_count: int,
    deduplicated_count: int,
    error_message: str | None = None,
) -> None:
    sync_at = datetime.now(tz=LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)
    sent_at = normalize_ingest_datetime(payload.sent_at)
    cur.execute(
        """
        INSERT INTO device_sync_state (
            device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
            last_error_message, last_items_count, last_accepted_count, last_deduplicated_count
        )
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            bundle_id=VALUES(bundle_id),
            last_seen_at=CURRENT_TIMESTAMP,
            last_sent_at=VALUES(last_sent_at),
            last_sync_at=VALUES(last_sync_at),
            last_sync_status=VALUES(last_sync_status),
            last_error_message=VALUES(last_error_message),
            last_items_count=VALUES(last_items_count),
            last_accepted_count=VALUES(last_accepted_count),
            last_deduplicated_count=VALUES(last_deduplicated_count)
        """,
        (
            payload.device_id,
            payload.bundle_id,
            sent_at,
            sync_at,
            status,
            error_message,
            len(payload.items),
            accepted_count,
            deduplicated_count,
        ),
    )


class IngestItem(BaseModel):
    source: str
    kind: str
    type: str
    uuid: str
    start_at: datetime
    end_at: datetime
    value: float | None = None
    unit: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class IngestPayload(BaseModel):
    device_id: str
    bundle_id: str
    sent_at: datetime
    items: list[IngestItem] = Field(default_factory=list)
    anchors: dict[str, str] = Field(default_factory=dict)


@app.on_event("startup")
def startup() -> None:
    with get_db() as db, db.cursor() as cur:
        ensure_import_status_schema(cur)
        ensure_ingest_tables(cur)


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


@app.get("/")
def root():
    if FRONTEND_DIR.exists():
        return RedirectResponse(url="/dashboard/")
    return {"message": "Apple Health API is running"}


@app.get("/dashboard.html")
def dashboard_html():
    index_file = FRONTEND_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(404, "前端页面不存在")
    return FileResponse(index_file)


@app.get("/api/profile")
def get_profile():
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM profile WHERE id=1")
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "尚未导入数据")
        return row


@app.get("/api/import-status")
def get_import_status():
    now = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None)
    with get_db() as db, db.cursor() as cur:
        ensure_import_status_schema(cur)
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
                   SUM(CASE WHEN run_started_records_seen IS NULL THEN 1 ELSE 0 END) AS missing_run_started_records_seen,
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
        cur_rate_reference_at = xml_file.get("last_progress_at") or now

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
            cur_rate_reference_at = latest_sample["recorded_at"] or cur_rate_reference_at

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
            elapsed_seconds = max(int((cur_rate_reference_at - batch["started_at"]).total_seconds()), 1)
            scan_rate_per_minute = round(current_run_records_seen / elapsed_seconds * 60, 1)
            if current_run_records_inserted and current_run_records_inserted > 0:
                insert_rate_per_minute = round(current_run_records_inserted / elapsed_seconds * 60, 1)

        if estimated_total_records and scan_rate_per_minute and scan_rate_per_minute > 0 and not stalled:
            remaining_records = max(estimated_total_records - xml_records_seen, 0)
            eta_minutes = round(remaining_records / scan_rate_per_minute, 1)
            estimated_completion_at = cur_rate_reference_at + timedelta(minutes=eta_minutes)

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


@app.post("/ingest")
def ingest_samples(payload: IngestPayload, authorization: str | None = Header(None)):
    require_ingest_token(authorization)

    unsupported_items = [item.kind for item in payload.items if item.kind != "sample"]
    if unsupported_items:
        raise HTTPException(400, f"暂不支持的 ingest kind: {', '.join(sorted(set(unsupported_items)))}")

    payload_json = serialize_payload(payload)
    accepted_count = len(payload.items)
    deduplicated_count = 0
    event_id: int | None = None

    with get_db(autocommit=False) as db, db.cursor() as cur:
        ensure_ingest_tables(cur)
        cur.execute(
            """
            INSERT INTO ingest_events (
                device_id, bundle_id, sent_at, item_count, accepted_count,
                deduplicated_count, status, payload_json
            )
            VALUES (%s, %s, %s, %s, 0, 0, 'received', %s)
            """,
            (
                payload.device_id,
                payload.bundle_id,
                normalize_ingest_datetime(payload.sent_at),
                accepted_count,
                payload_json,
            ),
        )
        event_id = int(cur.lastrowid)

        try:
            rows = [health_record_row_from_ingest(payload, item) for item in payload.items]
            inserted_count = 0
            if rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO health_records (
                        record_hash, type, source_name, source_version, device, unit,
                        value_text, value_num, creation_at, start_at, end_at, local_date, metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )
                inserted_count = max(cur.rowcount, 0)

            deduplicated_count = accepted_count - inserted_count

            upsert_device_sync_state(
                cur,
                payload=payload,
                status="completed",
                accepted_count=accepted_count,
                deduplicated_count=deduplicated_count,
            )

            if payload.anchors:
                cur.executemany(
                    """
                    INSERT INTO device_sync_anchors (device_id, record_type, anchor_value)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        anchor_value=VALUES(anchor_value),
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    [(payload.device_id, record_type, anchor_value) for record_type, anchor_value in payload.anchors.items()],
                )

            cur.execute(
                """
                UPDATE ingest_events
                SET accepted_count=%s, deduplicated_count=%s, status='completed'
                WHERE id=%s
                """,
                (accepted_count, deduplicated_count, event_id),
            )
            db.commit()
        except Exception as exc:
            db.rollback()
            error_message = str(exc)[:255]
            with get_db() as failed_db, failed_db.cursor() as failed_cur:
                ensure_ingest_tables(failed_cur)
                upsert_device_sync_state(
                    failed_cur,
                    payload=payload,
                    status="failed",
                    accepted_count=0,
                    deduplicated_count=0,
                    error_message=error_message,
                )
                failed_cur.execute(
                    """
                    INSERT INTO ingest_events (
                        device_id, bundle_id, sent_at, item_count, accepted_count,
                        deduplicated_count, status, error_message, payload_json
                    )
                    VALUES (%s, %s, %s, %s, 0, 0, 'failed', %s, %s)
                    """,
                    (
                        payload.device_id,
                        payload.bundle_id,
                        normalize_ingest_datetime(payload.sent_at),
                        accepted_count,
                        error_message,
                        payload_json,
                    ),
                )
            raise HTTPException(500, f"ingest failed: {error_message}") from exc

    return {
        "ok": True,
        "accepted": accepted_count,
        "deduplicated": deduplicated_count,
    }


@app.get("/api/device-sync-state")
def get_device_sync_state():
    with get_db() as db, db.cursor() as cur:
        ensure_ingest_tables(cur)
        cur.execute(
            """
            SELECT device_id, COUNT(*) AS anchor_count, MAX(updated_at) AS anchors_updated_at
            FROM device_sync_anchors
            GROUP BY device_id
            """
        )
        anchor_rows = {
            row["device_id"]: {
                "anchor_count": int(row["anchor_count"] or 0),
                "anchors_updated_at": row["anchors_updated_at"],
            }
            for row in cur.fetchall()
        }

        cur.execute(
            """
            SELECT device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
                   last_error_message, last_items_count, last_accepted_count, last_deduplicated_count, updated_at
            FROM device_sync_state
            ORDER BY updated_at DESC
            """
        )
        devices = rows_to_list(cur.fetchall())

        cur.execute(
            """
            SELECT id, device_id, bundle_id, sent_at, received_at, item_count,
                   accepted_count, deduplicated_count, status, error_message
            FROM ingest_events
            ORDER BY id DESC
            LIMIT 10
            """
        )
        events = rows_to_list(cur.fetchall())

    for device in devices:
        device.update(anchor_rows.get(device["device_id"], {"anchor_count": 0, "anchors_updated_at": None}))

    return {
        "devices": devices,
        "recent_events": events,
    }


@app.get("/api/device-sync-state/anchors")
def get_device_sync_anchors(
    device_id: str = Query(...),
    bundle_id: Optional[str] = Query(None),
):
    with get_db() as db, db.cursor() as cur:
        ensure_ingest_tables(cur)
        cur.execute(
            """
            SELECT device_id, bundle_id, last_seen_at, last_sent_at, last_sync_at, last_sync_status,
                   last_error_message, last_items_count, last_accepted_count, last_deduplicated_count, updated_at
            FROM device_sync_state
            WHERE device_id = %s
            """,
            (device_id,),
        )
        device = cur.fetchone()
        if not device:
            raise HTTPException(404, "未找到该 device_id 的同步状态")

        if bundle_id and device["bundle_id"] != bundle_id:
            raise HTTPException(404, "device_id 存在，但 bundle_id 不匹配")

        cur.execute(
            """
            SELECT record_type, anchor_value, updated_at
            FROM device_sync_anchors
            WHERE device_id = %s
            ORDER BY record_type
            """,
            (device_id,),
        )
        anchor_rows = rows_to_list(cur.fetchall())

    anchors = {row["record_type"]: row["anchor_value"] for row in anchor_rows}
    anchors_updated_at = max((row["updated_at"] for row in anchor_rows), default=None)
    device_payload = dict(device)
    device_payload["anchor_count"] = len(anchor_rows)
    device_payload["anchors_updated_at"] = anchors_updated_at

    return {
        "device": device_payload,
        "anchors": anchors,
        "anchor_records": anchor_rows,
    }


@app.get("/api/records/types")
def list_record_types():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT type, COUNT(*) AS count, MIN(local_date) AS first_date, MAX(local_date) AS last_date
            FROM health_records
            GROUP BY type
            ORDER BY count DESC
            """
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/records/recent")
def get_recent_records(
    device_id: str = Query(...),
    bundle_id: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
):
    conditions = ["JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_device_id')) = %s"]
    params: list = [device_id]
    if bundle_id:
        conditions.append("JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_bundle_id')) = %s")
        params.append(bundle_id)
    if type:
        conditions.append("type = %s")
        params.append(type)

    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    where = " AND ".join(conditions)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                id,
                type,
                source_name,
                source_version,
                unit,
                value_text,
                value_num,
                start_at,
                end_at,
                local_date,
                metadata,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_device_id')) AS bridge_device_id,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_bundle_id')) AS bridge_bundle_id,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_sent_at')) AS bridge_sent_at,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_kind')) AS bridge_kind,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.bridge_source')) AS bridge_source
            FROM health_records
            WHERE {where}
            ORDER BY start_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total FROM health_records WHERE {where}", params)
        total = cur.fetchone()["total"]
    return {"total": total, "data": rows_to_list(rows)}


@app.get("/api/records")
def get_records(
    type: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    limit: int = Query(1000, le=10000),
    offset: int = Query(0),
):
    conditions = ["type = %s"]
    params: list = [type]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    if source:
        conditions.append("source_name = %s")
        params.append(source)

    where = " AND ".join(conditions)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, type, source_name, unit, value_text, value_num,
                   start_at, end_at, local_date, metadata
            FROM health_records
            WHERE {where}
            ORDER BY start_at
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total FROM health_records WHERE {where}", params)
        total = cur.fetchone()["total"]
    return {"total": total, "data": rows_to_list(rows)}


@app.get("/api/records/daily")
def get_daily_records(
    type: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    agg: Literal["sum", "avg", "max", "min", "count"] = Query("sum"),
):
    agg_sql = {
        "sum": "SUM(value_num)",
        "avg": "AVG(value_num)",
        "max": "MAX(value_num)",
        "min": "MIN(value_num)",
        "count": "COUNT(*)",
    }[agg]
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = [type]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date, {agg_sql} AS value, COUNT(*) AS count, MIN(unit) AS unit
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date
            ORDER BY local_date
            """,
            params,
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/steps")
def get_steps(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierStepCount", start, end, "sum")


@app.get("/api/heart-rate")
def get_heart_rate(
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    granularity: Literal["raw", "hourly", "daily"] = Query("daily"),
):
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = ["HKQuantityTypeIdentifierHeartRate"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    where = " AND ".join(conditions)

    with get_db() as db, db.cursor() as cur:
        if granularity == "raw":
            cur.execute(
                f"""
                SELECT start_at, value_num AS bpm, source_name
                FROM health_records
                WHERE {where}
                ORDER BY start_at
                LIMIT 5000
                """,
                params,
            )
        elif granularity == "hourly":
            cur.execute(
                f"""
                SELECT DATE_FORMAT(start_at, '%%Y-%%m-%%d %%H:00:00') AS hour,
                       AVG(value_num) AS avg_bpm,
                       MIN(value_num) AS min_bpm,
                       MAX(value_num) AS max_bpm,
                       COUNT(*) AS count
                FROM health_records
                WHERE {where}
                GROUP BY DATE_FORMAT(start_at, '%%Y-%%m-%%d %%H')
                ORDER BY hour
                """,
                params,
            )
        else:
            cur.execute(
                f"""
                SELECT local_date AS date,
                       AVG(value_num) AS avg_bpm,
                       MIN(value_num) AS min_bpm,
                       MAX(value_num) AS max_bpm,
                       COUNT(*) AS count
                FROM health_records
                WHERE {where}
                GROUP BY local_date
                ORDER BY local_date
                """,
                params,
            )
        return rows_to_list(cur.fetchall())


@app.get("/api/heart-rate/variability")
def get_hrv(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", start, end, "avg")


@app.get("/api/sleep")
def get_sleep(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    conditions = ["type = %s"]
    params: list = ["HKCategoryTypeIdentifierSleepAnalysis"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date,
                   value_text AS value,
                   SUM(TIMESTAMPDIFF(SECOND, start_at, end_at)) / 60.0 AS minutes,
                   COUNT(*) AS segments
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date, value_text
            ORDER BY local_date, value_text
            """,
            params,
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/sleep/daily")
def get_sleep_daily(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    conditions = [
        "type = %s",
        "value_text <> %s",
    ]
    params: list = [
        "HKCategoryTypeIdentifierSleepAnalysis",
        "HKCategoryValueSleepAnalysisInBed",
    ]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date,
                   SUM(TIMESTAMPDIFF(SECOND, start_at, end_at)) / 3600.0 AS total_hours,
                   MIN(start_at) AS sleep_start,
                   MAX(end_at) AS sleep_end
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date
            ORDER BY local_date
            """,
            params,
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/body-metrics")
def get_body_metrics(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    types = [
        "HKQuantityTypeIdentifierBodyMass",
        "HKQuantityTypeIdentifierBodyMassIndex",
        "HKQuantityTypeIdentifierBodyFatPercentage",
        "HKQuantityTypeIdentifierLeanBodyMass",
        "HKQuantityTypeIdentifierHeight",
    ]
    placeholders = ", ".join(["%s"] * len(types))
    conditions = [f"type IN ({placeholders})"]
    params: list = list(types)
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT type, unit, value_num AS value, start_at, local_date AS date, source_name
            FROM health_records
            WHERE {" AND ".join(conditions)}
            ORDER BY start_at
            """,
            params,
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/energy")
def get_energy(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    conditions = [
        "type IN (%s, %s)",
    ]
    params: list = [
        "HKQuantityTypeIdentifierActiveEnergyBurned",
        "HKQuantityTypeIdentifierBasalEnergyBurned",
    ]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT local_date AS date,
                   SUM(CASE WHEN type=%s THEN value_num ELSE 0 END) AS active_cal,
                   SUM(CASE WHEN type=%s THEN value_num ELSE 0 END) AS basal_cal
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY local_date
            ORDER BY local_date
            """,
            [
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierBasalEnergyBurned",
                *params,
            ],
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/oxygen-saturation")
def get_spo2(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierOxygenSaturation", start, end, "avg")


@app.get("/api/respiratory-rate")
def get_respiratory_rate(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    return get_daily_records("HKQuantityTypeIdentifierRespiratoryRate", start, end, "avg")


@app.get("/api/vo2max")
def get_vo2max():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT local_date AS date, value_num AS value, unit, source_name
            FROM health_records
            WHERE type=%s
            ORDER BY local_date
            """,
            ("HKQuantityTypeIdentifierVO2Max",),
        )
        return rows_to_list(cur.fetchall())


@app.get("/api/workouts")
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
    return {"total": total, "data": rows_to_list(rows)}


@app.get("/api/workouts/summary")
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
        return rows_to_list(cur.fetchall())


@app.get("/api/workouts/{workout_id}")
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

        return workout


@app.get("/api/activity-summaries")
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
        return rows_to_list(cur.fetchall())


@app.get("/api/ecg")
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
        return rows_to_list(cur.fetchall())


@app.get("/api/ecg/{ecg_id}")
def get_ecg_detail(ecg_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM ecg_readings WHERE id=%s", (ecg_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "ECG 记录不存在")
        if row.get("voltage_data"):
            row["voltage_data"] = json.loads(row["voltage_data"])
        return row


@app.get("/api/stats/overview")
def get_overview():
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM profile WHERE id=1")
        profile = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS total_records,
                   COUNT(DISTINCT type) AS distinct_types,
                   MIN(local_date) AS earliest_date,
                   MAX(local_date) AS latest_date
            FROM health_records
            """
        )
        record_stats = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS total_workouts,
                   ROUND(SUM(duration), 0) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            """
        )
        workout_stats = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(DISTINCT local_date) AS days
            FROM health_records
            WHERE type=%s
            """,
            ("HKCategoryTypeIdentifierSleepAnalysis",),
        )
        sleep_days = cur.fetchone()

        cur.execute(
            """
            SELECT SUM(value_num) AS steps
            FROM health_records
            WHERE type=%s
            """,
            ("HKQuantityTypeIdentifierStepCount",),
        )
        total_steps = cur.fetchone()

        cur.execute(
            """
            SELECT local_date AS date, SUM(value_num) AS steps
            FROM health_records
            WHERE type=%s AND local_date >= (CURDATE() - INTERVAL 7 DAY)
            GROUP BY local_date
            ORDER BY local_date
            """,
            ("HKQuantityTypeIdentifierStepCount",),
        )
        recent_steps = cur.fetchall()

    return {
        "profile": profile,
        "records": record_stats,
        "workouts": workout_stats,
        "sleep_days": sleep_days["days"] if sleep_days else 0,
        "total_steps": int(total_steps["steps"] or 0) if total_steps else 0,
        "recent_steps": rows_to_list(recent_steps),
    }


@app.get("/api/stats/monthly")
def get_monthly_stats(year: Optional[int] = Query(None)):
    conditions = ["local_date IS NOT NULL"]
    params: list = []
    if year:
        conditions.append("YEAR(local_date) = %s")
        params.append(year)

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT DATE_FORMAT(local_date, '%%Y-%%m') AS month,
                   SUM(CASE WHEN type=%s THEN value_num END) AS steps,
                   AVG(CASE WHEN type=%s THEN value_num END) AS avg_heart_rate,
                   SUM(CASE WHEN type=%s THEN value_num END) AS active_calories,
                   AVG(CASE WHEN type=%s THEN value_num END) AS avg_spo2
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY DATE_FORMAT(local_date, '%%Y-%%m')
            ORDER BY month
            """,
            [
                "HKQuantityTypeIdentifierStepCount",
                "HKQuantityTypeIdentifierHeartRate",
                "HKQuantityTypeIdentifierActiveEnergyBurned",
                "HKQuantityTypeIdentifierOxygenSaturation",
                *params,
            ],
        )
        return rows_to_list(cur.fetchall())
