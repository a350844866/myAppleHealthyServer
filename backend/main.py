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
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.request
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
_dashboard_ai_cache: dict[str, dict[str, Any]] = {}
OPENROUTER_API_URL = os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
OPENROUTER_ALLOWED_MODELS = tuple(
    model.strip()
    for model in os.getenv(
        "OPENROUTER_ALLOWED_MODELS",
        "anthropic/claude-sonnet-4.6,minimax/minimax-m2.7",
    ).split(",")
    if model.strip()
)
OPENROUTER_DEFAULT_MODEL = os.getenv(
    "OPENROUTER_MODEL",
    OPENROUTER_ALLOWED_MODELS[0] if OPENROUTER_ALLOWED_MODELS else "anthropic/claude-sonnet-4.6",
)
AI_ANALYSIS_CACHE_TTL_SECONDS = int(os.getenv("AI_ANALYSIS_CACHE_TTL_SECONDS", "900"))
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


def round_or_none(value: Any, digits: int = 1) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def as_int(value: Any) -> int:
    return int(value or 0)


def percent_change(current: float | int | None, previous: float | int | None, digits: int = 1) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((float(current) - float(previous)) / float(previous)) * 100, digits)


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def get_ai_config() -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    allowed_models = list(OPENROUTER_ALLOWED_MODELS)
    default_model = OPENROUTER_DEFAULT_MODEL if OPENROUTER_DEFAULT_MODEL in allowed_models else (
        allowed_models[0] if allowed_models else None
    )
    return {
        "available": bool(api_key and default_model),
        "default_model": default_model,
        "models": allowed_models,
        "provider": "openrouter",
    }


def resolve_ai_model(model: str | None) -> str:
    config = get_ai_config()
    if not config["available"]:
        raise HTTPException(503, "AI 分析未配置 OPENROUTER_API_KEY 或默认模型")
    selected = (model or config["default_model"] or "").strip()
    if selected not in config["models"]:
        raise HTTPException(400, f"不允许的模型: {selected or '-'}")
    return selected


def parse_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        raise ValueError("empty response")
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        raise ValueError("response is not json object")
    payload = json.loads(match.group(0))
    if not isinstance(payload, dict):
        raise ValueError("json payload is not object")
    return payload


def normalize_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(part for part in parts if part).strip()
    return ""


def build_ai_dashboard_prompt(home: dict[str, Any]) -> str:
    compact_context = {
        "generated_at": str(home.get("generated_at")),
        "today": home.get("today"),
        "steps": home.get("steps"),
        "sleep": home.get("sleep"),
        "heart_rate": home.get("heart_rate"),
        "workouts": home.get("workouts"),
        "recent_types": home.get("recent_types"),
        "sync": home.get("sync"),
        "heuristic_insights": home.get("insights"),
    }
    return (
        "你是一个中文健康数据分析助手。"
        "下面是个人 Apple Health 首页最近数据，不要做医疗诊断，不要夸张，不要输出空话。"
        "请只基于给定数据做首页级总结，强调近期变化、数据新鲜度、值得继续观察的维度。"
        "如果数据不足，要明确指出。\n\n"
        "输出必须是 JSON 对象，字段固定为："
        "title(string), summary(string), bullets(array of 3 short strings), "
        "watchouts(array of 0-3 short strings), next_focus(array of 2 short strings), confidence(string)。"
        "不要输出 markdown，不要输出代码块，不要输出 JSON 之外的内容。\n\n"
        f"数据上下文:\n{json.dumps(compact_context, ensure_ascii=False, default=str)}"
    )


def request_openrouter_analysis(prompt: str, model: str) -> tuple[dict[str, Any], dict[str, Any]]:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(503, "缺少 OPENROUTER_API_KEY")

    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a concise health dashboard analyst. Return valid JSON only.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0.3,
        "max_tokens": 500,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    site_url = os.getenv("OPENROUTER_SITE_URL", "").strip()
    app_name = os.getenv("OPENROUTER_APP_NAME", "myAppleHealthy").strip()
    if site_url:
        headers["HTTP-Referer"] = site_url
    if app_name:
        headers["X-Title"] = app_name

    last_payload: dict[str, Any] | None = None
    for attempt in range(2):
        req = urllib.request.Request(
            OPENROUTER_API_URL,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise HTTPException(502, f"OpenRouter 请求失败: HTTP {exc.code} {detail[:300]}") from exc
        except urllib.error.URLError as exc:
            raise HTTPException(502, f"OpenRouter 网络请求失败: {exc.reason}") from exc

        last_payload = payload
        message = ((payload.get("choices") or [{}])[0] or {}).get("message") or {}
        content = normalize_message_content(message.get("content"))
        if content:
            try:
                parsed = parse_json_object(content)
                usage = payload.get("usage") or {}
                return parsed, {
                    "prompt_tokens": usage.get("prompt_tokens"),
                    "completion_tokens": usage.get("completion_tokens"),
                    "total_tokens": usage.get("total_tokens"),
                }
            except (ValueError, json.JSONDecodeError):
                pass
        body["messages"][-1]["content"] = (
            prompt
            + "\n\n再次强调：请把一个合法 JSON 对象直接放在 assistant message.content 中，不要加解释，不要加代码块，不要只放 reasoning。"
        )

    preview = ""
    if last_payload:
        preview = json.dumps(last_payload.get("choices", [])[:1], ensure_ascii=False)[:600]
    raise HTTPException(502, f"OpenRouter 返回内容为空或无法解析，预览: {preview}")


def build_ai_cache_key(model: str, home: dict[str, Any]) -> str:
    snapshot = {
        "model": model,
        "home": home,
    }
    encoded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_string_list(value: Any, *, limit: int = 3) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized[:limit]


def normalize_ai_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": str(analysis.get("title") or "AI 近期分析").strip()[:255],
        "summary": str(analysis.get("summary") or "").strip(),
        "bullets": normalize_string_list(analysis.get("bullets"), limit=3),
        "watchouts": normalize_string_list(analysis.get("watchouts"), limit=3),
        "next_focus": normalize_string_list(analysis.get("next_focus"), limit=3),
        "confidence": str(analysis.get("confidence") or "").strip()[:64],
    }


def deserialize_json_field(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def row_to_ai_report(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "snapshot_hash": row.get("snapshot_hash"),
        "model": row.get("model"),
        "generated_at": row.get("generated_at"),
        "analysis": {
            "title": row.get("title") or "",
            "summary": row.get("summary") or "",
            "bullets": deserialize_json_field(row.get("bullets_json"), []),
            "watchouts": deserialize_json_field(row.get("watchouts_json"), []),
            "next_focus": deserialize_json_field(row.get("next_focus_json"), []),
            "confidence": row.get("confidence") or "",
        },
        "usage": deserialize_json_field(row.get("usage_json"), {}),
    }


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


def ensure_dashboard_ai_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_dashboard_reports (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            snapshot_hash CHAR(64) NOT NULL,
            model VARCHAR(128) NOT NULL,
            title VARCHAR(255) NOT NULL,
            summary TEXT NOT NULL,
            bullets_json JSON NOT NULL,
            watchouts_json JSON NOT NULL,
            next_focus_json JSON NOT NULL,
            confidence VARCHAR(64) NULL,
            usage_json JSON NULL,
            snapshot_json JSON NOT NULL,
            generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_ai_reports_generated (generated_at),
            KEY idx_ai_reports_snapshot_model_time (snapshot_hash, model, generated_at)
        )
        """
    )


def query_sleep_daily_rows(cur, *, start: Optional[str] = None, end: Optional[str] = None) -> list[dict]:
    date_conditions, date_params = build_date_filters("local_date", start, end)
    extra_where = (" AND " + " AND ".join(date_conditions)) if date_conditions else ""
    cur.execute(
        f"""
        SELECT local_date AS date,
               SUM(TIMESTAMPDIFF(SECOND, start_at, end_at)) / 3600.0 AS total_hours,
               MIN(start_at) AS sleep_start,
               MAX(end_at) AS sleep_end
        FROM health_records
        WHERE type = %s
          AND value_text NOT IN (%s, %s)
          AND (
              value_text <> %s
              OR local_date NOT IN (
                  SELECT DISTINCT local_date FROM health_records
                  WHERE type = %s
                    AND value_text IN (%s, %s, %s)
                    {extra_where}
              )
          )
          {extra_where}
        GROUP BY local_date
        ORDER BY local_date
        """,
        [
            "HKCategoryTypeIdentifierSleepAnalysis",
            "HKCategoryValueSleepAnalysisInBed",
            "HKCategoryValueSleepAnalysisAwake",
            "HKCategoryValueSleepAnalysisAsleepUnspecified",
            "HKCategoryTypeIdentifierSleepAnalysis",
            "HKCategoryValueSleepAnalysisAsleepCore",
            "HKCategoryValueSleepAnalysisAsleepDeep",
            "HKCategoryValueSleepAnalysisAsleepREM",
            *date_params,
            *date_params,
        ],
    )
    return rows_to_list(cur.fetchall())


def query_daily_heart_rate_rows(cur, *, start: Optional[str] = None, end: Optional[str] = None) -> list[dict]:
    conditions = ["type = %s", "value_num IS NOT NULL"]
    params: list = ["HKQuantityTypeIdentifierHeartRate"]
    date_conditions, date_params = build_date_filters("local_date", start, end)
    conditions.extend(date_conditions)
    params.extend(date_params)
    cur.execute(
        f"""
        SELECT local_date AS date,
               AVG(value_num) AS avg_bpm,
               MIN(value_num) AS min_bpm,
               MAX(value_num) AS max_bpm,
               COUNT(*) AS count
        FROM health_records
        WHERE {" AND ".join(conditions)}
        GROUP BY local_date
        ORDER BY local_date
        """,
        params,
    )
    return rows_to_list(cur.fetchall())


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


class DashboardAIRequest(BaseModel):
    model: str | None = None
    force_refresh: bool = False


@app.on_event("startup")
def startup() -> None:
    with get_db() as db, db.cursor() as cur:
        ensure_import_status_schema(cur)
        ensure_ingest_tables(cur)
        ensure_dashboard_ai_schema(cur)


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
            business_deduped_count = 0
            if rows:
                # Business dedup: filter out records already present by content identity
                # (type, start_at, end_at, source_name). Process in chunks to avoid
                # query size limits and slow IN() scans on large tables.
                DEDUP_CHUNK = 50
                existing_keys: set = set()
                check_keys = [(row[1], row[9], row[10], row[2]) for row in rows]
                for i in range(0, len(check_keys), DEDUP_CHUNK):
                    chunk = check_keys[i : i + DEDUP_CHUNK]
                    placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(chunk))
                    flat_params = [v for key in chunk for v in key]
                    cur.execute(
                        f"SELECT type, start_at, end_at, source_name FROM health_records "
                        f"WHERE (type, start_at, end_at, source_name) IN ({placeholders})",
                        flat_params,
                    )
                    existing_keys.update(
                        (r["type"], r["start_at"], r["end_at"], r["source_name"])
                        for r in cur.fetchall()
                    )
                filtered_rows = [
                    row for row in rows
                    if (row[1], row[9], row[10], row[2]) not in existing_keys
                ]
                business_deduped_count = len(rows) - len(filtered_rows)

                if filtered_rows:
                    cur.executemany(
                        """
                        INSERT IGNORE INTO health_records (
                            record_hash, type, source_name, source_version, device, unit,
                            value_text, value_num, creation_at, start_at, end_at, local_date, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        filtered_rows,
                    )
                    inserted_count = max(cur.rowcount, 0)

            deduplicated_count = accepted_count - inserted_count - business_deduped_count

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


@app.get("/api/dashboard/home")
def get_dashboard_home():
    now = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0)
    today = now.date()
    fourteen_days_ago = (today - timedelta(days=13)).isoformat()
    thirty_days_ago = (today - timedelta(days=29)).isoformat()

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(value_num), 0) AS v
            FROM health_records
            WHERE type=%s AND local_date=CURDATE()
            """,
            ("HKQuantityTypeIdentifierStepCount",),
        )
        steps_today = as_int(cur.fetchone()["v"])

        cur.execute(
            """
            SELECT COALESCE(SUM(value_num), 0) AS v
            FROM health_records
            WHERE type=%s AND local_date=CURDATE()
            """,
            ("HKQuantityTypeIdentifierActiveEnergyBurned",),
        )
        active_calories_today = as_int(cur.fetchone()["v"])

        cur.execute(
            """
            SELECT AVG(value_num) AS avg, MIN(value_num) AS min, MAX(value_num) AS max, COUNT(*) AS cnt
            FROM health_records
            WHERE type=%s AND local_date=CURDATE() AND value_num IS NOT NULL
            """,
            ("HKQuantityTypeIdentifierHeartRate",),
        )
        today_hr = cur.fetchone() or {}

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
            SELECT COUNT(*) AS count,
                   ROUND(SUM(duration), 1) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            WHERE start_at >= (CURDATE() - INTERVAL 6 DAY)
            """
        )
        workouts_7d = cur.fetchone() or {}

        cur.execute(
            """
            SELECT COUNT(*) AS count,
                   ROUND(SUM(duration), 1) AS total_minutes,
                   ROUND(SUM(total_energy_burned), 0) AS total_calories
            FROM workouts
            WHERE start_at >= (CURDATE() - INTERVAL 29 DAY)
            """
        )
        workouts_30d = cur.fetchone() or {}

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

        ensure_ingest_tables(cur)
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
        devices = rows_to_list(cur.fetchall())

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
        insights.append({
            "level": "warn",
            "title": "还没有同步记录",
            "detail": "首页先聚焦近期状态，但当前服务端还没有收到 bridge 的完成同步事件。",
        })
    elif hours_since_last_sync >= 24:
        insights.append({
            "level": "warn",
            "title": "最近 24 小时没有新同步",
            "detail": f"距离上次完成同步已过去 {hours_since_last_sync} 小时，近期数据可能不是最新。",
        })

    sleep_avg_7d = mean(sleep_last_7)
    sleep_avg_prev_7d = mean(sleep_prev_7)
    if sleep_avg_7d is not None and sleep_avg_7d < 7:
        insights.append({
            "level": "notice",
            "title": "近 7 晚平均睡眠偏少",
            "detail": f"最近 7 晚平均约 {round(sleep_avg_7d, 1)} 小时，可以单独追踪晚睡和补觉波动。",
            "raw_type": "HKCategoryTypeIdentifierSleepAnalysis",
        })

    if steps_avg_7d is not None and steps_avg_prev_7d and steps_avg_7d < steps_avg_prev_7d * 0.8:
        insights.append({
            "level": "notice",
            "title": "最近一周活动量下降",
            "detail": f"近 7 天日均步数 {round(steps_avg_7d):,}，低于前一周的 {round(steps_avg_prev_7d):,}。",
            "raw_type": "HKQuantityTypeIdentifierStepCount",
        })
    elif steps_avg_7d is not None and steps_avg_7d >= 8000:
        insights.append({
            "level": "good",
            "title": "最近一周步数保持不错",
            "detail": f"近 7 天日均步数约 {round(steps_avg_7d):,}，首页可以继续把步数作为主维度展示。",
            "raw_type": "HKQuantityTypeIdentifierStepCount",
        })

    if as_int(workouts_7d.get("count")) == 0:
        insights.append({
            "level": "notice",
            "title": "最近 7 天没有运动记录",
            "detail": "可以把首页的训练卡片作为提醒入口，而不是放全历史运动总量。",
        })
    elif recent_workouts:
        last_workout = recent_workouts[0]
        insights.append({
            "level": "good",
            "title": "最近训练还在持续",
            "detail": f"最近一次训练是 {last_workout.get('date')} 的 {last_workout.get('activity_type') or '运动'}。",
        })

    return {
        "generated_at": now,
        "ai": get_ai_config(),
        "today": {
            "steps": steps_today,
            "active_calories": active_calories_today,
            "heart_rate": {
                "avg": round_or_none(today_hr.get("avg"), 1),
                "min": round_or_none(today_hr.get("min"), 1),
                "max": round_or_none(today_hr.get("max"), 1),
                "count": as_int(today_hr.get("cnt")),
            },
        },
        "steps": {
            "today": steps_today,
            "last_7_days": steps_last_7_days,
            "total_7d": steps_total_7d,
            "avg_7d": round_or_none(steps_avg_7d, 1),
            "avg_prev_7d": round_or_none(steps_avg_prev_7d, 1),
            "delta_vs_prev_7d": percent_change(steps_avg_7d, steps_avg_prev_7d),
        },
        "sleep": {
            "last_14_days": sleep_rows,
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
            "count_7d": as_int(workouts_7d.get("count")),
            "count_30d": as_int(workouts_30d.get("count")),
            "total_minutes_7d": round_or_none(workouts_7d.get("total_minutes"), 1),
            "total_minutes_30d": round_or_none(workouts_30d.get("total_minutes"), 1),
            "total_calories_30d": as_int(workouts_30d.get("total_calories")),
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


def fetch_recent_ai_report_from_db(snapshot_hash: str, model: str, *, max_age_seconds: int) -> dict[str, Any] | None:
    threshold = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0) - timedelta(seconds=max_age_seconds)
    with get_db() as db, db.cursor() as cur:
        ensure_dashboard_ai_schema(cur)
        cur.execute(
            """
            SELECT id, snapshot_hash, model, title, summary, bullets_json, watchouts_json,
                   next_focus_json, confidence, usage_json, generated_at
            FROM ai_dashboard_reports
            WHERE snapshot_hash=%s AND model=%s AND generated_at >= %s
            ORDER BY generated_at DESC, id DESC
            LIMIT 1
            """,
            (snapshot_hash, model, threshold),
        )
        row = cur.fetchone()
    return row_to_ai_report(row) if row else None


def store_ai_report(
    *,
    snapshot_hash: str,
    model: str,
    analysis: dict[str, Any],
    usage: dict[str, Any],
    snapshot_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = normalize_ai_analysis(analysis)
    with get_db() as db, db.cursor() as cur:
        ensure_dashboard_ai_schema(cur)
        cur.execute(
            """
            INSERT INTO ai_dashboard_reports (
                snapshot_hash, model, title, summary, bullets_json, watchouts_json,
                next_focus_json, confidence, usage_json, snapshot_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_hash,
                model,
                normalized["title"],
                normalized["summary"],
                json.dumps(normalized["bullets"], ensure_ascii=False),
                json.dumps(normalized["watchouts"], ensure_ascii=False),
                json.dumps(normalized["next_focus"], ensure_ascii=False),
                normalized["confidence"] or None,
                json.dumps(usage or {}, ensure_ascii=False),
                json.dumps(snapshot_payload, ensure_ascii=False, default=str),
            ),
        )
        report_id = int(cur.lastrowid)
        cur.execute(
            """
            SELECT id, snapshot_hash, model, title, summary, bullets_json, watchouts_json,
                   next_focus_json, confidence, usage_json, generated_at
            FROM ai_dashboard_reports
            WHERE id=%s
            """,
            (report_id,),
        )
        row = cur.fetchone()
    return row_to_ai_report(row)


@app.get("/api/dashboard/ai-reports")
def list_dashboard_ai_reports(limit: int = Query(6, ge=1, le=30)):
    with get_db() as db, db.cursor() as cur:
        ensure_dashboard_ai_schema(cur)
        cur.execute(
            """
            SELECT id, snapshot_hash, model, title, summary, bullets_json, watchouts_json,
                   next_focus_json, confidence, usage_json, generated_at
            FROM ai_dashboard_reports
            ORDER BY generated_at DESC, id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [row_to_ai_report(row) for row in rows]


@app.post("/api/dashboard/ai-analysis")
def get_dashboard_ai_analysis(payload: DashboardAIRequest):
    model = resolve_ai_model(payload.model)
    home = get_dashboard_home()
    cache_key = build_ai_cache_key(model, home)
    now_ts = time.time()
    cached = _dashboard_ai_cache.get(cache_key)
    if cached and not payload.force_refresh and now_ts - cached["created_at_ts"] <= AI_ANALYSIS_CACHE_TTL_SECONDS:
        return {
            "model": model,
            "cached": True,
            "generated_at": cached["generated_at"],
            "analysis": cached["analysis"],
            "usage": cached["usage"],
        }

    if not payload.force_refresh:
        db_cached = fetch_recent_ai_report_from_db(cache_key, model, max_age_seconds=AI_ANALYSIS_CACHE_TTL_SECONDS)
        if db_cached:
            _dashboard_ai_cache[cache_key] = {
                "created_at_ts": now_ts,
                "generated_at": db_cached["generated_at"],
                "analysis": db_cached["analysis"],
                "usage": db_cached["usage"],
            }
            return {
                "model": model,
                "cached": True,
                "generated_at": db_cached["generated_at"],
                "analysis": db_cached["analysis"],
                "usage": db_cached["usage"],
            }

    prompt = build_ai_dashboard_prompt(home)
    analysis, usage = request_openrouter_analysis(prompt, model)
    stored_report = store_ai_report(
        snapshot_hash=cache_key,
        model=model,
        analysis=analysis,
        usage=usage,
        snapshot_payload={
            "home": home,
        },
    )
    result = {
        "created_at_ts": now_ts,
        "generated_at": stored_report["generated_at"],
        "analysis": stored_report["analysis"],
        "usage": stored_report["usage"],
    }
    _dashboard_ai_cache[cache_key] = result
    return {
        "model": model,
        "cached": False,
        "generated_at": result["generated_at"],
        "analysis": result["analysis"],
        "usage": result["usage"],
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


@app.get("/api/records/hourly")
def get_hourly_records(
    type: str = Query(...),
    date: Optional[str] = Query(None, description="YYYY-MM-DD, defaults to today"),
    agg: Literal["sum", "avg", "max", "min", "count"] = Query("sum"),
):
    agg_sql = {
        "sum": "SUM(value_num)",
        "avg": "AVG(value_num)",
        "max": "MAX(value_num)",
        "min": "MIN(value_num)",
        "count": "COUNT(*)",
    }[agg]
    target_date = date or "CURDATE()"
    date_filter = "local_date = CURDATE()" if not date else "local_date = %s"
    conditions = ["type = %s", "value_num IS NOT NULL", date_filter]
    params: list = [type] + ([date] if date else [])

    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"""
            SELECT HOUR(start_at) AS hour, {agg_sql} AS value, COUNT(*) AS count, MIN(unit) AS unit
            FROM health_records
            WHERE {" AND ".join(conditions)}
            GROUP BY HOUR(start_at)
            ORDER BY hour
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
            return query_daily_heart_rate_rows(cur, start=start, end=end)
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
    with get_db() as db, db.cursor() as cur:
        return query_sleep_daily_rows(cur, start=start, end=end)


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


@app.get("/api/stats/today")
def get_today_stats():
    with get_db() as db, db.cursor() as cur:
        # Today's steps
        cur.execute(
            "SELECT COALESCE(SUM(value_num), 0) AS v FROM health_records WHERE type=%s AND local_date=CURDATE()",
            ("HKQuantityTypeIdentifierStepCount",),
        )
        steps = int(cur.fetchone()["v"])

        # Today's active energy
        cur.execute(
            "SELECT COALESCE(SUM(value_num), 0) AS v FROM health_records WHERE type=%s AND local_date=CURDATE()",
            ("HKQuantityTypeIdentifierActiveEnergyBurned",),
        )
        active_cal = round(cur.fetchone()["v"])

        # Today's avg heart rate
        cur.execute(
            "SELECT AVG(value_num) AS avg, MIN(value_num) AS min, MAX(value_num) AS max, COUNT(*) AS cnt "
            "FROM health_records WHERE type=%s AND local_date=CURDATE() AND value_num IS NOT NULL",
            ("HKQuantityTypeIdentifierHeartRate",),
        )
        hr = cur.fetchone()

        # Today's workouts
        cur.execute("SELECT COUNT(*) AS cnt FROM workouts WHERE local_date=CURDATE()")
        workouts = cur.fetchone()["cnt"]

        # Today's record count
        cur.execute("SELECT COUNT(*) AS cnt FROM health_records WHERE local_date=CURDATE()")
        today_records = cur.fetchone()["cnt"]

        # Today's distinct types
        cur.execute("SELECT COUNT(DISTINCT type) AS cnt FROM health_records WHERE local_date=CURDATE()")
        today_types = cur.fetchone()["cnt"]

        # Last sync time
        cur.execute(
            "SELECT MAX(received_at) AS last_sync_at, COUNT(*) AS sync_count, "
            "SUM(accepted_count) AS total_accepted "
            "FROM ingest_events WHERE DATE(received_at)=CURDATE() AND status='completed'"
        )
        sync_row = cur.fetchone()

        # Last sync overall
        cur.execute("SELECT MAX(received_at) AS last_sync_at FROM ingest_events WHERE status='completed'")
        last_sync = cur.fetchone()

    return {
        "steps": steps,
        "active_calories": active_cal,
        "heart_rate": {
            "avg": round(hr["avg"], 1) if hr["avg"] else None,
            "min": round(hr["min"], 1) if hr["min"] else None,
            "max": round(hr["max"], 1) if hr["max"] else None,
            "count": hr["cnt"],
        },
        "workouts": workouts,
        "today_records": today_records,
        "today_types": today_types,
        "today_sync_count": sync_row["sync_count"] or 0,
        "today_sync_accepted": int(sync_row["total_accepted"] or 0),
        "today_last_sync_at": sync_row["last_sync_at"],
        "last_sync_at": last_sync["last_sync_at"] if last_sync else None,
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
