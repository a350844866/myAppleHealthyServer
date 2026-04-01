from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
LOCAL_TIMEZONE = ZoneInfo(os.getenv("HEALTH_LOCAL_TZ", "Asia/Shanghai"))
IMPORT_STALE_SECONDS = int(os.getenv("IMPORT_STALE_SECONDS", "300"))
IMPORT_RATE_WINDOW_MINUTES = int(os.getenv("IMPORT_RATE_WINDOW_MINUTES", "10"))
XML_PATH = BASE_DIR / "apple_health_export" / "导出.xml"

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
SUMMARY_STALE_SECONDS = int(os.getenv("SUMMARY_STALE_SECONDS", "300"))

DB_POOL_MIN_CACHED = int(os.getenv("HEALTH_DB_POOL_MIN", "2"))
DB_POOL_MAX_CACHED = int(os.getenv("HEALTH_DB_POOL_MAX_CACHED", "10"))
DB_POOL_MAX_CONNECTIONS = int(os.getenv("HEALTH_DB_POOL_MAX_CONNECTIONS", "10"))


def allowed_origins() -> list[str]:
    raw = os.getenv(
        "HEALTH_ALLOWED_ORIGINS",
        "http://localhost:18000,http://127.0.0.1:18000",
    )
    return [origin.strip() for origin in raw.split(",") if origin.strip()]
