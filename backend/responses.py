from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.config import LOCAL_TIMEZONE


def _generated_at() -> str:
    return datetime.now(LOCAL_TIMEZONE).replace(microsecond=0).isoformat()


def api_response(data: Any, **meta: Any) -> dict[str, Any]:
    return {
        "data": data,
        "meta": {
            "generated_at": _generated_at(),
            **meta,
        },
    }


def list_response(data: list[Any], *, total: int | None = None, **meta: Any) -> dict[str, Any]:
    resolved_total = total if total is not None else len(data)
    return api_response(data, total=resolved_total, **meta)
