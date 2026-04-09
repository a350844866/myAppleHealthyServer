from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from fastapi import HTTPException

from backend.config import (
    AI_ANALYSIS_CACHE_TTL_SECONDS,
    LOCAL_TIMEZONE,
    OPENROUTER_ALLOWED_MODELS,
    OPENROUTER_API_URL,
    OPENROUTER_DEFAULT_MODEL,
)
from backend.database import get_db
from backend.utils import deserialize_json_field

from backend.cache import TTLCache as _TTLCache

_dashboard_ai_cache = _TTLCache(default_ttl_seconds=AI_ANALYSIS_CACHE_TTL_SECONDS)


def get_ai_config() -> dict[str, Any]:
    import os

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
    import os

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
    for _ in range(2):
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
    encoded = json.dumps({"model": model, "home": home}, ensure_ascii=False, sort_keys=True, default=str)
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


def fetch_recent_ai_report_from_db(snapshot_hash: str, model: str, *, max_age_seconds: int) -> dict[str, Any] | None:
    threshold = datetime.now(LOCAL_TIMEZONE).replace(tzinfo=None, microsecond=0) - timedelta(seconds=max_age_seconds)
    with get_db() as db, db.cursor() as cur:
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


def fetch_latest_ai_report_from_db(model: str) -> dict[str, Any] | None:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            SELECT id, snapshot_hash, model, title, summary, bullets_json, watchouts_json,
                   next_focus_json, confidence, usage_json, generated_at
            FROM ai_dashboard_reports
            WHERE model=%s
            ORDER BY generated_at DESC, id DESC
            LIMIT 1
            """,
            (model,),
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


def list_recent_ai_reports(limit: int) -> list[dict]:
    with get_db() as db, db.cursor() as cur:
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


def analyze_dashboard(home: dict[str, Any], *, model: str, force_refresh: bool) -> dict[str, Any]:
    cache_key = build_ai_cache_key(model, home)
    now_ts = time.time()
    cached = None if force_refresh else _dashboard_ai_cache.get(cache_key)
    if cached is not None:
        return {
            "model": model,
            "cached": True,
            "generated_at": cached["generated_at"],
            "analysis": cached["analysis"],
            "usage": cached["usage"],
        }

    if not force_refresh:
        db_cached = fetch_recent_ai_report_from_db(cache_key, model, max_age_seconds=AI_ANALYSIS_CACHE_TTL_SECONDS)
        if db_cached:
            _dashboard_ai_cache.set(cache_key, {
                "generated_at": db_cached["generated_at"],
                "analysis": db_cached["analysis"],
                "usage": db_cached["usage"],
            })
            return {
                "model": model,
                "cached": True,
                "generated_at": db_cached["generated_at"],
                "analysis": db_cached["analysis"],
                "usage": db_cached["usage"],
            }

    prompt = build_ai_dashboard_prompt(home)
    try:
        analysis, usage = request_openrouter_analysis(prompt, model)
    except HTTPException as exc:
        fallback_report = fetch_latest_ai_report_from_db(model)
        if fallback_report:
            return {
                "model": model,
                "cached": True,
                "degraded": True,
                "fallback_reason": str(exc.detail),
                "generated_at": fallback_report["generated_at"],
                "analysis": fallback_report["analysis"],
                "usage": fallback_report["usage"],
            }
        raise
    stored_report = store_ai_report(
        snapshot_hash=cache_key,
        model=model,
        analysis=analysis,
        usage=usage,
        snapshot_payload={"home": home},
    )
    result = {
        "generated_at": stored_report["generated_at"],
        "analysis": stored_report["analysis"],
        "usage": stored_report["usage"],
    }
    _dashboard_ai_cache.set(cache_key, result)
    return {
        "model": model,
        "cached": False,
        "degraded": False,
        "generated_at": result["generated_at"],
        "analysis": result["analysis"],
        "usage": result["usage"],
    }
