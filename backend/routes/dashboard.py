from __future__ import annotations

from fastapi import APIRouter, Query

from backend.models import DashboardAIRequest
from backend.responses import api_response, list_response
from backend.services.ai_service import analyze_dashboard, list_recent_ai_reports, resolve_ai_model
from backend.services.dashboard_service import get_dashboard_home_payload

router = APIRouter(prefix="/api/dashboard")


@router.get("/home")
def get_dashboard_home():
    return api_response(get_dashboard_home_payload())


@router.get("/ai-reports")
def list_dashboard_ai_reports(limit: int = Query(6, ge=1, le=30)):
    reports = list_recent_ai_reports(limit)
    return list_response(reports, limit=limit)


@router.post("/ai-analysis")
def get_dashboard_ai_analysis(payload: DashboardAIRequest):
    model = resolve_ai_model(payload.model)
    home = get_dashboard_home_payload(force_refresh=payload.force_refresh)
    return api_response(
        analyze_dashboard(home, model=model, force_refresh=payload.force_refresh),
        model=model,
        force_refresh=payload.force_refresh,
    )
