from __future__ import annotations

from fastapi import APIRouter

from .dashboard import router as dashboard_router
from .ingest import router as ingest_router
from .records import router as records_router
from .root import router as root_router
from .stats import router as stats_router
from .sync import router as sync_router
from .workouts import router as workouts_router


api_router = APIRouter()
for router in (
    root_router,
    ingest_router,
    sync_router,
    dashboard_router,
    records_router,
    workouts_router,
    stats_router,
):
    api_router.include_router(router)
