"""
Apple Health Personal API for MySQL 8.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.config import FRONTEND_DIR, allowed_origins
from backend.routes import api_router
from backend.services.schema_service import ensure_runtime_schema


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_runtime_schema()
    yield


app = FastAPI(title="Apple Health Personal API", version="2.1", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIR.exists():
    app.mount("/dashboard", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="dashboard")

app.include_router(api_router)
