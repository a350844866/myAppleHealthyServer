from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse

from backend.config import FRONTEND_DIR
from backend.database import get_db
from backend.responses import api_response
from backend.services.import_service import get_import_status_payload

router = APIRouter()


@router.get("/")
def root():
    if FRONTEND_DIR.exists():
        return RedirectResponse(url="/dashboard/")
    return api_response({"message": "Apple Health API is running"})


@router.get("/dashboard.html")
def dashboard_html():
    index_file = FRONTEND_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(404, "前端页面不存在")
    return FileResponse(index_file)


@router.get("/api/profile")
def get_profile():
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT * FROM profile WHERE id=1")
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "尚未导入数据")
        return api_response(row)


@router.get("/api/import-status")
def get_import_status():
    return api_response(get_import_status_payload())
