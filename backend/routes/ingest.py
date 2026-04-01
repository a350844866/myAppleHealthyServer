from __future__ import annotations

from fastapi import APIRouter, Header

from backend.models import IngestPayload
from backend.responses import api_response
from backend.services.ingest_service import ingest_samples

router = APIRouter()


@router.post("/ingest")
def ingest_endpoint(payload: IngestPayload, authorization: str | None = Header(None)):
    return api_response(ingest_samples(payload, authorization))
