from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


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
