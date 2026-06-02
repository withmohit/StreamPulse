from pydantic import BaseModel, field_validator, model_validator
from typing import Literal, Dict, Any, Optional
from datetime import datetime, timezone

class EventPayload(BaseModel):
    event_type: Literal["pageview", "purchase", "click", "error"]
    tenant_id: str
    timestamp: datetime
    data: Dict[str, Any]

    @field_validator("tenant_id")
    @classmethod
    def tenant_not_empty(cls, v):
        if not v.strip():
            raise ValueError("tenant_id cannot be empty or whitespace")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_future(cls, v):
        # events more than 5 min in future are suspicious
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        diff = (v - now).total_seconds()
        if diff > 300:
            raise ValueError(f"timestamp is {diff:.0f}s in the future")
        return v

    @field_validator("data")
    @classmethod
    def data_not_empty(cls, v):
        if not v:
            raise ValueError("data payload cannot be empty")
        return v


class IngestResponse(BaseModel):
    event_id: str
    status: str        # "accepted" | "rejected"
    message: str


class HealthResponse(BaseModel):
    status: str        # "ok" | "degraded"
    kafka: str         # "connected" | "disconnected"
    timestamp: str