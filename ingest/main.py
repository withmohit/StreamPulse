import uuid
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import os
from dotenv import load_dotenv

from ingest.schemas import EventPayload, IngestResponse, HealthResponse
from ingest.producer import init_producer, shutdown_producer, send_event

load_dotenv()
log = structlog.get_logger()

# ── Rate limiter ───────────────────────────────────────────────────────────────
# Per IP for now — in production you'd rate limit per tenant_id
limiter = Limiter(key_func=get_remote_address)

# ── Lifespan — init/teardown ───────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    log.info("ingest_service_starting")
    try:
        init_producer()
        log.info("ingest_service_ready")
    except Exception as e:
        log.error("startup_failed", error=str(e))
        raise
    yield
    # Shutdown
    shutdown_producer()
    log.info("ingest_service_stopped")


app = FastAPI(
    title="StreamPulse Ingest API",
    description="Event ingestion endpoint for the StreamPulse pipeline",
    version="0.1.0",
    lifespan=lifespan
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    """Liveness check — is the service and Kafka connection alive?"""
    from ingest.producer import _producer
    kafka_status = "connected" if _producer else "disconnected"
    return HealthResponse(
        status="ok" if _producer else "degraded",
        kafka=kafka_status,
        timestamp=__import__('datetime').datetime.utcnow().isoformat()
    )


@app.post("/ingest", response_model=IngestResponse)
@limiter.limit("200/minute")   # 200 events/min per IP
async def ingest_event(request: Request, event: EventPayload):
    """
    Main ingest endpoint.
    Valid events   → events topic (keyed by tenant_id)
    Invalid events → caught by Pydantic before reaching here → 422
    """
    topic = os.getenv("KAFKA_TOPIC_EVENTS", "events")

    try:
        event_id = send_event(
            topic=topic,
            event=event.model_dump(mode="json"),
            key=event.tenant_id   # partition by tenant
        )
        log.info("event_accepted",
            event_id=event_id,
            event_type=event.event_type,
            tenant_id=event.tenant_id
        )
        return IngestResponse(
            event_id=event_id,
            status="accepted",
            message="Event queued for processing"
        )
    except Exception as e:
        log.error("event_routing_failed", error=str(e))
        raise HTTPException(status_code=503, detail="Pipeline unavailable")


@app.post("/ingest/raw")
@limiter.limit("200/minute")
async def ingest_raw(request: Request):
    """
    Fallback endpoint — accepts anything, sends malformed to DLQ.
    Use this to test your DLQ pipeline explicitly.
    """
    dlq_topic = os.getenv("KAFKA_TOPIC_DLQ", "events.dlq")
    body = await request.json()

    try:
        # Try to validate
        event = EventPayload(**body)
        topic = os.getenv("KAFKA_TOPIC_EVENTS", "events")
        event_id = send_event(topic, event.model_dump(mode="json"), key=event.tenant_id)
        return IngestResponse(event_id=event_id, status="accepted", message="Valid event queued")

    except Exception as validation_error:
        # Invalid — route to DLQ with reason
        dlq_payload = {
            "raw": body,
            "failure_reason": str(validation_error),
            "source": "ingest_raw"
        }
        event_id = send_event(dlq_topic, dlq_payload, key=None)
        log.warning("event_sent_to_dlq",
            event_id=event_id,
            reason=str(validation_error)[:200]
        )
        return IngestResponse(
            event_id=event_id,
            status="rejected",
            message=f"Validation failed: {str(validation_error)[:100]}"
        )