"""
StreamPulse Metrics Service
---------------------------
FastAPI application exposing read-only pipeline health and throughput data.

All live counters are read from Redis (written by the consumer).
Windowed aggregations are read from the pipeline_metrics Postgres table.

Endpoints
---------
GET /health    — liveness check for all dependencies
GET /throughput — total events processed + latest 60-second window breakdown
GET /lag       — per-partition consumer lag (updated every 30 s by consumer)
GET /errors    — DLQ totals + recent error reasons from the DB
GET /summary   — combined one-page overview
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis as redis_lib
import structlog
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

load_dotenv()
log = structlog.get_logger()

# ------------------------------------------------------------------ #
# Dependency singletons                                                #
# ------------------------------------------------------------------ #

_POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://streampulse:streampulse@localhost:5432/streampulse",
)
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_engine = create_engine(_POSTGRES_URL, pool_pre_ping=True)
_redis: redis_lib.Redis = redis_lib.Redis.from_url(_REDIS_URL, decode_responses=True)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _redis_int(key: str) -> int:
    val = _redis.get(key)
    return int(val) if val else 0


def _redis_lag() -> dict[str, int]:
    raw = _redis.hgetall("consumer:lag")
    return {k: int(v) for k, v in raw.items()}


def _latest_window() -> dict | None:
    """Return the most recent row from pipeline_metrics, or None."""
    with Session(_engine) as session:
        row = session.execute(
            text(
                """
                SELECT window_start, window_end, total_events,
                       pageview_count, purchase_count, click_count, error_count
                FROM   pipeline_metrics
                ORDER  BY window_end DESC
                LIMIT  1
                """
            )
        ).fetchone()
    if row is None:
        return None
    return {
        "window_start": row.window_start.isoformat(),
        "window_end": row.window_end.isoformat(),
        "total": row.total_events,
        "by_type": {
            "pageview": row.pageview_count,
            "purchase": row.purchase_count,
            "click": row.click_count,
            "error": row.error_count,
        },
    }


def _recent_dlq_reasons(limit: int = 10) -> list[dict]:
    """Return the most recent DLQ error reasons from the dead_letter_queue table."""
    with Session(_engine) as session:
        rows = session.execute(
            text(
                """
                SELECT error_reason, COUNT(*) AS cnt
                FROM   dead_letter_queue
                GROUP  BY error_reason
                ORDER  BY cnt DESC
                LIMIT  :limit
                """
            ),
            {"limit": limit},
        ).fetchall()
    return [{"error_reason": r.error_reason, "count": r.cnt} for r in rows]


# ------------------------------------------------------------------ #
# App                                                                  #
# ------------------------------------------------------------------ #

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("metrics_service_starting")
    yield
    log.info("metrics_service_stopped")


app = FastAPI(title="StreamPulse Metrics", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],   # Vite dev server
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------ #
# Routes                                                               #
# ------------------------------------------------------------------ #

@app.get("/health")
def health():
    """Liveness check: verifies Postgres and Redis connectivity."""
    pg_status = "connected"
    redis_status = "connected"

    try:
        with Session(_engine) as session:
            session.execute(text("SELECT 1"))
    except Exception as e:
        log.error("health_postgres_fail", error=str(e))
        pg_status = "disconnected"

    try:
        _redis.ping()
    except Exception as e:
        log.error("health_redis_fail", error=str(e))
        redis_status = "disconnected"

    overall = "ok" if pg_status == "connected" and redis_status == "connected" else "degraded"
    return {
        "status": overall,
        "postgres": pg_status,
        "redis": redis_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/throughput")
def throughput():
    """Total events processed and the latest 60-second window breakdown."""
    total = _redis_int("events:total")
    window = _latest_window()

    # Scan per-type counters from Redis
    type_keys = _redis.keys("events:type:*")
    by_type = {}
    if type_keys:
        vals = _redis.mget(type_keys)
        for key, val in zip(type_keys, vals):
            event_type = key.split(":")[-1]
            by_type[event_type] = int(val) if val else 0

    return {
        "total_events": total,
        "by_type_cumulative": by_type,
        "last_window": window,
    }


@app.get("/lag")
def lag():
    """Per-partition consumer lag for the events topic (refreshed every 30 s)."""
    partition_lag = _redis_lag()
    total_lag = sum(partition_lag.values())
    return {
        "topic": os.getenv("KAFKA_TOPIC_EVENTS", "events"),
        "group_id": "streampulse-consumer",
        "partitions": partition_lag,
        "total_lag": total_lag,
    }


@app.get("/errors")
def errors():
    """DLQ volume and breakdown of error reasons stored in dead_letter_queue."""
    dlq_total = _redis_int("events:dlq:total")

    try:
        reasons = _recent_dlq_reasons()
    except Exception as e:
        log.error("errors_db_fail", error=str(e))
        raise HTTPException(status_code=503, detail="Database unavailable")

    return {
        "dlq_total": dlq_total,
        "top_error_reasons": reasons,
    }


@app.get("/summary")
def summary():
    """Combined snapshot: throughput, lag, and error rate."""
    total = _redis_int("events:total")
    dlq_total = _redis_int("events:dlq:total")
    partition_lag = _redis_lag()
    total_lag = sum(partition_lag.values())
    window = _latest_window()

    error_rate = round(dlq_total / total, 6) if total > 0 else 0.0

    return {
        "total_events": total,
        "dlq_total": dlq_total,
        "error_rate": error_rate,
        "total_lag": total_lag,
        "lag_by_partition": partition_lag,
        "last_window": window,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
