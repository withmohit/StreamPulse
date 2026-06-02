import os

import redis
import structlog
from dotenv import load_dotenv

load_dotenv()
log = structlog.get_logger()

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        url = os.getenv("REDIS_URL", "redis://localhost:6379")
        _client = redis.Redis.from_url(url, decode_responses=True)
        log.info("redis_client_created", url=url)
    return _client


def increment_event_counters(event_type: str, tenant_id: str) -> None:
    """Increment per-type and per-tenant counters after a successful event write."""
    client = get_client()
    pipe = client.pipeline()
    pipe.incr("events:total")
    pipe.incr(f"events:type:{event_type}")
    pipe.incr(f"events:tenant:{tenant_id}")
    pipe.execute()
    log.debug(
        "event_counters_incremented",
        event_type=event_type,
        tenant_id=tenant_id,
    )


def increment_dlq_counter(topic: str) -> None:
    """Increment DLQ counters after storing a dead-letter message."""
    client = get_client()
    pipe = client.pipeline()
    pipe.incr("events:dlq:total")
    pipe.incr(f"events:dlq:topic:{topic}")
    pipe.execute()
    log.debug("dlq_counter_incremented", topic=topic)


def get_counters() -> dict:
    """Return a snapshot of the top-level event counters stored in Redis."""
    client = get_client()
    total = client.get("events:total") or 0
    dlq_total = client.get("events:dlq:total") or 0
    return {
        "total": int(total),
        "dlq_total": int(dlq_total),
    }
