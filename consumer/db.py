import os
from datetime import datetime, timezone

import structlog
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

load_dotenv()
log = structlog.get_logger()

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = os.getenv(
            "POSTGRES_URL",
            "postgresql://streampulse:streampulse@localhost:5432/streampulse",
        )
        _engine = create_engine(url, pool_pre_ping=True)
        log.info("db_engine_created", url=url)
    return _engine


def _extract_value(event: dict) -> float:
    """Pull a meaningful numeric value out of an event's data payload."""
    event_type = event.get("event_type", "")
    data = event.get("data", {})
    if event_type == "purchase":
        return float(data.get("amount", 0.0))
    if event_type == "pageview":
        return float(data.get("load_time_ms", 0.0))
    # click / error — count as 1
    return 1.0


def insert_event(event: dict) -> None:
    """Insert a validated event into the events table."""
    meta = event.get("_meta", {})
    event_id = meta.get("event_id", "")
    source = event.get("tenant_id", "")
    event_type = event.get("event_type", "")
    timestamp = event.get("timestamp")
    value = _extract_value(event)

    with Session(get_engine()) as session:
        session.execute(
            text(
                """
                INSERT INTO events (id, source, type, value, timestamp)
                VALUES (:id, :source, :type, :value, :timestamp)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {
                "id": event_id,
                "source": source,
                "type": event_type,
                "value": value,
                "timestamp": timestamp,
            },
        )
        session.commit()

    log.info("event_inserted", event_id=event_id, type=event_type, source=source)


def insert_dead_letter(
    raw_value: str,
    error_reason: str,
    topic: str,
    partition: int,
    offset: int,
) -> None:
    """Insert a failed/malformed message into the dead_letter_queue table."""
    with Session(get_engine()) as session:
        session.execute(
            text(
                """
                INSERT INTO dead_letter_queue
                    (raw_value, error_reason, topic, partition, "offset", received_at)
                VALUES
                    (:raw_value, :error_reason, :topic, :partition, :offset, :received_at)
                """
            ),
            {
                "raw_value": raw_value,
                "error_reason": error_reason,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "received_at": datetime.now(timezone.utc),
            },
        )
        session.commit()

    log.info(
        "dead_letter_inserted",
        topic=topic,
        partition=partition,
        offset=offset,
        error_reason=error_reason,
    )
