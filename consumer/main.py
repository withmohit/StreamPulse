import json
import os
import signal
import time

import structlog
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from consumer.aggregator import record_event
from consumer.cache import increment_event_counters
from consumer.db import insert_event
from consumer.lag import LAG_INTERVAL_SECONDS, calculate_and_store_lag

load_dotenv()
log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "events")
GROUP_ID = "streampulse-consumer"

_running = True


def _handle_signal(sig, frame):
    global _running
    log.info("shutdown_signal_received", signal=sig)
    _running = False


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,      # commit manually after a successful DB write
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,      # StopIteration every 1 s so _running is re-checked
    )


def _process_message(msg) -> None:
    """Persist one Kafka message, update Redis counters, and record in aggregator."""
    event = msg.value
    event_type = event.get("event_type", "unknown")
    tenant_id = event.get("tenant_id", "unknown")

    insert_event(event)
    increment_event_counters(event_type, tenant_id)
    record_event(event)                            # feeds the 60-second window

    log.info(
        "event_processed",
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        event_type=event_type,
        tenant_id=tenant_id,
    )


def main():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("consumer_starting", topic=TOPIC, group_id=GROUP_ID)
    consumer = _build_consumer()

    # Compute lag once on start-up so Redis is populated immediately
    calculate_and_store_lag()
    _last_lag_check = time.monotonic()

    try:
        while _running:
            # consumer_timeout_ms causes StopIteration when the poll window is
            # empty, so the for-loop exits and we loop back to check _running
            # and the lag timer.
            for msg in consumer:
                if not _running:
                    break
                try:
                    _process_message(msg)
                    # Commit the offset *after* the DB write succeeds.
                    # ON CONFLICT DO NOTHING in insert_event makes retries safe.
                    consumer.commit()
                except KafkaError as e:
                    log.error(
                        "kafka_error",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error=str(e),
                    )
                    # Do not commit — message will be retried on restart
                except Exception as e:
                    log.error(
                        "message_processing_failed",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error=str(e),
                    )
                    # Do not commit — allow re-delivery

            # Periodic lag calculation (runs even during quiet periods)
            now = time.monotonic()
            if now - _last_lag_check >= LAG_INTERVAL_SECONDS:
                calculate_and_store_lag()
                _last_lag_check = now
    finally:
        consumer.close()
        log.info("consumer_stopped")


if __name__ == "__main__":
    main()
