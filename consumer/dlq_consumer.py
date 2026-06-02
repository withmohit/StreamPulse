"""
DLQ Consumer
------------
Reads from the dead-letter-queue Kafka topic (events.dlq) and persists every
message — including malformed ones — into the dead_letter_queue Postgres table.

Run separately from the main consumer:
    python -m consumer.dlq_consumer
"""

import json
import os
import signal

import structlog
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from consumer.cache import increment_dlq_counter
from consumer.db import insert_dead_letter

load_dotenv()
log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DLQ_TOPIC = os.getenv("KAFKA_TOPIC_DLQ", "events.dlq")
GROUP_ID = "streampulse-dlq-consumer"

_running = True


def _handle_signal(sig, frame):
    global _running
    log.info("dlq_shutdown_signal_received", signal=sig)
    _running = False


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,      # manual commit after successful DB write
        # Keep raw bytes — DLQ messages may be malformed JSON we still want to store
        value_deserializer=lambda b: b,
        consumer_timeout_ms=1000,
    )


def _extract_error_reason(raw_str: str) -> str:
    """Try to read an _error field; fall back to a descriptive label."""
    try:
        payload = json.loads(raw_str)
        return str(payload.get("_error", "unspecified"))
    except (json.JSONDecodeError, ValueError):
        return "json_decode_error"


def main():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("dlq_consumer_starting", topic=DLQ_TOPIC, group_id=GROUP_ID)
    consumer = _build_consumer()

    try:
        while _running:
            for msg in consumer:
                if not _running:
                    break
                try:
                    raw_str = msg.value.decode("utf-8", errors="replace")
                    error_reason = _extract_error_reason(raw_str)

                    insert_dead_letter(
                        raw_value=raw_str,
                        error_reason=error_reason,
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                    )
                    increment_dlq_counter(msg.topic)

                    # Commit only after the row is safely in Postgres
                    consumer.commit()

                    log.info(
                        "dlq_message_stored",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error_reason=error_reason,
                    )
                except KafkaError as e:
                    log.error(
                        "dlq_kafka_error",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error=str(e),
                    )
                except Exception as e:
                    log.error(
                        "dlq_processing_failed",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                        error=str(e),
                    )
    finally:
        consumer.close()
        log.info("dlq_consumer_stopped")


if __name__ == "__main__":
    main()
