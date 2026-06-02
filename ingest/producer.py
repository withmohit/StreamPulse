import json
import uuid
import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import os
from dotenv import load_dotenv

load_dotenv()
log = structlog.get_logger()

_producer: KafkaProducer | None = None

def get_producer() -> KafkaProducer:
    """Return the singleton producer. Raises if not initialized."""
    if _producer is None:
        raise RuntimeError("Kafka producer not initialized — call init_producer() first")
    return _producer


def init_producer() -> KafkaProducer:
    """Create and return the Kafka producer. Call once on app startup."""
    global _producer
    try:
        _producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",           # wait for all replicas to ack — durability
            retries=3,            # retry transient failures
            linger_ms=5,          # batch events for 5ms — throughput optimization
            compression_type="gzip",
        )
        log.info("kafka_producer_initialized",
                 servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        return _producer
    except NoBrokersAvailable as e:
        log.error("kafka_no_brokers", error=str(e))
        raise


def shutdown_producer():
    """Flush and close the producer. Call on app shutdown."""
    global _producer
    if _producer:
        _producer.flush()     # wait for all pending messages to be sent
        _producer.close()
        log.info("kafka_producer_closed")
        _producer = None


def send_event(topic: str, event: dict, key: str | None = None):
    """
    Send one event to a Kafka topic.
    key should be tenant_id — ensures same tenant goes to same partition.
    """
    producer = get_producer()
    event_id = str(uuid.uuid4())
    event["_meta"] = {"event_id": event_id}

    future = producer.send(
        topic,
        value=event,
        key=key
    )

    # Block briefly to catch immediate send errors
    # In production you'd handle this async with callbacks
    try:
        record_metadata = future.get(timeout=5)
        log.info("event_sent",
            event_id=event_id,
            topic=record_metadata.topic,
            partition=record_metadata.partition,
            offset=record_metadata.offset,
            key=key
        )
        return event_id
    except KafkaError as e:
        log.error("event_send_failed",
            event_id=event_id,
            topic=topic,
            error=str(e)
        )
        raise