"""
Consumer Lag Calculator
-----------------------
Computes per-partition consumer lag for the main events topic by comparing
the broker's log-end-offset with the consumer group's committed offset.

Results are pushed to Redis under the hash  consumer:lag
so the metrics service can read them without connecting to Kafka directly.

Called periodically from consumer/main.py (every LAG_INTERVAL_SECONDS seconds).
"""

import os

import structlog
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

from consumer.cache import store_lag

load_dotenv()
log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "events")
GROUP_ID = "streampulse-consumer"

LAG_INTERVAL_SECONDS = 30   # how often the main loop should call this


def calculate_and_store_lag() -> dict[int, int]:
    """
    Fetch end-offsets from the broker and committed offsets for GROUP_ID,
    compute lag per partition, store in Redis, and return the result.

    Returns an empty dict if Kafka is unreachable.
    """
    temp_consumer: KafkaConsumer | None = None
    admin: KafkaAdminClient | None = None

    try:
        # ---- 1. Discover partitions & end-offsets -------------------------
        # Use a throwaway consumer with no group_id so we never join the group.
        temp_consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        partition_ids = temp_consumer.partitions_for_topic(TOPIC) or set()
        tps = [TopicPartition(TOPIC, p) for p in sorted(partition_ids)]
        end_offsets: dict[TopicPartition, int] = temp_consumer.end_offsets(tps)

        # ---- 2. Committed offsets for the consumer group ------------------
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        committed_raw = admin.list_consumer_group_offsets(GROUP_ID)
        # committed_raw: {TopicPartition -> OffsetAndMetadata}

        # ---- 3. Compute lag -----------------------------------------------
        lag: dict[int, int] = {}
        for tp in tps:
            end = end_offsets.get(tp, 0)
            meta = committed_raw.get(tp)
            committed = meta.offset if meta is not None else 0
            lag[tp.partition] = max(0, end - committed)

        store_lag(lag)
        log.info("lag_calculated", topic=TOPIC, group_id=GROUP_ID, lag=lag)
        return lag

    except KafkaError as e:
        log.error("lag_kafka_error", error=str(e))
        return {}
    except Exception as e:
        log.error("lag_calculation_failed", error=str(e))
        return {}
    finally:
        if temp_consumer:
            temp_consumer.close()
        if admin:
            admin.close()
