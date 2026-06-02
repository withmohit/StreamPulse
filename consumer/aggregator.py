"""
Event Aggregator
----------------
Maintains an in-memory 60-second tumbling window over processed events.
A background daemon thread flushes the window to the pipeline_metrics Postgres
table at the end of each period, even if no events arrived.

Usage (called from consumer/main.py):
    from consumer.aggregator import record_event
    record_event(event)
"""

import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from consumer.db import get_engine

log = structlog.get_logger()

WINDOW_SECONDS = 60


class _EventAggregator:
    def __init__(self, window_seconds: int = WINDOW_SECONDS) -> None:
        self._window_seconds = window_seconds
        self._lock = threading.Lock()
        self._reset()

        # Daemon thread: dies automatically when the main process exits
        t = threading.Thread(target=self._flush_loop, daemon=True, name="aggregator-flush")
        t.start()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def record_event(self, event: dict) -> None:
        """Add one event to the current window. Thread-safe."""
        event_type = event.get("event_type", "unknown")
        with self._lock:
            self._type_counts[event_type] += 1
            self._total += 1

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _reset(self) -> None:
        """Start a fresh window. Must be called while holding self._lock."""
        self._window_start: datetime = datetime.now(timezone.utc)
        self._type_counts: dict[str, int] = defaultdict(int)
        self._total: int = 0

    def _flush_loop(self) -> None:
        while True:
            time.sleep(self._window_seconds)
            try:
                self._flush()
            except Exception as e:
                log.error("aggregator_flush_error", error=str(e))

    def _flush(self) -> None:
        """Snapshot the current window, reset it, and write to Postgres."""
        with self._lock:
            window_start = self._window_start
            window_end = datetime.now(timezone.utc)
            type_counts = dict(self._type_counts)
            total = self._total
            self._reset()

        if total == 0:
            log.debug("aggregator_window_empty", window_start=str(window_start))
            return

        with Session(get_engine()) as session:
            session.execute(
                text(
                    """
                    INSERT INTO pipeline_metrics
                        (window_start, window_end, total_events,
                         pageview_count, purchase_count, click_count, error_count)
                    VALUES
                        (:ws, :we, :total, :pv, :pu, :cl, :er)
                    """
                ),
                {
                    "ws": window_start,
                    "we": window_end,
                    "total": total,
                    "pv": type_counts.get("pageview", 0),
                    "pu": type_counts.get("purchase", 0),
                    "cl": type_counts.get("click", 0),
                    "er": type_counts.get("error", 0),
                },
            )
            session.commit()

        log.info(
            "window_flushed",
            window_start=str(window_start),
            window_end=str(window_end),
            total=total,
            by_type=type_counts,
        )


# Module-level singleton — imported by consumer/main.py
_aggregator = _EventAggregator()


def record_event(event: dict) -> None:
    """Public entry point: add one event to the current 60-second window."""
    _aggregator.record_event(event)
