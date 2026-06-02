CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    raw_value TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
