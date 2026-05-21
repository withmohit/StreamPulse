# StreamPulse

A skeleton for a streaming ingestion and processing pipeline using FastAPI, Kafka, and Postgres.

## Project layout

- `docker-compose.yml` - defines Postgres, Zookeeper, Kafka, ingest API, consumer, and metrics services.
- `ingest/` - FastAPI ingestion API, event schemas, and Kafka producer stub.
- `consumer/` - Kafka consumer entrypoint and windowed aggregation stub.
- `metrics/` - FastAPI metrics service.
- `db/` - database schema initialization script.
- `simulator/` - sample fake event generator.

## Quick start

1. Install dependencies:
   ```bash
   poetry install
   ```

2. Run services:
   ```bash
   docker compose up --build
   ```

3. Test ingest API:
   ```bash
   curl -X POST http://localhost:8000/ingest \
     -H "Content-Type: application/json" \
     -d '{"id":"1","source":"app","type":"click","value":42.0,"timestamp":"2026-05-21T00:00:00Z"}'
   ```
