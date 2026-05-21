from fastapi import FastAPI
from ingest.schemas import Event
from ingest.producer import send_event

app = FastAPI(title="StreamPulse Ingest API")

@app.post("/ingest")
async def ingest_event(event: Event):
    await send_event(event)
    return {"status": "accepted", "event_id": event.id}
