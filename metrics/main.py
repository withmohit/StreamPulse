from fastapi import FastAPI

app = FastAPI(title="StreamPulse Metrics")

@app.get("/metrics")
def metrics():
    return {"status": "ok", "metrics": {"events_processed": 0}}
