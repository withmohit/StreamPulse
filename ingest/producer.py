import json

async def send_event(event):
    # Replace this stub with Kafka producer logic.
    payload = event.json()
    print(f"Sending event to Kafka: {payload}")
    # TODO: integrate aiokafka producer and publish to a topic
