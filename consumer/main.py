import asyncio

from consumer.aggregator import process_event

async def main():
    print("Starting consumer...")
    # TODO: Replace with Kafka consumer implementation
    while True:
        await asyncio.sleep(5)
        dummy_event = {"id": "fake", "value": 1}
        await process_event(dummy_event)

if __name__ == "__main__":
    asyncio.run(main())
