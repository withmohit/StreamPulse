import asyncio
import random
import uuid
from datetime import datetime

async def generate_event():
    event = {
        "id": str(uuid.uuid4()),
        "source": "simulator",
        "type": random.choice(["click", "view", "purchase"]),
        "value": round(random.random() * 100, 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    print(event)

async def main():
    while True:
        await generate_event()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
