from datetime import datetime
from pydantic import BaseModel

class Event(BaseModel):
    id: str
    source: str
    type: str
    value: float
    timestamp: datetime
