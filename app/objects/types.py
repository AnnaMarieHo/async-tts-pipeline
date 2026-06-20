import asyncio
from pydantic import BaseModel

type QueueNotifier = asyncio.Event

class FullAudio(BaseModel):
    seq_id: int
    audio: bytes
