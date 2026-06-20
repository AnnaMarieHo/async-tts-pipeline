from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import AsyncIterator, Any
from app.objects.EventQueue import EventQueue
from app.objects.types import QueueNotifier
from app.objects.types import FullAudio
from app.utils.batching import chunk_by_sentence, batch_text
from app.services.TTS_service import fetch_data_with_retry
from app.utils.exception_handler import stream_exception_handler
import base64
import asyncio

class BatchResult(BaseModel):
    batch_id: int
    batch: list[str]
    data: list[FullAudio]

class AudioStream(BaseModel):
    job_id: str
    batch: list[str]
    audio_data: str


async def stream_batch(batches: list[list[str]]) -> AsyncIterator[BatchResult]:
    for batch_id, batch in enumerate(batches):
        print("batch_id:", batch_id)
        print("batch:", batch)

        tasks = [
            asyncio.create_task(fetch_data_with_retry(seq_id, text))
            for seq_id, text in enumerate(batch)
        ]

        try:
            completed_batch_results = await asyncio.gather(*tasks)
            completed_batch_results.sort(key=lambda item: item.seq_id)

            yield BatchResult(
                batch_id=batch_id,
                batch=batch,
                data=completed_batch_results
            )

        except asyncio.CancelledError:
            print(f"[BATCH CANCEL] Generator interrupted. Force-cancelling {len(tasks)} running workers.")

            for t in tasks:
                if not t.done():
                    t.cancel()

            # Wait for tasks to clear out so semaphores release cleanly
            await asyncio.gather(*tasks, return_exceptions=True)
            raise


async def process_and_stream(websocket : WebSocket, event_queue: EventQueue, queue_notifier: QueueNotifier) -> None:
    async with stream_exception_handler(event_queue) as ctx:
        while True:

            if event_queue.get_length() == 0:
                await queue_notifier.wait()
                queue_notifier.clear()
            if event_queue.get_length() == 0:
                break

            event = event_queue.pop()

            if not event:
                continue

            ctx["event"] = event
            print(f"Processing job {event.job_id} created at {event.created_at}")


            chunks = chunk_by_sentence(event.text)
            batches = batch_text(chunks, 2)

            ctx["batches"] = batches
            ctx["current_batch_idx"] = 0

            async for finished_batch in stream_batch(batches):
                for item in finished_batch.data:
                    audio_bytes = item.audio
                    b64_audio = base64.b64encode(audio_bytes).decode("utf-8")

                    payload = AudioStream(
                        job_id=event.job_id,
                        batch=finished_batch.batch,
                        audio_data=b64_audio
                    )

                    await websocket.send_json(payload.model_dump())

                ctx["current_batch_idx"] += 1