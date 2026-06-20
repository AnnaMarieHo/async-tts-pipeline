import asyncio
import json
import base64
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from app.utils.batching import chunk_by_sentence, batch_text
from app.services.TTS_service import stream_batch
from app.objects.EventQueue import EventQueue

app = FastAPI()
@app.get("/")
async def index():
    return {"status": "ok"}


@app.websocket("/stream-audio")
async def stream_audio(websocket: WebSocket):
    await websocket.accept()
    event_queue = EventQueue()
    queue_notifier = asyncio.Event()


    async def listen_to_client():
        try:
            while True:
                data = await websocket.receive_text()
                payload = json.loads(data)
                prompts = payload.get("prompts", [])
                for prompt in prompts:
                    event_queue.put(prompt)
                if prompts:
                    queue_notifier.set()
        except WebSocketDisconnect:
            print("Client dropped connection")
            queue_notifier.set()

    async def process_and_stream():
        try:
            while True:
                if event_queue.get_length() == 0:
                    await queue_notifier.wait()
                    queue_notifier.clear()
                if event_queue.get_length() == 0:
                    break
                event = event_queue.pop()
                if not event:
                    continue

                print(f"Processing job {event.job_id} created at {event.created_at}")

                chunks = chunk_by_sentence(event.text)
                batches = batch_text(chunks, 2)
                current_batch_idx = 0
                async for finished_batch in stream_batch(batches):
                    for item in finished_batch["data"]:
                        audio_bytes = item["audio"]
                        b64_audio = base64.b64encode(audio_bytes).decode("utf-8")

                        await websocket.send_json({
                            "job_id": event.job_id,
                            "batch": finished_batch["batch"],
                            "audio_data": b64_audio
                        })
                    current_batch_idx += 1

        except (asyncio.CancelledError, WebSocketDisconnect) as e:
            print("Streamer task caught explicit cancel signal. Halting LLM/TTS streams.")

            if 'event' in locals() and event:
                print(f"Total batches generated {len(batches)}")
                print(f"Successfully sent up batch: {current_batch_idx}")

                unfinished_batches = batches[current_batch_idx:]
                print(f"Number of unprocessed batches: {len(unfinished_batches)}")
                for idx, b in enumerate(unfinished_batches, start=current_batch_idx):
                    print(f"Unsent batch [{idx}]: {b}")

            backlog_count = event_queue.get_length()
            print(f"Number of unprocessed jobs in queue: {backlog_count}")
            while event_queue.get_length() > 0:
                stale_event = event_queue.pop()
                if stale_event:
                    print(f"Unprocessed Job ID: {stale_event.job_id}")

            if isinstance(e, asyncio.CancelledError):
                raise

    listener_task = asyncio.create_task(listen_to_client())
    streamer_task = asyncio.create_task(process_and_stream())

    done, pending = await asyncio.wait(
        [listener_task, streamer_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    print("Websocket Pipeline completely flushed and shut down safely")