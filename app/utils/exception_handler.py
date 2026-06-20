import asyncio
from contextlib import asynccontextmanager
from fastapi import WebSocketDisconnect
from app.objects.EventQueue import EventQueue

@asynccontextmanager
async def stream_exception_handler(event_queue: EventQueue):
    state = {
        "event": None,
        "batches": [],
        "current_batch_idx": 0
    }
    try:
        yield state
    except (asyncio.CancelledError, WebSocketDisconnect) as e:
        print("Streamer task caught explicit cancel signal. Halting LLM/TTS streams.")

        event = state["event"]
        batches = state["batches"]
        current_batch_idx = state["current_batch_idx"]

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
