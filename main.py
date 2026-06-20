import asyncio
import json
import base64
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from app.services.listener_service import listen_to_client
from app.services.stream_service import process_and_stream
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


    listener_task = asyncio.create_task(listen_to_client(websocket, event_queue, queue_notifier))
    streamer_task = asyncio.create_task(process_and_stream(websocket, event_queue, queue_notifier))

    done, pending = await asyncio.wait(
        [listener_task, streamer_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    print("Websocket Pipeline completely flushed and shut down safely")