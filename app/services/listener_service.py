import json
from fastapi import WebSocket, WebSocketDisconnect
from app.objects.EventQueue import EventQueue
from app.objects.types import QueueNotifier

async def listen_to_client(websocket: WebSocket, event_queue: EventQueue, queue_notifier: QueueNotifier):
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
