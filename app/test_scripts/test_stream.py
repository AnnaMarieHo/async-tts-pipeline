import asyncio
import websockets
import json
from app.utils.prompts import tts_prompt1, tts_prompt2


async def test_stream():
    uri = "ws://127.0.0.1:8000/stream-audio"
    async with websockets.connect(uri) as websocket:
        print("Connected to Websocket")
        payload = {"prompts": [tts_prompt1, tts_prompt2]}
        await websocket.send(json.dumps(payload))
        print("Sent prompts to server")
        chunk_count = 0
        try:
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                print(f"Received stream chunk: {data}")

                chunk_count += 1
                # SIMULATE CANCELLATION: Cut the cord after 2 chunks arrive
                if chunk_count == 2:
                    print("\nSimulating user closing the tab / canceling now\n")
                    break  # Exiting this loop exits the context manager, killing the socket

        except websockets.exceptions.ConnectionClosed:
            print("Connection closed by server")
    print("Client disconnected cleanly from test side. Check server logs!")
asyncio.run(test_stream())


