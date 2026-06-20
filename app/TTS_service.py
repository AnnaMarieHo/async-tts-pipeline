import asyncio
import time
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ELEVEN_LABS_API")
VOICE_ID = os.getenv("VOICE_ID")
ELEVENLABS_SEMAPHORE = asyncio.Semaphore(2)


async def fetch_data_with_retry(seq_id, text):
    retries = 3
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}/stream"

    headers = {
        "xi-api-key": API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "text": text,
        "model_id": "eleven_multilingual_v2"
    }
    params = {"output_format": "pcm_16000"}

    for attempt in range(retries):
        async with ELEVENLABS_SEMAPHORE:
            async with httpx.AsyncClient() as client:
                try:
                    print(f"[API] seq_id {seq_id} running concurrent stream (Attempt {attempt + 1})...")
                    t1 = time.perf_counter()

                    async with client.stream("POST", url, json=payload, headers=headers, params=params) as response:
                        if response.status_code != 200:
                            raise Exception(f"ElevenLabs error: {response.status_code}")

                        audio_chunks = []

                        async for chunk in response.aiter_bytes():
                            audio_chunks.append(chunk)

                        final_audio = b"".join(audio_chunks)
                        t2 = time.perf_counter()

                        print(f"[TTFB] {t2 - t1}")
                        print(f"[API] seq_id {seq_id} successfully serialized ({len(final_audio)} bytes).")
                        return {"seq_id": seq_id, "audio": final_audio}

                except asyncio.CancelledError:
                    #  intercept the millisecond the websocket drops mid-stream
                    print(f"[STATE-LOCK] Network socket hard-severed for seq_id {seq_id}. Token consumption halted.")
                    raise  # Propagate to instantly clear the semaphore
                except Exception as e:
                    if attempt == retries - 1:
                        raise e


async def stream_batch(batches):
    for batch_id, batch in enumerate(batches):
        print("batch_id:", batch_id)
        print("batch:", batch)

        tasks = [
            asyncio.create_task(fetch_data_with_retry(seq_id, text))
            for seq_id, text in enumerate(batch)
        ]

        try:
            completed_batch_results = await asyncio.gather(*tasks)
            completed_batch_results.sort(key=lambda item: item['seq_id'])
            yield {
                "batch_id": batch_id,
                "batch": batch,
                "data": completed_batch_results
            }
        except asyncio.CancelledError:
            print(f"[BATCH CANCEL] Generator interrupted. Force-cancelling {len(tasks)} running workers.")
            for t in tasks:
                if not t.done():
                    t.cancel()
            # Wait for tasks to clear out so semaphores release cleanly
            await asyncio.gather(*tasks, return_exceptions=True)
            raise