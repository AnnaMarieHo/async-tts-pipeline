An asynchronous text-to-speech (TTS) streaming pipeline built with FastAPI and asyncio. Rather than using server-sent-event (SSE) patterns, this project uses a state-locked, concurrent architecture leveraging httpx and asyncio.gather for reduced Time-To-First-Byte (TTFB) and near immediate cancellation feedback. Features include sentence-based batching, real-time audio chunk serialization, and proactive network-layer task cancellation to optimize API quota and resource usage

<h4>Project Overview</h4>
This service pursues low-latency, streaming TTS by optimizing the synthesis-to-playback lifecycle. 
The architecture utilizes State-Locked Cancellation via native asyncio.Task propagation, ensuring that active 
ElevenLabs HTTP network connections are severed the millisecond a WebSocket disconnect event occurs.

<h4>Current Engineering Features</h4>

**Concurrent Pipelining:** Uses asyncio.gather and Semaphore(2) to concurrently fetch audio chunks for batched sentences, reducing the total time to process a multi-sentence paragraph while maintaining strict output order.

**State-Locked Cancellation:** Decouples network I/O from execution logic. When a client disconnects, the event loop injects a CancelledError that propagates through the task group, triggering an immediate termination of the underlying httpx socket.

**Efficient Serialization:** Bypasses unnecessary decode/re-encode cycles by forwarding raw base64 PCM frames directly from the ElevenLabs stream to the WebSocket client.

**Backpressure & Queueing:** Implements an EventQueue to manage incoming prompts, ensuring session cleanup and log-based auditing of unprocessed workloads during abrupt interruptions.
</br>
</br>
<h4>Tech Stack</h4>

**Core:** Python 3.12+, FastAPI, Asyncio</br>

**Networking:** httpx.AsyncClient (replaces legacy SDK wrappers for granular socket control)</br>

**Synthesis:** ElevenLabs API (/v1/text-to-speech/{voice_id}/stream)</br>

**Concurrency:** Structured Task Groups (asyncio.gather), Semaphores for rate-limiting.
