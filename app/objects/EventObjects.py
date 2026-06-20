import time
from collections import deque
import uuid

class TtsEvent:
    def __init__(self, text, created_at, job_id):
        self.job_id = job_id
        self.text = text
        self.created_at = created_at
    def __repr__(self):
        return f"job_id: {self.job_id}, created_at: {self.created_at}, has_text: {len(self.text) > 0}"


class EventQueue:
    def __init__(self):
        self._jobs: deque[TtsEvent] = deque()

    def __getitem__(self, item) -> TtsEvent:
        return self._jobs[item]

    def _purge_expired(self) -> None:
        ttl = 900
        now = time.monotonic()
        while self._jobs and (now - self._jobs[0].created_at > ttl):
            self._jobs.popleft()

    def show_events(self):
        return list(self._jobs)

    def get_length(self):
        return len(self._jobs)

    def put(self, text:str) -> str:
        self._purge_expired()
        job_id = str(uuid.uuid4())
        self._jobs.append(TtsEvent(text=text, created_at=time.monotonic(), job_id=job_id))
        return job_id

    def pop(self) -> TtsEvent | None:
        self._purge_expired()
        return self._jobs.popleft()

