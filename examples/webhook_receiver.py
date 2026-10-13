"""Local webhook receiver with repeatable success, retry and timeout scenarios."""

import asyncio
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response

app = FastAPI(title="Local webhook receiver", docs_url=None, redoc_url=None)
events: deque[dict[str, Any]] = deque(maxlen=1000)
attempts: Counter[tuple[str, str]] = Counter()
lock = asyncio.Lock()


async def record(request: Request) -> int:
    payload = await request.json()
    event_id = str(payload.get("event_id", "missing"))
    path = request.url.path
    async with lock:
        attempts[(path, event_id)] += 1
        attempt_no = attempts[(path, event_id)]
        events.append(
            {
                "received_at": datetime.now(UTC).isoformat(),
                "path": path,
                "request_no": attempt_no,
                "event_id": event_id,
                "payload": payload,
            }
        )
    return attempt_no


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook", status_code=204)
async def webhook(request: Request) -> Response:
    await record(request)
    return Response(status_code=204)


@app.post("/fail", status_code=500)
async def fail(request: Request) -> Response:
    await record(request)
    return Response(status_code=500)


@app.post("/flaky")
async def flaky(request: Request) -> Response:
    attempt_no = await record(request)
    return Response(status_code=500 if attempt_no <= 2 else 204)


@app.post("/slow")
async def slow(request: Request) -> Response:
    await record(request)
    await asyncio.sleep(20)
    return Response(status_code=204)


@app.get("/events")
async def list_events() -> dict[str, Any]:
    async with lock:
        result = list(events)
    return {"count": len(result), "events": result}


@app.delete("/events", status_code=204)
async def clear_events() -> Response:
    async with lock:
        events.clear()
        attempts.clear()
    return Response(status_code=204)
