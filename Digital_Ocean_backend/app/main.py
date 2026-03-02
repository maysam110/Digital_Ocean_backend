import hashlib
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from app.ingestion.manager import IngestionManager
from db import close_shared_pool, create_shared_pool

log = logging.getLogger("ingestion.app")

# Global references for readiness + clean shutdown.
_ingestion_manager: IngestionManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Process lifecycle with a single shared DB pool.

    Structural correction:
      - create one asyncpg pool here
      - pass the same pool to webhook path + ingestion manager
      - close it once during shutdown
    """
    global _ingestion_manager

    pool = await create_shared_pool()
    app.state.db_pool = pool

    _ingestion_manager = IngestionManager(pool)
    await _ingestion_manager.start()
    app.state.ingestion_manager = _ingestion_manager

    yield

    # Shutdown order: workers -> HTTP clients -> DB pool.
    if _ingestion_manager is not None:
        await _ingestion_manager.stop()
    await close_shared_pool(pool)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    """Liveness check — process is running."""
    return {"status": "ok"}


@app.get("/ready")
async def ready(request: Request):
    """Readiness check — DB responds and both workers are running."""
    pool = request.app.state.db_pool
    manager: IngestionManager = request.app.state.ingestion_manager

    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB not ready: {exc}")

    workers_running = manager.workers_running() if manager else False
    if not workers_running:
        raise HTTPException(status_code=503, detail="Ingestion workers not ready")

    return {
        "ready": True,
        "db": "connected",
        "ingestion_workers": "running",
    }


@app.get("/")
def home():
    return {"message": "Server is running"}


def _extract_event_id(payload: dict) -> str:
    """
    Extract a stable unique identifier from the webhook payload.

    Priority:
      1. message.id  (for message events)
      2. status.id   (for status events)
      3. SHA-256 hash of full payload as deterministic fallback
    """
    message = payload.get("message") or payload.get("messages", [{}])
    if isinstance(message, dict) and message.get("id"):
        return str(message["id"])
    if isinstance(message, list) and message and message[0].get("id"):
        return str(message[0]["id"])

    status_data = payload.get("status") or payload.get("statuses", [{}])
    if isinstance(status_data, dict) and status_data.get("id"):
        return str(status_data["id"])
    if isinstance(status_data, list) and status_data and status_data[0].get("id"):
        return str(status_data[0]["id"])

    payload_str = json.dumps(payload, sort_keys=True, default=str)
    return "hash-" + hashlib.sha256(payload_str.encode()).hexdigest()[:32]


@app.post("/webhooks/turn")
async def turn_webhook(request: Request):
    """
    Raw-only webhook ingestion.

    Hard requirement:
      - write only to webhook_events
      - idempotent ON CONFLICT DO NOTHING
      - no writes to messages/statuses tables
    """
    payload = await request.json()
    event_type = payload.get("type")
    external_event_id = _extract_event_id(payload)
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO webhook_events (provider, event_type, external_event_id, payload, processed)
            VALUES ($1, $2, $3, $4::jsonb, FALSE)
            ON CONFLICT (provider, external_event_id) DO NOTHING
            """,
            "turn_io",
            event_type,
            external_event_id,
            json.dumps(payload),
        )

    return {"status": "saved"}
