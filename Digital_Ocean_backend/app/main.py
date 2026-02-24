import json
import os
import asyncio
import logging
import hashlib
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException
from db import connect_db, get_db, db_pool
from app.ingestion.manager import IngestionManager

log = logging.getLogger("ingestion.app")

# ── FIX M3: Deferred BUSINESS_NUMBER_ID — read at startup, not import ────────
_BUSINESS_NUMBER_ID: int = None

# Global references to keep manager + task alive
_ingestion_manager: IngestionManager = None
_ingestion_task: asyncio.Task = None


# ── FIX 4: Done callback to surface silent task failures ────────────────────
def _ingestion_task_done(task: asyncio.Task) -> None:
    """Callback fired when the ingestion manager task completes."""
    try:
        exc = task.exception()
        if exc is not None:
            log.critical(
                f"🚨 Ingestion manager task CRASHED: {exc}",
                exc_info=exc,
            )
    except asyncio.CancelledError:
        log.info("Ingestion manager task was cancelled")


# ── FIX M4: Lifespan replaces deprecated @app.on_event ─────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown logic."""
    global _ingestion_manager, _ingestion_task, _BUSINESS_NUMBER_ID

    # ── STARTUP ──────────────────────────────────────────────────────────
    # 1. Validate BUSINESS_NUMBER_ID (deferred from import time)
    raw_id = os.getenv("BUSINESS_NUMBER_ID")
    if not raw_id:
        raise RuntimeError(
            "BUSINESS_NUMBER_ID environment variable is required. "
            "Set it to the ID of the business number in the 'numbers' table."
        )
    _BUSINESS_NUMBER_ID = int(raw_id)
    log.info(f"✓ BUSINESS_NUMBER_ID = {_BUSINESS_NUMBER_ID}")

    # 2. Connect webhook DB pool
    await connect_db()

    # 3. Launch ingestion workers as background tasks (non-blocking)
    _ingestion_manager = IngestionManager()
    _ingestion_task = asyncio.create_task(_ingestion_manager.start(pool=db_pool))
    _ingestion_task.add_done_callback(_ingestion_task_done)

    yield  # ── application runs here ──

    # ── SHUTDOWN ─────────────────────────────────────────────────────────
    if _ingestion_manager:
        await _ingestion_manager.stop()


app = FastAPI(lifespan=lifespan)


# ── Health & readiness endpoints ────────────────────────────────────────────

@app.get("/health")
async def health():
    """Liveness check — process is running."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness check — DB is reachable and workers are alive."""
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        raise HTTPException(status_code=503, detail="DB not ready")

    workers_alive = True
    if _ingestion_task is not None and _ingestion_task.done():
        workers_alive = False

    return {
        "ready": True,
        "db": "connected",
        "ingestion_workers": "running" if workers_alive else "stopped",
    }


@app.get("/")
def home():
    return {"message": "Server is running"}


# ── Helper: extract a stable event ID from webhook payload ──────────────────

def _extract_event_id(payload: dict) -> str:
    """
    Extract a stable unique identifier from the webhook payload.

    Priority:
      1. message.id  (for message events)
      2. status.id   (for status events)
      3. SHA-256 hash of the payload (fallback for unknown event types)
    """
    # Message events
    message = payload.get("message") or payload.get("messages", [{}])
    if isinstance(message, dict) and message.get("id"):
        return str(message["id"])
    if isinstance(message, list) and message and message[0].get("id"):
        return str(message[0]["id"])

    # Status events
    status_data = payload.get("status") or payload.get("statuses", [{}])
    if isinstance(status_data, dict) and status_data.get("id"):
        return str(status_data["id"])
    if isinstance(status_data, list) and status_data and status_data[0].get("id"):
        return str(status_data[0]["id"])

    # Fallback: deterministic hash of payload
    payload_str = json.dumps(payload, sort_keys=True, default=str)
    return "hash-" + hashlib.sha256(payload_str.encode()).hexdigest()[:32]


@app.post("/webhooks/turn")
async def turn_webhook(request: Request, conn=Depends(get_db)):
    payload = await request.json()
    event_type = payload.get("type")

    # ── Idempotent webhook INSERT (raw data → webhook_events) ───────────
    external_event_id = _extract_event_id(payload)

    await conn.execute(
        """
        INSERT INTO webhook_events (provider, event_type, external_event_id, payload, processed)
        VALUES ($1, $2, $3, $4::jsonb, FALSE)
        ON CONFLICT (provider, external_event_id) DO NOTHING
        """,
        "turn_io",
        event_type,
        external_event_id,
        json.dumps(payload)
    )

    # ── MESSAGE EVENT → messages table ──────────────────────────────────
    if event_type == "message":
        message = payload.get("message", {})
        external_id = message.get("id")

        existing_msg = await conn.fetchrow(
            "SELECT id FROM messages WHERE external_id = $1",
            external_id
        )

        # FIX M2: Extract direction from payload instead of hardcoding
        direction = message.get("direction", "inbound")

        if not existing_msg:
            try:
                await conn.execute(
                    """
                    INSERT INTO messages (
                        external_id,
                        number_id,
                        content,
                        direction,
                        raw_body
                    )
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    """,
                    external_id,
                    _BUSINESS_NUMBER_ID,
                    message.get("text", ""),
                    direction,
                    json.dumps(message)
                )
            except Exception as exc:
                log.error(
                    f"Failed to insert message (external_id={external_id}): {exc}. "
                    f"This may indicate messages.id has no default sequence."
                )

    # ── STATUS EVENT → statuses table ───────────────────────────────────
    if event_type == "status":
        status_data = payload.get("status", {})

        message_row = await conn.fetchrow(
            """
            SELECT id, number_id
            FROM messages
            WHERE external_id = $1
            """,
            status_data.get("id")
        )

        if message_row:
            # FIX M1: Use timestamp from payload, fall back to now()
            raw_ts = status_data.get("timestamp")
            if raw_ts:
                try:
                    from datetime import datetime, timezone
                    # Turn.io sends epoch seconds as string
                    ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)
                    ts_clause = "$5"
                    ts_value = ts
                except (ValueError, TypeError, OSError):
                    ts_clause = "now()"
                    ts_value = None
            else:
                ts_clause = "now()"
                ts_value = None

            if ts_value is not None:
                await conn.execute(
                    f"""
                    INSERT INTO statuses (
                        message_id,
                        number_id,
                        status,
                        raw_body,
                        status_timestamp
                    )
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    """,
                    message_row["id"],
                    message_row["number_id"],
                    status_data.get("status"),
                    json.dumps(status_data),
                    ts_value
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO statuses (
                        message_id,
                        number_id,
                        status,
                        raw_body,
                        status_timestamp
                    )
                    VALUES ($1, $2, $3, $4::jsonb, now())
                    """,
                    message_row["id"],
                    message_row["number_id"],
                    status_data.get("status"),
                    json.dumps(status_data)
                )

    return {"status": "saved"}
