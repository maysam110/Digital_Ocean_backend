#!/usr/bin/env python3
"""
Normalization Worker — Batch Processing & Orchestration

Contains the main batch polling loop, per-event transaction orchestration
(8-step message processing, status event processing), heartbeat task,
and database retry logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
from typing import Optional

import asyncpg

from config import (
    BATCH_SIZE,
    EMPTY_POLL_INTERVAL_SECONDS,
    POLL_INTERVAL_SECONDS,
    RETRY_DELAYS,
)
from models import WorkerState
from parsers import (
    has_valid_vnd,
    parse_attachment,
    parse_chat,
    parse_chat_state,
    parse_contact,
    parse_message,
    parse_status_from_message,
    validate_status,
)
import db_writer
import state_manager

logger = logging.getLogger("normalization")


# ═══════════════════════════════════════════════════════════════════════
# Batch Fetch with Retry
# ═══════════════════════════════════════════════════════════════════════

async def fetch_batch_with_retry(
    pool: asyncpg.Pool,
    last_id: int,
    batch_size: int,
) -> list:
    """
    Fetch a batch of unprocessed webhook_events, retrying on DB errors.

    Polls by id offset — no event_type filter in WHERE clause.
    On exhausted retries, raises to crash and let the container restart.
    """
    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT id, event_type, payload "
                    "FROM webhook_events "
                    "WHERE id > $1 "
                    "ORDER BY id ASC "
                    "LIMIT $2",
                    last_id,
                    batch_size,
                )
        except (asyncpg.PostgresConnectionError, OSError) as e:
            if attempt == len(RETRY_DELAYS) - 1:
                logger.error({
                    "event": "db_retry_exhausted",
                    "context": "fetch_batch",
                    "error": str(e),
                })
                raise
            logger.warning({
                "event": "db_retry",
                "context": "fetch_batch",
                "attempt": attempt + 1,
                "delay": delay,
                "error": str(e),
            })
            await asyncio.sleep(delay)

    return []  # unreachable, but satisfies type checker


# ═══════════════════════════════════════════════════════════════════════
# Per-Event Processing — Message (8 Steps)
# ═══════════════════════════════════════════════════════════════════════

async def process_message_event(
    conn: asyncpg.Connection,
    webhook_event_id: int,
    payload: dict,
) -> None:
    """
    Process a single message event through all 8 normalization steps.

    All steps execute on the same connection within the same transaction.
    """
    # Step 0a: Guard — validate external_id
    external_id = payload.get("id")
    if not external_id:
        raise ValueError("missing_external_id: payload.id is null or missing")

    # Step 0b: Guard — validate _vnd block
    if not has_valid_vnd(payload):
        raise ValueError("missing_vnd_block")

    # Step 1: Contact resolution
    contact_row = parse_contact(payload)
    contact_id = await db_writer.upsert_contact(conn, contact_row)

    # Step 2: Chat resolution (with FK violation safety on assigned_to_uuid)
    chat_row = parse_chat(payload, contact_id)
    try:
        chat_id = await db_writer.upsert_chat(conn, chat_row)
    except asyncpg.ForeignKeyViolationError as e:
        if "assigned_to_uuid" in str(e) or "fk_chats_operator" in str(e):
            logger.warning({
                "event": "assigned_to_uuid_fk_violation",
                "chat_uuid": str(chat_row.uuid),
                "assigned_to_uuid": str(chat_row.assigned_to_uuid),
                "message": "Operator not found in operators table. Retrying with assigned_to_uuid=NULL.",
            })
            chat_row.assigned_to_uuid = None
            chat_id = await db_writer.upsert_chat(conn, chat_row)
        else:
            raise  # re-raise if it's a different FK violation

    # Step 3: Campaign / journey linking (lookup only, no auto-create)
    campaign_id = await _resolve_campaign(conn, payload)

    # Step 4: Message insert
    message_row = parse_message(payload, chat_id, campaign_id)
    message_id = await db_writer.insert_message(conn, message_row)

    # Step 5: Status upsert (only if last_status is present)
    status_row = parse_status_from_message(payload, message_id)
    if status_row is not None:
        status_row.raw_body = payload  # pass dict directly — asyncpg handles jsonb
        await db_writer.upsert_status(conn, status_row)

    # Step 6: Chat state upsert
    chat_state_row = parse_chat_state(payload, chat_id)
    await db_writer.upsert_chat_state(conn, chat_state_row)

    # Step 7: Attachment insert (only for media types)
    attachment_row = parse_attachment(payload, message_id, chat_id)
    if attachment_row is not None:
        await db_writer.insert_attachment(conn, attachment_row)

    # Step 8: Update chats.last_message_id
    await db_writer.update_last_message(conn, chat_id, message_id)


async def _resolve_campaign(
    conn: asyncpg.Connection,
    payload: dict,
) -> Optional[object]:
    """
    Resolve a campaign_id from the author's journey_uuid.

    Does NOT auto-create campaigns. Lookup only.
    Returns UUID or None.
    """
    v1 = payload.get("_vnd", {}).get("v1", {})
    author = v1.get("author", {})
    journey_uuid = author.get("journey_uuid")

    if not journey_uuid:
        return None

    row = await conn.fetchrow("""
        SELECT DISTINCT jp.campaign_id
        FROM journey_progress jp
        WHERE jp.journey_id = $1
          AND jp.campaign_id IS NOT NULL
        LIMIT 1
    """, journey_uuid)

    if row:
        return row["campaign_id"]
    return None


# ═══════════════════════════════════════════════════════════════════════
# Per-Event Processing — Status
# ═══════════════════════════════════════════════════════════════════════

async def process_status_event(
    conn: asyncpg.Connection,
    webhook_event_id: int,
    payload: dict,
) -> None:
    """
    Process a standalone status event (delivery callback).

    Resolves the original message by external_id, then upserts a status row.
    """
    # Guard: check _vnd block (status events may have simpler structure)
    vnd = payload.get("_vnd")
    if not isinstance(vnd, dict):
        raise ValueError("missing_vnd_block")
    v1 = vnd.get("v1")
    if not isinstance(v1, dict):
        raise ValueError("missing_vnd_v1_block")

    message_external_id = payload.get("id")
    if not message_external_id:
        raise ValueError("status_event_missing_id")

    status = v1.get("last_status")
    if not status:
        # Nothing to update — skip silently
        return

    # C3: Pre-validate status enum value
    validated_status = validate_status(status)

    # C2: Raise on missing number_id instead of fallback to 0
    # FALLBACK: Use environment variable if missing from payload
    raw_number_id = payload.get("number_id", config.BUSINESS_NUMBER_ID)
    number_id = int(raw_number_id)

    status_timestamp = v1.get("last_status_timestamp")

    await db_writer.process_status_event_db(
        conn=conn,
        webhook_event_id=webhook_event_id,
        message_external_id=message_external_id,
        status=validated_status,
        status_timestamp=status_timestamp,
        number_id=number_id,
        payload=payload,  # pass dict directly — asyncpg handles jsonb
    )


# ═══════════════════════════════════════════════════════════════════════
# Batch Processing
# ═══════════════════════════════════════════════════════════════════════

async def process_batch(
    pool: asyncpg.Pool,
    events: list,
) -> tuple[int, int, int]:
    """
    Process a batch of webhook events, one transaction per event.

    Returns (max_id, processed_count, failure_count).
    """
    max_id = 0
    processed = 0
    failures = 0

    for row in events:
        event_id = row["id"]
        max_id = max(max_id, event_id)

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Parse payload exactly ONCE
                    raw_payload = row["payload"]
                    if isinstance(raw_payload, str):
                        payload = json.loads(raw_payload)
                    else:
                        # asyncpg may return jsonb as a dict already
                        payload = raw_payload

                    event_type = row["event_type"]

                    if event_type == "status":
                        await process_status_event(conn, event_id, payload)
                    elif event_type == "message":
                        await process_message_event(conn, event_id, payload)
                    else:
                        # Unknown event type — log as failure and skip
                        raise ValueError(f"unknown_event_type: {event_type}")

            processed += 1

        except Exception as e:
            failures += 1
            error_msg = str(e)
            stack = traceback.format_exc()

            logger.warning({
                "event": "event_failed",
                "webhook_event_id": event_id,
                "error_message": error_msg,
                "stack_trace": stack[:500],
            })

            # Use a SEPARATE connection for failure recording
            try:
                raw_payload = row["payload"]
                payload_str = (
                    raw_payload if isinstance(raw_payload, str)
                    else json.dumps(raw_payload)
                )
                async with pool.acquire() as fail_conn:
                    await db_writer.insert_failure(
                        fail_conn,
                        event_id,
                        payload_str,
                        error_msg,
                        stack,
                    )
            except Exception as fail_exc:
                logger.error({
                    "event": "failure_record_error",
                    "webhook_event_id": event_id,
                    "error": str(fail_exc),
                })

    return max_id, processed, failures


# ═══════════════════════════════════════════════════════════════════════
# Heartbeat Background Task
# ═══════════════════════════════════════════════════════════════════════

async def heartbeat_loop(
    state: WorkerState,
    shutdown_event: asyncio.Event,
) -> None:
    """Emit a heartbeat log every 60 seconds until shutdown."""
    while not shutdown_event.is_set():
        logger.info({
            "event": "worker_alive",
            "checkpoint": state.last_event_id,
            "events_per_minute": state.events_per_minute(),
            "total_processed": state.total_processed,
            "total_failures": state.total_failures,
            "uptime_seconds": state.uptime_seconds(),
        })
        try:
            await asyncio.wait_for(
                asyncio.shield(shutdown_event.wait()),
                timeout=60,
            )
        except asyncio.TimeoutError:
            pass  # normal — emit next heartbeat


# ═══════════════════════════════════════════════════════════════════════
# Main Worker Loop
# ═══════════════════════════════════════════════════════════════════════

async def run_worker(
    pool: asyncpg.Pool,
    state: WorkerState,
    shutdown_event: asyncio.Event,
) -> None:
    """
    Main batch polling loop.

    Continuously fetches batches of webhook_events, processes each event
    in its own transaction, then advances the checkpoint.

    Exits cleanly when shutdown_event is set.
    """
    logger.info({
        "event": "worker_loop_started",
        "checkpoint": state.last_event_id,
        "batch_size": BATCH_SIZE,
        "poll_interval": POLL_INTERVAL_SECONDS,
        "empty_poll_interval": EMPTY_POLL_INTERVAL_SECONDS,
    })

    while not shutdown_event.is_set():
        try:
            batch_start = time.monotonic()

            # Fetch batch
            events = await fetch_batch_with_retry(
                pool, state.last_event_id, BATCH_SIZE
            )

            if not events:
                logger.info({
                    "event": "batch_empty",
                    "checkpoint": state.last_event_id,
                    "sleep_seconds": EMPTY_POLL_INTERVAL_SECONDS,
                })
                # Wait for empty poll interval, but break early on shutdown
                try:
                    await asyncio.wait_for(
                        asyncio.shield(shutdown_event.wait()),
                        timeout=EMPTY_POLL_INTERVAL_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            # Process batch
            max_id, processed, failures = await process_batch(pool, events)

            # Advance checkpoint to highest id attempted
            if max_id > state.last_event_id:
                await state_manager.update_checkpoint(pool, max_id)
                state.last_event_id = max_id

            # Update metrics
            state.total_processed += processed
            state.total_failures += failures
            state.record_batch(processed)

            batch_duration_ms = int((time.monotonic() - batch_start) * 1000)

            logger.info({
                "event": "batch_complete",
                "batch_size": len(events),
                "processed": processed,
                "failures": failures,
                "duration_ms": batch_duration_ms,
                "last_event_id": state.last_event_id,
                "events_per_minute": state.events_per_minute(),
            })

            # Short sleep between non-empty batches
            try:
                await asyncio.wait_for(
                    asyncio.shield(shutdown_event.wait()),
                    timeout=POLL_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

        except asyncio.CancelledError:
            logger.info({"event": "worker_loop_cancelled"})
            break
        except Exception as e:
            logger.error({
                "event": "worker_loop_error",
                "error": str(e),
                "stack_trace": traceback.format_exc()[:1000],
            })
            # Back off before retrying the loop
            try:
                await asyncio.wait_for(
                    asyncio.shield(shutdown_event.wait()),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                pass

    logger.info({"event": "worker_loop_exited"})
