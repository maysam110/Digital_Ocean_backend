#!/usr/bin/env python3
"""
Normalization Worker — State Manager

Owns the normalization checkpoint table and schema setup.
Checkpoint tracking uses normalization_state.last_event_id
to track which webhook_events rows have been processed.
"""

from __future__ import annotations

import asyncio
import logging

import asyncpg

from config import RETRY_DELAYS

logger = logging.getLogger("normalization")


# ═══════════════════════════════════════════════════════════════════════
# Schema Setup (run once at startup)
# ═══════════════════════════════════════════════════════════════════════

async def setup_schema(pool: asyncpg.Pool) -> None:
    """
    Create the normalization infrastructure tables and indexes.

    Runs at startup — all operations are idempotent (IF NOT EXISTS).
    """
    async with pool.acquire() as conn:
        # 1. Normalization state (checkpoint) table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS normalization_state (
                id integer PRIMARY KEY,
                last_event_id bigint DEFAULT 0,
                updated_at timestamptz DEFAULT now()
            )
        """)
        await conn.execute("""
            INSERT INTO normalization_state (id, last_event_id)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        """)

        # 2. Normalization failures table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS normalization_failures (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                webhook_event_id bigint,
                error_message text,
                stack_trace text,
                payload jsonb,
                created_at timestamptz DEFAULT now()
            )
        """)

        # 3. Partial index on webhook_events for efficient polling
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_webhook_events_normalizer
            ON webhook_events (id)
            WHERE provider = 'turn_io'
        """)

    logger.info({
        "event": "schema_setup_complete",
        "message": "normalization_state, normalization_failures, and index created/verified",
    })


# ═══════════════════════════════════════════════════════════════════════
# Checkpoint Read
# ═══════════════════════════════════════════════════════════════════════

async def get_checkpoint(conn: asyncpg.Connection) -> int:
    """
    Read the current checkpoint (last_event_id) from normalization_state.

    Returns 0 if no checkpoint exists.
    """
    row = await conn.fetchrow(
        "SELECT last_event_id FROM normalization_state WHERE id = 1"
    )
    if row and row["last_event_id"] is not None:
        return row["last_event_id"]
    return 0


# ═══════════════════════════════════════════════════════════════════════
# Checkpoint Write (with retry)
# ═══════════════════════════════════════════════════════════════════════

async def update_checkpoint(pool: asyncpg.Pool, last_id: int) -> None:
    """
    Advance the checkpoint to last_id.

    Uses retry-with-backoff — if all retries fail, raises to crash
    the worker. On restart, the worker will re-process the last batch
    (idempotent writes make this safe).
    """
    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE normalization_state
                    SET last_event_id = $1,
                        updated_at = now()
                    WHERE id = 1
                """, last_id)

            logger.info({
                "event": "checkpoint_updated",
                "last_event_id": last_id,
            })
            return

        except (asyncpg.PostgresConnectionError, OSError) as e:
            if attempt == len(RETRY_DELAYS) - 1:
                logger.error({
                    "event": "checkpoint_update_failed",
                    "error": str(e),
                    "message": "Checkpoint update failed after all retries — crashing",
                })
                raise
            logger.warning({
                "event": "db_retry",
                "context": "checkpoint_update",
                "attempt": attempt + 1,
                "delay": delay,
                "error": str(e),
            })
            await asyncio.sleep(delay)
