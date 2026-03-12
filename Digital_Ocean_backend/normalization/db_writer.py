#!/usr/bin/env python3
"""
Normalization Worker — Database Writer

Owns the connection pool lifecycle and all SQL write functions.
Every function accepts `conn: asyncpg.Connection` as its first argument —
they do NOT acquire their own connections. Connection lifecycle is managed
entirely by worker.py.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from config import DATABASE_URL, DB_POOL_MIN, DB_POOL_MAX

import asyncpg

from models import (
    AttachmentRow,
    ChatRow,
    ChatStateRow,
    ContactRow,
    MessageRow,
    StatusRow,
)

logger = logging.getLogger("normalization")

_pool: asyncpg.Pool | None = None


# ═══════════════════════════════════════════════════════════════════════
# Pool Lifecycle
# ═══════════════════════════════════════════════════════════════════════

async def init_pool() -> asyncpg.Pool:
    """
    Create the asyncpg connection pool.

    Retries up to 5 times on startup to handle transient DB connectivity.
    Crashes if DB is unreachable after all attempts.
    """
    global _pool

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required")

    min_size = DB_POOL_MIN
    max_size = DB_POOL_MAX

    import asyncio
    for attempt in range(5):
        try:
            _pool = await asyncpg.create_pool(
                dsn=DATABASE_URL,
                min_size=min_size,
                max_size=max_size,
            )
            logger.info({
                "event": "pool_created",
                "min_size": min_size,
                "max_size": max_size,
            })
            return _pool
        except (asyncpg.PostgresConnectionError, OSError, ConnectionRefusedError) as e:
            delay = 2 ** attempt
            if attempt < 4:
                logger.warning({
                    "event": "pool_init_retry",
                    "attempt": attempt + 1,
                    "delay": delay,
                    "error": str(e),
                })
                await asyncio.sleep(delay)
            else:
                logger.error({
                    "event": "pool_init_failed",
                    "error": str(e),
                    "message": "Database unreachable after 5 attempts — crashing",
                })
                raise



# ═══════════════════════════════════════════════════════════════════════
# Contact Upsert
# ═══════════════════════════════════════════════════════════════════════

async def upsert_contact(conn: asyncpg.Connection, row: ContactRow) -> int:
    """
    Upsert a contact by uuid. Preserves real contact names — never
    overwrites a real name with NULL (from bot-authored messages).

    Returns the contact's internal id.
    """
    result = await conn.fetchrow("""
        INSERT INTO contacts (
            uuid, number_id, whatsapp_id, whatsapp_profile_name,
            inserted_at, updated_at
        )
        VALUES ($1, $2, $3, $4, now(), now())
        ON CONFLICT (uuid) DO UPDATE SET
            whatsapp_id = EXCLUDED.whatsapp_id,
            whatsapp_profile_name = CASE
                WHEN EXCLUDED.whatsapp_profile_name IS NOT NULL
                THEN EXCLUDED.whatsapp_profile_name
                ELSE contacts.whatsapp_profile_name
            END,
            updated_at = now()
        RETURNING id
    """,
        row.uuid,
        row.number_id,
        row.whatsapp_id,
        row.whatsapp_profile_name,
    )
    return result["id"]


# ═══════════════════════════════════════════════════════════════════════
# Chat Upsert
# ═══════════════════════════════════════════════════════════════════════

async def upsert_chat(conn: asyncpg.Connection, row: ChatRow) -> int:
    """
    Upsert a chat using uuid as the conflict target.

    Uses the chats_uuid_key unique constraint. One contact can have
    multiple chats — (number_id, contact_id) must NOT be used.

    Returns the chat's internal id.
    """
    result = await conn.fetchrow("""
        INSERT INTO chats (
            uuid, number_id, contact_id, state, state_reason, owner,
            assigned_to_uuid, unread_count,
            inbound_timestamp, outbound_timestamp,
            inserted_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            CASE WHEN $9 = 'inbound' THEN $10::timestamptz ELSE NULL END,
            CASE WHEN $9 = 'outbound' THEN $10::timestamptz ELSE NULL END,
            $11, now()
        )
        ON CONFLICT (uuid) DO UPDATE SET
            state            = EXCLUDED.state,
            state_reason     = EXCLUDED.state_reason,
            unread_count     = EXCLUDED.unread_count,
            assigned_to_uuid = EXCLUDED.assigned_to_uuid,
            inbound_timestamp = COALESCE(
                GREATEST(chats.inbound_timestamp, EXCLUDED.inbound_timestamp),
                EXCLUDED.inbound_timestamp
            ),
            outbound_timestamp = COALESCE(
                GREATEST(chats.outbound_timestamp, EXCLUDED.outbound_timestamp),
                EXCLUDED.outbound_timestamp
            ),
            updated_at = now()
        RETURNING id
    """,
        row.uuid,                   # $1
        row.number_id,              # $2
        row.contact_id,             # $3
        row.state,                  # $4
        row.state_reason,           # $5
        row.owner,                  # $6
        row.assigned_to_uuid,       # $7
        row.unread_count,           # $8
        row.direction,              # $9
        row.event_timestamp,        # $10
        row.chat_inserted_at,       # $11
    )
    return result["id"]


# ═══════════════════════════════════════════════════════════════════════
# Message Insert
# ═══════════════════════════════════════════════════════════════════════

async def insert_message(conn: asyncpg.Connection, row: MessageRow) -> int:
    """
    Insert a message with ON CONFLICT (external_id) DO NOTHING.

    If the message already exists (duplicate), fetches and returns
    the existing message id. Always returns a valid message_id.
    """
    # Serialize jsonb fields
    metadata_json = json.dumps(row.message_metadata) if row.message_metadata else None
    media_json = json.dumps(row.media_object) if row.media_object else None

    result = await conn.fetchrow("""
        INSERT INTO messages (
            uuid, number_id, chat_id,
            message_type, author, author_type, from_addr,
            content, rendered_content, direction,
            external_id, external_timestamp,
            last_status, last_status_timestamp,
            has_media, is_deleted,
            faq_uuid, card_uuid,
            message_metadata, media_object,
            campaign_id,
            inserted_at, updated_at
        )
        VALUES (
            $1,  $2,  $3,
            $4::message_type_enum, $5, $6, $7,
            $8,  $9,  $10::message_direction,
            $11, $12,
            $13, $14,
            $15, false,
            $16, $17,
            $18::jsonb, $19::jsonb,
            $20,
            now(), now()
        )
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
    """,
        row.uuid,                   # $1
        row.number_id,              # $2
        row.chat_id,                # $3
        row.message_type,           # $4
        row.author,                 # $5
        row.author_type,            # $6
        row.from_addr,              # $7
        row.content,                # $8
        row.rendered_content,       # $9
        row.direction,              # $10
        row.external_id,            # $11
        row.external_timestamp,     # $12
        row.last_status,            # $13
        row.last_status_timestamp,  # $14
        row.has_media,              # $15
        row.faq_uuid,               # $16
        row.card_uuid,              # $17
        metadata_json,              # $18
        media_json,                 # $19
        row.campaign_id,            # $20
    )

    if result:
        return result["id"]

    # Duplicate — fetch existing id
    existing = await conn.fetchrow(
        "SELECT id FROM messages WHERE external_id = $1",
        row.external_id,
    )
    if existing:
        return existing["id"]

    raise RuntimeError(f"Message insert returned nothing and lookup failed for external_id={row.external_id}")


# ═══════════════════════════════════════════════════════════════════════
# Status Upsert
# ═══════════════════════════════════════════════════════════════════════

async def upsert_status(conn: asyncpg.Connection, row: StatusRow) -> None:
    """
    Upsert a status row with conflict on (message_id, status).

    On conflict, update the status_timestamp if newer.
    """
    await conn.execute("""
        INSERT INTO statuses (
            message_id, message_uuid, number_id, status,
            status_timestamp, raw_body, inserted_at, updated_at
        )
        VALUES ($1, $2, $3, $4::message_status_enum, $5, $6, now(), now())
        ON CONFLICT (message_id, status) DO UPDATE SET
            status_timestamp = EXCLUDED.status_timestamp,
            updated_at = now()
    """,
        row.message_id,
        row.message_uuid,
        row.number_id,
        row.status,
        row.status_timestamp,
        row.raw_body,
    )


# ═══════════════════════════════════════════════════════════════════════
# Chat State Upsert
# ═══════════════════════════════════════════════════════════════════════

async def upsert_chat_state(conn: asyncpg.Connection, row: ChatStateRow) -> None:
    """
    Upsert chat_state with GREATEST on last_message_at, last_inbound_at,
    and last_outbound_at to prevent regression from out-of-order events.
    """
    await conn.execute("""
        INSERT INTO chat_state (
            chat_id, last_message_at, last_inbound_at, last_outbound_at,
            unread_count, current_status, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (chat_id) DO UPDATE SET
            last_message_at = GREATEST(chat_state.last_message_at, EXCLUDED.last_message_at),
            last_inbound_at = CASE
                WHEN EXCLUDED.last_inbound_at IS NOT NULL
                THEN GREATEST(chat_state.last_inbound_at, EXCLUDED.last_inbound_at)
                ELSE chat_state.last_inbound_at
            END,
            last_outbound_at = CASE
                WHEN EXCLUDED.last_outbound_at IS NOT NULL
                THEN GREATEST(chat_state.last_outbound_at, EXCLUDED.last_outbound_at)
                ELSE chat_state.last_outbound_at
            END,
            unread_count = EXCLUDED.unread_count,
            current_status = EXCLUDED.current_status,
            updated_at = now()
    """,
        row.chat_id,
        row.last_message_at,
        row.last_inbound_at,
        row.last_outbound_at,
        row.unread_count,
        row.current_status,
    )


# ═══════════════════════════════════════════════════════════════════════
# Attachment Insert
# ═══════════════════════════════════════════════════════════════════════

async def insert_attachment(conn: asyncpg.Connection, row: AttachmentRow) -> None:
    """
    Insert a message attachment with ON CONFLICT (message_id) DO NOTHING.

    Idempotent — duplicates are silently skipped.
    """
    await conn.execute("""
        INSERT INTO message_attachments (
            message_id, chat_id, number_id, filename, caption,
            inserted_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, now(), now())
        ON CONFLICT (message_id) DO NOTHING
    """,
        row.message_id,
        row.chat_id,
        row.number_id,
        row.filename,
        row.caption,
    )


# ═══════════════════════════════════════════════════════════════════════
# Update Chat Last Message
# ═══════════════════════════════════════════════════════════════════════

async def update_last_message(
    conn: asyncpg.Connection,
    chat_id: int,
    message_id: int,
) -> None:
    """
    Update chats.last_message_id only if the new message is chronologically
    newer than the current last_message_id. Handles out-of-order delivery.
    """
    await conn.execute("""
        UPDATE chats
        SET last_message_id = $1,
            updated_at = now()
        WHERE id = $2
          AND (
            last_message_id IS NULL
            OR (
                SELECT external_timestamp FROM messages WHERE id = chats.last_message_id
            ) <= (
                SELECT external_timestamp FROM messages WHERE id = $1
            )
          )
    """,
        message_id,
        chat_id,
    )


# ═══════════════════════════════════════════════════════════════════════
# Status Event Processing (standalone status events)
# ═══════════════════════════════════════════════════════════════════════

async def process_status_event_db(
    conn: asyncpg.Connection,
    webhook_event_id: int,
    message_external_id: str,
    status: str,
    status_timestamp: Optional[object],
    number_id: int,
    payload: dict,
) -> None:
    """
    Process a standalone status event.

    Looks up the message by external_id, then upserts a status row.
    Raises ValueError if the referenced message doesn't exist yet.

    payload is passed as a raw dict — asyncpg handles jsonb serialization.
    """
    from parsers import parse_timestamp as _pts

    row = await conn.fetchrow(
        "SELECT id FROM messages WHERE external_id = $1",
        message_external_id,
    )

    if not row:
        raise ValueError(f"status_message_not_found: {message_external_id}")

    message_id = row["id"]

    ts = _pts(status_timestamp) if not isinstance(status_timestamp, type(None)) else status_timestamp

    await conn.execute("""
        INSERT INTO statuses (
            message_id, message_uuid, number_id, status,
            status_timestamp, raw_body, inserted_at, updated_at
        )
        VALUES ($1, $2, $3, $4::message_status_enum, $5, $6, now(), now())
        ON CONFLICT (message_id, status) DO UPDATE SET
            status_timestamp = EXCLUDED.status_timestamp,
            updated_at = now()
    """,
        message_id,
        message_external_id,
        number_id,
        status,
        ts,
        payload,  # pass dict directly — asyncpg handles jsonb serialization
    )


# ═══════════════════════════════════════════════════════════════════════
# Failure Recording
# ═══════════════════════════════════════════════════════════════════════

async def insert_failure(
    conn: asyncpg.Connection,
    webhook_event_id: int,
    payload: str | None,
    error_message: str,
    stack_trace: str,
) -> None:
    """
    Record a normalization failure for manual review.

    Uses a separate connection to avoid being rolled back with the
    failed event's transaction.
    """
    # Truncate stack trace to 2000 chars
    truncated_trace = stack_trace[:2000] if stack_trace else None

    await conn.execute("""
        INSERT INTO normalization_failures (
            webhook_event_id, error_message, stack_trace, payload, created_at
        )
        VALUES ($1, $2, $3, $4::jsonb, now())
    """,
        webhook_event_id,
        error_message[:500] if error_message else None,
        truncated_trace,
        payload,
    )
