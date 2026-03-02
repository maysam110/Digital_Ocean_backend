#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ContactWorker — Continuous Turn.io Contact Ingestion

Refactored from ingest_contacts.py (ContactIngester class).
All internal ingestion logic is preserved EXACTLY — this is a structural
refactor only.

Changes from original:
  - REMOVED: File-based checkpoint (last_contact_fetch.txt)
  - ADDED:   DB-backed checkpoint using ingestion_state table (id=2)
  - REMOVED: CLI entry point, argument parser
  - ADDED:   Continuous loop with configurable interval
  - Structure: class ContactWorker with async start()

Preserved:
  - Parallel chunk backfill via get_contacts_parallel()
  - UPSERT: ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload, processed = FALSE
  - Producer/consumer batching with NUM_CONSUMERS consumers
  - All retry logic, cursor expiry handling
"""

import asyncio
import datetime
import json
import logging
from typing import Optional

import asyncpg

from app.ingestion.base import (
    TurnContactClient,
    BATCH_SIZE,
    NUM_CHUNKS,
    NUM_CONSUMERS,
    CONSUMER_QUEUE_SIZE,
    ERROR_BACKOFF_S,
    CONTACT_INTERVAL_S,
    STARTUP_FROM_DATE,
    log,
)

# Fixed start date for the startup backfill.
# On every service start, contacts are fetched from this date to NOW.
# Use a child logger for contacts
log = logging.getLogger("ingestion.contacts")


# Overlap seconds for contact checkpoint (same pattern as messages)
CONTACT_OVERLAP_SECONDS = 2


class ContactWorker:
    """
    Continuous contact ingestion worker.

    Runs in a loop:
        1. Read DB checkpoint (updated_at from last contact seen)
        2. Apply 2-second overlap buffer
        3. Fetch contacts from checkpoint → NOW via parallel chunks
        4. UPSERT into webhook_events
        5. Update checkpoint to MAX(updated_at) of contacts seen
        6. Sleep for CONTACT_INTERVAL_S (default: 60 minutes)
        7. Repeat

    Safety invariants:
        • UPSERT: ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload, processed = FALSE
        • Checkpoint only advances on actual data
        • Overlap buffer prevents boundary loss
        • Parallel chunking preserved
        • Idempotent: safe to re-run
    """

    def __init__(self, pool: asyncpg.Pool, client: TurnContactClient) -> None:
        self.pool = pool
        self.client = client
        self.shutdown_event = asyncio.Event()

    # ══════════════════════════════════════════════════════════════════════
    # DB-backed checkpoint (id=2 in ingestion_state)
    # ══════════════════════════════════════════════════════════════════════

    async def ensure_checkpoint(self) -> None:
        """Ensure ingestion_state row id=2 exists for contacts."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ingestion_state (id, last_external_timestamp)
                VALUES (2, $1::timestamptz)
                ON CONFLICT (id) DO NOTHING
            """, STARTUP_FROM_DATE)
        log.info("✓ ingestion_state row ready (contacts, id=2)")

    async def get_checkpoint(self) -> datetime.datetime:
        """Return the current contact checkpoint timestamp."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_external_timestamp FROM ingestion_state WHERE id = 2"
            )
            if row and row["last_external_timestamp"]:
                return row["last_external_timestamp"]
            return datetime.datetime.fromisoformat(STARTUP_FROM_DATE.replace("Z", "+00:00"))

    async def update_checkpoint(self, max_ts: datetime.datetime) -> None:
        """
        Advance the contact checkpoint to *max_ts*.

        Guards (same pattern as messages):
            - max_ts must be timezone-aware
            - max_ts must be <= NOW()
            - Only advances forward
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        if max_ts.tzinfo is None:
            log.error(f"⛔ Contact checkpoint rejected: naive timestamp: {max_ts}")
            return

        if max_ts > now_utc:
            log.warning(f"⚠️ Contact checkpoint clamped from future {max_ts} to NOW")
            max_ts = now_utc

        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE ingestion_state
                SET last_external_timestamp = $1,
                    updated_at              = NOW()
                WHERE id = 2
                  AND (last_external_timestamp IS NULL
                       OR last_external_timestamp < $1)
            """, max_ts)

    # ══════════════════════════════════════════════════════════════════════
    # Core ingestion (preserved from ContactIngester.run_raw_ingestion)
    # ══════════════════════════════════════════════════════════════════════

    async def run_raw_ingestion(
        self, from_date: str, until_date: str
    ) -> Optional[datetime.datetime]:
        """
        Fetch contacts and UPSERT into webhook_events.

        Preserved from ingest_contacts.py:
          - Parallel chunk backfill via get_contacts_parallel()
          - Producer/consumer batching
          - UPSERT with payload overwrite and processed = FALSE

        Returns:
            MAX(updated_at) of contacts seen, or None if no contacts.
        """
        log.info("=" * 70)
        log.info("CONTACT INGESTION CYCLE")
        log.info(f"  From: {from_date}")
        log.info(f"  Until: {until_date}")
        log.info("=" * 70)

        ingest_queue: asyncio.Queue = asyncio.Queue(maxsize=CONSUMER_QUEUE_SIZE)
        producers_done = asyncio.Event()
        stats = {"contacts": 0, "batches": 0}
        max_contact_ts: Optional[datetime.datetime] = None

        async def batch_consumer(consumer_id: int):
            """Consumer: pull from queue and batch insert into DB."""
            log.info(f"  ⚡ Consumer-{consumer_id} started")
            batch = []
            async with self.pool.acquire() as conn:
                while True:
                    try:
                        item = await asyncio.wait_for(ingest_queue.get(), timeout=1.0)
                        batch.append(item)
                        if len(batch) >= BATCH_SIZE:
                            await self._flush_batch(conn, batch, stats, consumer_id)
                            batch = []
                    except asyncio.TimeoutError:
                        if batch:
                            await self._flush_batch(conn, batch, stats, consumer_id)
                            batch = []
                        if producers_done.is_set() and ingest_queue.empty():
                            break
                if batch:
                    await self._flush_batch(conn, batch, stats, consumer_id)
            log.info(f"  🏁 Consumer-{consumer_id} finished")

        async def contact_producer():
            """Producer: fetch contacts and enqueue for batching."""
            nonlocal max_contact_ts
            count = 0

            try:
                async for contact in self.client.get_contacts_parallel(
                    from_date, until_date, num_chunks=NUM_CHUNKS
                ):
                    ext_id = contact.get("id")
                    if ext_id is None:
                        continue
                    ext_id = str(ext_id)  # Turn.io returns int IDs, DB expects str

                    # Track MAX updated_at for checkpoint
                    ts_raw = contact.get("updated_at") or contact.get("inserted_at")
                    if ts_raw:
                        try:
                            ts_str = str(ts_raw).strip()
                            if ts_str.endswith("Z"):
                                ts_str = ts_str[:-1] + "+00:00"
                            ts_dt = datetime.datetime.fromisoformat(ts_str)
                            if ts_dt.tzinfo is None:
                                ts_dt = ts_dt.replace(tzinfo=datetime.timezone.utc)
                            if max_contact_ts is None or ts_dt > max_contact_ts:
                                max_contact_ts = ts_dt
                        except (ValueError, TypeError) as exc:
                            log.debug(f"Skipping unparsable contact timestamp {ts_raw}: {exc}")

                    if ingest_queue.full():
                        log.warning(f"  ⚠️ [CONTACTS] Ingest queue full, waiting for DB flush...")
                    await ingest_queue.put(
                        ("turn_io", "contact", ext_id, contact)
                    )
                    count += 1

                    if count % BATCH_SIZE == 0:
                        log.info(
                            f"  → {count} contacts fetched, "
                            f"{stats['batches']} batches written..."
                        )

                stats["contacts"] = count
                log.info(f"✓ Contact fetch complete: {count} total")
            finally:
                producers_done.set()

        log.info("\n📥 Starting parallel fetch and batch insert (contacts)...")
        log.info(f"   Batch size: {BATCH_SIZE} contacts per transaction")
        log.info(f"   Queue size: {CONSUMER_QUEUE_SIZE} contacts (memory bounded)")

        consumer_tasks = [
            asyncio.create_task(batch_consumer(i))
            for i in range(NUM_CONSUMERS)
        ]
        producer_task = asyncio.create_task(contact_producer())
        pipeline_tasks = [producer_task, *consumer_tasks]

        try:
            await asyncio.gather(*pipeline_tasks)
        except (Exception, asyncio.CancelledError) as exc:
            log.error(f"❌ Contact ingestion pipeline error or cancellation: {exc}", exc_info=True)
            producers_done.set()
            for task in pipeline_tasks:
                task.cancel()
            await asyncio.gather(*pipeline_tasks, return_exceptions=True)
            raise

        log.info("\n" + "=" * 70)
        log.info(f"✅ CONTACT INGESTION CYCLE COMPLETE")
        log.info(f"   Contacts fetched: {stats['contacts']}")
        log.info(f"   Total batches written: {stats['batches']}")
        log.info("=" * 70)

        return max_contact_ts

    async def _flush_batch(self, conn, batch, stats, consumer_id: int = -1):
        """Flush a batch of contact events to database using executemany.

        UPSERT: ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload, processed = FALSE
        """
        if not batch:
            return

        tag = f"Consumer-{consumer_id}" if consumer_id >= 0 else "flush"

        try:
            records = [
                (provider, event_type, ext_id, json.dumps(payload))
                for provider, event_type, ext_id, payload in batch
            ]

            log.debug(f"  → [{tag}] Flushing batch {stats['batches'] + 1} ({len(batch)} items)...")
            await conn.executemany(
                """
                INSERT INTO webhook_events (provider, event_type, external_event_id, payload, processed)
                VALUES ($1, $2, $3, $4::jsonb, FALSE)
                ON CONFLICT (provider, external_event_id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    processed = FALSE
                """,
                records,
            )

            stats["batches"] += 1
            log.info(
                f"  ✓ [{tag}] Batch {stats['batches']}: {len(batch)} contact events written"
            )

        except Exception as e:
            log.error(
                f"[{tag}] Failed to flush batch of {len(batch)} contact events: {e}"
            )
            raise

    # ══════════════════════════════════════════════════════════════════════
    # Main loop
    # ══════════════════════════════════════════════════════════════════════

    async def start(self) -> None:
        """
        Contact ingestion lifecycle:

            PHASE 1 — Startup backfill (every restart):
                from_ts = STARTUP_FROM_DATE (default 2024-01-01)
                until_ts = NOW
                Run one full sweep via parallel chunks.
                Update checkpoint to MAX(updated_at) seen.
                Purpose: refresh mutable contact fields (name, profile, etc.)
                         that may have changed since last run.

            PHASE 2 — Normal incremental loop (every 60 min):
                from_ts = checkpoint − 2 s  (overlap window)
                until_ts = NOW
                UPSERT into webhook_events.
                Update checkpoint.
                Sleep CONTACT_INTERVAL_S.
                Repeat until shutdown.

        The startup backfill is safe to run on every restart because
        ContactWorker uses ON CONFLICT DO UPDATE — re-ingesting the same
        contacts simply refreshes their payloads.
        """
        log.info("=" * 70)
        log.info("🚀 CONTACT WORKER STARTING")
        log.info("=" * 70)

        await self.ensure_checkpoint()

        # ── PHASE 1: Startup backfill (fixed date → NOW) ─────────────────────
        log.info(
            f"🔄 [STARTUP BACKFILL] Fetching all contacts from "
            f"{STARTUP_FROM_DATE} to NOW..."
        )
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            # from_date is normalized during module initialization
            backfill_from_str = STARTUP_FROM_DATE
            backfill_until_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

            log.info("-" * 40)
            log.info("CONTACT STARTUP BACKFILL")
            log.info(f"Target Start Date:  {STARTUP_FROM_DATE}")
            log.info(f"Effective Range:    {backfill_from_str} → {backfill_until_str}")
            log.info("-" * 40)

            max_ts = await self.run_raw_ingestion(backfill_from_str, backfill_until_str)

            if max_ts is not None:
                await self.update_checkpoint(max_ts)
                log.info(
                    f"✅ [STARTUP BACKFILL] Complete. "
                    f"Checkpoint advanced to {max_ts.isoformat()}"
                )
            else:
                log.info(
                    "✅ [STARTUP BACKFILL] Complete. No contacts found — checkpoint unchanged."
                )

        except asyncio.CancelledError:
            log.info("Contact worker cancelled during startup backfill")
            return
        except Exception as exc:
            log.error(
                f"❌ [STARTUP BACKFILL] Failed: {exc}. "
                f"Proceeding to normal loop from last checkpoint.",
                exc_info=True,
            )

        # ── PHASE 2: Normal incremental loop ─────────────────────────────────
        log.info("🔁 Entering normal incremental contact loop...")

        while not self.shutdown_event.is_set():
            try:
                checkpoint = await self.get_checkpoint()
                now_utc = datetime.datetime.now(datetime.timezone.utc)

                from_ts = checkpoint - datetime.timedelta(seconds=CONTACT_OVERLAP_SECONDS)
                from_date = from_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                until_date = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                log.info(
                    f"[CONTACTS] checkpoint={checkpoint.isoformat()} "
                    f"range={from_date}→{until_date}"
                )

                max_ts = await self.run_raw_ingestion(from_date, until_date)

                if max_ts is not None:
                    await self.update_checkpoint(max_ts)
                    log.info(f"[CONTACTS] checkpoint advanced to {max_ts.isoformat()}")
                else:
                    log.info("[CONTACTS] no new contacts — checkpoint unchanged")

                # Sleep until next cycle (interruptible every 10s for clean shutdown)
                log.info(
                    f"💤 Contact worker sleeping for {CONTACT_INTERVAL_S // 60} minutes..."
                )
                sleep_remaining = CONTACT_INTERVAL_S
                while sleep_remaining > 0 and not self.shutdown_event.is_set():
                    await asyncio.sleep(min(sleep_remaining, 10))
                    sleep_remaining -= 10

            except asyncio.CancelledError:
                log.info("Contact worker cancelled")
                break
            except Exception as exc:
                log.error(f"❌ Contact ingestion cycle failed: {exc}", exc_info=True)
                log.warning(f"⚠️  Retrying in {ERROR_BACKOFF_S} seconds...")
                sleep_remaining = ERROR_BACKOFF_S
                while sleep_remaining > 0 and not self.shutdown_event.is_set():
                    await asyncio.sleep(min(sleep_remaining, 10))
                    sleep_remaining -= 10

        log.info("🛑 Contact worker stopped")
