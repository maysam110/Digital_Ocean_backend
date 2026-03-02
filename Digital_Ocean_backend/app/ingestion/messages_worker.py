#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MessageWorker — Continuous Turn.io Message Ingestion

Refactored from continuous_ingest.py (ContinuousIngestor class).
All internal ingestion logic is preserved EXACTLY — this is a structural
refactor only. The CLI main() and restart loop have been removed;
lifecycle is managed by IngestionManager + FastAPI container restart.

Four concurrent tasks:
    1. incremental_loop    — every 30 s, fetch checkpoint−5s → NOW
    2. recovery_scheduler  — once/day, fetch NOW−3d → NOW
    3. audit_scheduler     — once/week (Monday), fetch NOW−30d → NOW
    4. metrics_server      — HTTP :9102/metrics (Prometheus-compatible)
"""

import asyncio
import datetime
import json
import logging
from typing import Optional, Tuple

import asyncpg

from app.ingestion.base import (
    TurnClient,
    extract_message_timestamp,
    NUM_CONSUMERS,
    log,
)

# Use a child logger for messages
log = logging.getLogger("ingestion.messages")


class MessageWorker:
    """
    Enterprise-grade continuous message ingestion worker.

    Critical invariants (ALL preserved from continuous_ingest.py):
        • Checkpoint is ALWAYS derived from MAX(timestamp) of durably inserted messages.
        • Checkpoint is NEVER set to NOW() or any wall-clock value.
        • Every fetch overlaps by 5 seconds to guard against boundary races.
        • webhook_events UNIQUE(provider, external_event_id) prevents dupes.
        • If no new messages are returned, checkpoint does NOT move forward.
        • Scheduler state persists across restarts via DB table.
        • Minimum 1s advance prevents clock-jitter checkpoint thrashing.
        • Fetch timeout prevents infinite API hangs.
        • Lag > 900s triggers automatic self-healing recovery.
    """

    # ── constants ──────────────────────────────────────────────────────────
    BATCH_SIZE: int = 1000
    QUEUE_SIZE: int = 10_000
    OVERLAP_SECONDS: int = 5
    INCREMENTAL_INTERVAL: int = 30
    ERROR_BACKOFF: int = 10
    RECOVERY_CHECK_INTERVAL: int = 600
    AUDIT_CHECK_INTERVAL: int = 3600
    FETCH_TIMEOUT: int = 7200              # 2 hours max per fetch (large catch-ups need time)
    MIN_CHECKPOINT_ADVANCE: int = 1        # minimum seconds to advance
    LAG_RECOVERY_THRESHOLD: int = 300      # aggressive catch-up at 5 min
    LAG_WATCHDOG_THRESHOLD: int = 900      # emergency recovery at 15 min
    QUEUE_BACKPRESSURE_PCT: float = 0.8    # warn at 80% queue capacity
    METRICS_PORT: int = 9102
    DEFAULT_BOOTSTRAP_DATE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

    def __init__(self, pool: asyncpg.Pool, client: TurnClient) -> None:
        self.pool = pool
        self.client = client
        self.shutdown_event = asyncio.Event()

        # Serialize ALL API fetch_and_insert calls so that incremental,
        # recovery and audit never hit the Turn.io API concurrently.
        self.api_lock = asyncio.Lock()

        # Statistics (lifetime of process)
        self.stats = {
            "total_scanned": 0,
            "total_inserted": 0,
            "incremental_runs": 0,
            "recovery_runs": 0,
            "audit_runs": 0,
        }

        # Current lag (updated every cycle, exposed via metrics)
        self._current_lag: int = 0

        # Scheduler guards — loaded from DB on startup
        self._last_recovery_date: Optional[datetime.date] = None
        self._last_audit_date: Optional[datetime.date] = None

        # Metrics server handle
        self._metrics_server: Optional[asyncio.AbstractServer] = None

    # ══════════════════════════════════════════════════════════════════════
    # Checkpoint persistence
    # ══════════════════════════════════════════════════════════════════════

    async def ensure_checkpoint_row(self) -> None:
        """
        Ensure `ingestion_state.id=1` exists and bootstrap safely.

        Bootstrap order (first run only):
          1) existing `ingestion_state.id=1`
          2) MAX(received_at) from webhook_events where event_type='message' minus 1 day
          3) fixed fallback 2024-01-01T00:00:00Z
        """
        async with self.pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT last_external_timestamp FROM ingestion_state WHERE id = 1"
            )
            if existing is not None:
                log.info(f"✓ Existing message checkpoint found: {existing.isoformat()}")
                return

            max_received_at = await conn.fetchval(
                """
                SELECT MAX(received_at)
                FROM webhook_events
                WHERE event_type = 'message'
                """
            )
            if max_received_at is not None:
                bootstrap_ts = max_received_at - datetime.timedelta(days=1)
                log.info(
                    f"✓ Bootstrapping message checkpoint from webhook_events: "
                    f"{bootstrap_ts.isoformat()} (max(received_at)-1d)"
                )
            else:
                bootstrap_ts = self.DEFAULT_BOOTSTRAP_DATE
                log.info(
                    f"✓ Bootstrapping message checkpoint from fixed fallback: "
                    f"{bootstrap_ts.isoformat()}"
                )

            await conn.execute(
                """
                INSERT INTO ingestion_state (id, last_external_timestamp, updated_at)
                VALUES (1, $1, NOW())
                ON CONFLICT (id) DO UPDATE
                SET last_external_timestamp = COALESCE(
                    ingestion_state.last_external_timestamp,
                    EXCLUDED.last_external_timestamp
                ),
                    updated_at = NOW()
                """,
                bootstrap_ts,
            )

    # ══════════════════════════════════════════════════════════════════════
    # Persistent scheduler state
    # ══════════════════════════════════════════════════════════════════════

    async def ensure_scheduler_state_row(self) -> None:
        """
        Ensure scheduler state row exists (no schema mutation).
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ingestion_scheduler_state (id)
                VALUES (1)
                ON CONFLICT (id) DO NOTHING
            """)
        log.info("✓ ingestion_scheduler_state row ensured")

    async def load_scheduler_state(self) -> None:
        """Load persisted scheduler dates into memory on startup."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_recovery_date, last_audit_date "
                "FROM ingestion_scheduler_state WHERE id = 1"
            )
            if row:
                self._last_recovery_date = row["last_recovery_date"]
                self._last_audit_date = row["last_audit_date"]
                log.info(
                    f"✓ Scheduler state loaded: "
                    f"last_recovery={self._last_recovery_date}, "
                    f"last_audit={self._last_audit_date}"
                )

    async def persist_scheduler_state(self) -> None:
        """Write current scheduler dates to DB."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE ingestion_scheduler_state
                SET last_recovery_date = $1,
                    last_audit_date    = $2,
                    updated_at         = NOW()
                WHERE id = 1
            """, self._last_recovery_date, self._last_audit_date)

    # ══════════════════════════════════════════════════════════════════════
    # Checkpoint read / write
    # ══════════════════════════════════════════════════════════════════════

    async def get_checkpoint(self) -> datetime.datetime:
        """Return the current checkpoint timestamp."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_external_timestamp FROM ingestion_state WHERE id = 1"
            )
            if row and row["last_external_timestamp"]:
                return row["last_external_timestamp"]
            return self.DEFAULT_BOOTSTRAP_DATE

    async def update_checkpoint(self, max_ts: datetime.datetime) -> None:
        """
        Advance the checkpoint to *max_ts* with full defensive validation.

        Guards:
            - max_ts must be timezone-aware
            - max_ts must be <= NOW()
            - max_ts must not be > 30 days behind current checkpoint
            - max_ts must advance checkpoint by >= 1 second (jitter guard)
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Validation: must be tz-aware
        if max_ts.tzinfo is None:
            log.error(f"⛔ Checkpoint rejected: max_ts is naive (no timezone): {max_ts}")
            return

        # Validation: must not be in the future
        if max_ts > now_utc:
            log.error(f"⛔ Checkpoint rejected: max_ts is in the future: {max_ts}")
            return

        # Validation: sanity check
        checkpoint = await self.get_checkpoint()
        if max_ts < checkpoint - datetime.timedelta(days=30):
            log.error(
                f"⛔ Checkpoint rejected: max_ts ({max_ts.isoformat()}) is >30 days "
                f"behind current checkpoint ({checkpoint.isoformat()})"
            )
            return

        # Minimum advance guard (1 second)
        if max_ts <= checkpoint + datetime.timedelta(seconds=self.MIN_CHECKPOINT_ADVANCE):
            log.debug(
                f"Checkpoint not advanced: max_ts ({max_ts.isoformat()}) is within "
                f"{self.MIN_CHECKPOINT_ADVANCE}s of current ({checkpoint.isoformat()})"
            )
            return

        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE ingestion_state
                SET last_external_timestamp = $1,
                    updated_at              = NOW()
                WHERE id = 1
                  AND (last_external_timestamp IS NULL
                       OR last_external_timestamp < $1)
            """, max_ts)

    # ══════════════════════════════════════════════════════════════════════
    # Lag monitoring
    # ══════════════════════════════════════════════════════════════════════

    async def monitor_lag(self) -> int:
        """Compute, log and return ingestion lag in seconds."""
        checkpoint = await self.get_checkpoint()
        now = datetime.datetime.now(datetime.timezone.utc)
        lag = int((now - checkpoint).total_seconds())
        self._current_lag = lag

        if lag < 60:
            log.info(f"⏱️  Ingestion lag: {lag}s (healthy)")
        elif lag < 120:
            log.warning(f"⚠️  Ingestion lag: {lag}s (elevated)")
        elif lag < 300:
            log.warning(f"⚠️  Ingestion lag: {lag}s (high)")
        elif lag < self.LAG_WATCHDOG_THRESHOLD:
            log.error(f"❌ Ingestion lag: {lag}s (critical)")
        else:
            log.critical(
                f"🚨 Ingestion lag: {lag}s (EMERGENCY — watchdog will trigger recovery)"
            )

        return lag

    # ══════════════════════════════════════════════════════════════════════
    # Prometheus-compatible metrics server
    # ══════════════════════════════════════════════════════════════════════

    async def _handle_metrics_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single HTTP request on the metrics port."""
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if line in (b"\r\n", b"\n", b""):
                    break

            path = request_line.decode().split(" ")[1] if request_line else "/"

            if path == "/metrics":
                body = (
                    f"# HELP ingestion_lag_seconds Current ingestion lag\n"
                    f"# TYPE ingestion_lag_seconds gauge\n"
                    f"ingestion_lag_seconds {self._current_lag}\n"
                    f"# HELP ingestion_total_scanned Total messages scanned\n"
                    f"# TYPE ingestion_total_scanned counter\n"
                    f"ingestion_total_scanned {self.stats['total_scanned']}\n"
                    f"# HELP ingestion_total_inserted Total messages inserted\n"
                    f"# TYPE ingestion_total_inserted counter\n"
                    f"ingestion_total_inserted {self.stats['total_inserted']}\n"
                    f"# HELP ingestion_incremental_runs Total incremental loop runs\n"
                    f"# TYPE ingestion_incremental_runs counter\n"
                    f"ingestion_incremental_runs {self.stats['incremental_runs']}\n"
                    f"# HELP ingestion_recovery_runs Total recovery sweep runs\n"
                    f"# TYPE ingestion_recovery_runs counter\n"
                    f"ingestion_recovery_runs {self.stats['recovery_runs']}\n"
                    f"# HELP ingestion_audit_runs Total audit sweep runs\n"
                    f"# TYPE ingestion_audit_runs counter\n"
                    f"ingestion_audit_runs {self.stats['audit_runs']}\n"
                )
                status = "200 OK"
            else:
                body = "Not Found\n"
                status = "404 Not Found"

            response = (
                f"HTTP/1.1 {status}\r\n"
                f"Content-Type: text/plain; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
                f"{body}"
            )
            writer.write(response.encode())
            await writer.drain()
        except Exception as exc:
            log.error(f"Metrics request handling failed: {exc}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_metrics_server(self) -> None:
        """Start Prometheus-compatible metrics HTTP server on port 9102."""
        try:
            self._metrics_server = await asyncio.start_server(
                self._handle_metrics_request,
                host="0.0.0.0",
                port=self.METRICS_PORT,
            )
            log.info(f"📊 Metrics server listening on http://0.0.0.0:{self.METRICS_PORT}/metrics")
        except OSError as exc:
            log.warning(f"⚠️  Metrics server failed to start (port {self.METRICS_PORT}): {exc}")

    async def stop_metrics_server(self) -> None:
        """Gracefully stop metrics server."""
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
            log.info("✓ Metrics server stopped")

    # ══════════════════════════════════════════════════════════════════════
    # Core fetch-and-insert (used by ALL three modes)
    # ══════════════════════════════════════════════════════════════════════

    async def fetch_and_insert(
        self,
        from_date: str,
        until_date: str,
        mode: str,
    ) -> Tuple[int, int, Optional[datetime.datetime]]:
        """
        Fetch messages from Turn.io and insert into ``webhook_events``.

        Uses the same producer-consumer pattern as ingest.py:
        1 producer  → async generator from TurnClient.get_messages()
        2 consumers → drain queue and batch-INSERT with actual-row counting

        Returns:
            ``(scanned, inserted, max_durable_timestamp)``
        """
        scanned = 0
        inserted = 0
        max_durable_ts: Optional[datetime.datetime] = None

        queue: asyncio.Queue = asyncio.Queue(maxsize=self.QUEUE_SIZE)
        producers_done = asyncio.Event()
        backpressure_threshold = int(self.QUEUE_SIZE * self.QUEUE_BACKPRESSURE_PCT)
        durable_lock = asyncio.Lock()

        async def mark_durable(candidate_ts: Optional[datetime.datetime]) -> None:
            """
            Update durable max timestamp only after committed DB flush.

            This is the core checkpoint safety guarantee: timestamps are
            promoted only once a batch write has completed successfully.
            """
            nonlocal max_durable_ts
            if candidate_ts is None:
                return
            async with durable_lock:
                if max_durable_ts is None or candidate_ts > max_durable_ts:
                    max_durable_ts = candidate_ts

        async def batch_consumer() -> None:
            """Drain queue in batches and INSERT to count real inserts."""
            nonlocal inserted

            batch: list = []
            last_flush = asyncio.get_event_loop().time()
            flush_interval = 2.0

            async with self.pool.acquire() as conn:
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=0.5)
                        batch.append(item)

                        if len(batch) >= self.BATCH_SIZE:
                            batch_inserted, batch_max_ts = await self._flush_batch(conn, batch)
                            inserted += batch_inserted
                            await mark_durable(batch_max_ts)
                            batch = []
                            last_flush = asyncio.get_event_loop().time()

                    except asyncio.TimeoutError:
                        now = asyncio.get_event_loop().time()
                        if batch and (now - last_flush >= flush_interval or producers_done.is_set()):
                            batch_inserted, batch_max_ts = await self._flush_batch(conn, batch)
                            inserted += batch_inserted
                            await mark_durable(batch_max_ts)
                            batch = []
                            last_flush = now

                        if producers_done.is_set() and queue.empty():
                            if batch:
                                batch_inserted, batch_max_ts = await self._flush_batch(conn, batch)
                                inserted += batch_inserted
                                await mark_durable(batch_max_ts)
                            break

        async def message_producer() -> None:
            """Fetch via TurnClient.get_messages and enqueue."""
            nonlocal scanned

            try:
                async for msg in self.client.get_messages(from_date, until_date):
                    ext_id = msg.get("id")
                    if not ext_id:
                        continue

                    msg_ts = extract_message_timestamp(msg)

                    # Queue backpressure — throttle producer when > 80%
                    if queue.qsize() > backpressure_threshold:
                        log.warning(
                            f"⚠️  [{mode}] Queue backpressure: {queue.qsize()}/{self.QUEUE_SIZE} "
                            f"({queue.qsize() * 100 // self.QUEUE_SIZE}%) — throttling producer"
                        )
                        await asyncio.sleep(0.01)

                    # Enqueue message event
                    if queue.full():
                        log.warning(f"  ⚠️ [{mode}] Ingest queue full, waiting for DB flush...")
                    await queue.put(("turn_io", "message", ext_id, msg, msg_ts))
                    scanned += 1

                    if scanned % 500 == 0:
                        log.info(f"  [{mode}] fetched {scanned:,} messages so far …")

            except Exception as exc:
                log.error(f"❌ [{mode}] Fetch failed: {exc}", exc_info=True)
                raise
            finally:
                producers_done.set()

        # ── orchestrate ──────────────────────────────────────────────────

        consumer_tasks = [
            asyncio.create_task(batch_consumer(), name=f"{mode}-consumer-{i}")
            for i in range(NUM_CONSUMERS)
        ]
        producer_task = asyncio.create_task(message_producer(), name=f"{mode}-producer")
        pipeline_tasks = [producer_task, *consumer_tasks]

        try:
            await asyncio.gather(*pipeline_tasks)
        except (Exception, asyncio.CancelledError) as exc:
            log.error(f"❌ [{mode}] Ingestion pipeline error or cancellation: {exc}")
            for t in pipeline_tasks:
                t.cancel()
            await asyncio.gather(*pipeline_tasks, return_exceptions=True)
            raise

        # Deterministic checkpoint source for the caller: durable writes only.
        return scanned, inserted, max_durable_ts

    # ── Timeout-protected fetch wrapper ───────────────────────────────────

    async def safe_fetch_and_insert(
        self,
        from_date: str,
        until_date: str,
        mode: str,
    ) -> Tuple[int, int, Optional[datetime.datetime]]:
        """
        Timeout-protected wrapper around ``fetch_and_insert``.

        If the fetch takes longer than FETCH_TIMEOUT (2 hours), it is
        cancelled. The checkpoint is NOT moved.

        Returns:
            Same as fetch_and_insert, or (0, 0, None) on timeout.
        """
        try:
            return await asyncio.wait_for(
                self.fetch_and_insert(from_date, until_date, mode),
                timeout=self.FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.error(
                f"⏰ [{mode}] TIMEOUT after {self.FETCH_TIMEOUT}s — fetch cancelled. "
                f"Checkpoint NOT moved. Will retry next cycle."
            )
            return 0, 0, None

    # ── batch writer ─────────────────────────────────────────────────────

    async def _flush_batch(self, conn, batch: list) -> Tuple[int, Optional[datetime.datetime]]:
        """
        Insert a batch into ``webhook_events`` and return the **actual**
        number of rows inserted (after ON CONFLICT dedup) plus the max
        timestamp from rows actually inserted in this committed batch.
        """
        if not batch:
            return 0, None

        try:
            placeholders = []
            values = []
            idx = 1
            ts_by_key = {}
            for provider, event_type, ext_id, payload, msg_ts in batch:
                placeholders.append(f"(${idx}, ${idx+1}, ${idx+2}, ${idx+3}::jsonb, FALSE)")
                values.extend([provider, event_type, ext_id, json.dumps(payload)])
                idx += 4
                key = (provider, ext_id)
                existing = ts_by_key.get(key)
                if msg_ts is not None and (existing is None or msg_ts > existing):
                    ts_by_key[key] = msg_ts

            sql = f"""
                INSERT INTO webhook_events
                    (provider, event_type, external_event_id, payload, processed)
                VALUES {", ".join(placeholders)}
                ON CONFLICT (provider, external_event_id) DO NOTHING
                RETURNING provider, external_event_id
            """
            inserted_rows = await conn.fetch(sql, *values)
            actual_inserted = len(inserted_rows)
            if actual_inserted == 0:
                return 0, None

            inserted_max_ts: Optional[datetime.datetime] = None
            for row in inserted_rows:
                key = (row["provider"], row["external_event_id"])
                ts = ts_by_key.get(key)
                if ts is not None and (inserted_max_ts is None or ts > inserted_max_ts):
                    inserted_max_ts = ts
            return actual_inserted, inserted_max_ts

        except Exception as exc:
            log.error(f"Failed to flush batch of {len(batch)} events: {exc}")
            raise

    # ══════════════════════════════════════════════════════════════════════
    # Task 1 — Continuous incremental loop
    # ══════════════════════════════════════════════════════════════════════

    async def incremental_loop(self) -> None:
        """
        Every 30 seconds:
            1. Read checkpoint
            2. from_date = checkpoint − 5 s  (overlap window)
            3. until_date = NOW()
            4. Fetch & insert (with timeout protection)
            5. If inserts committed → checkpoint = MAX(timestamp of inserted rows)
            6. If lag > 300 s → skip sleep (aggressive catch-up)
            7. If lag > 900 s → trigger emergency recovery
            8. Sleep 30 s
        """
        log.info("🔄 Starting incremental loop (every 30 s)")

        while not self.shutdown_event.is_set():
            try:
                checkpoint = await self.get_checkpoint()
                now_utc = datetime.datetime.now(datetime.timezone.utc)

                # Apply overlap window
                from_ts = checkpoint - datetime.timedelta(seconds=self.OVERLAP_SECONDS)
                from_date = from_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                until_date = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                async with self.api_lock:
                    scanned, inserted, durable_max_ts = await self.safe_fetch_and_insert(
                        from_date, until_date, "INCREMENTAL"
                    )

                self.stats["total_scanned"] += scanned
                self.stats["total_inserted"] += inserted
                self.stats["incremental_runs"] += 1

                # Advance checkpoint ONLY from durable batch writes.
                if durable_max_ts is not None:
                    await self.update_checkpoint(durable_max_ts)

                # Enhanced logging
                lag = await self.monitor_lag()
                log.info(
                    f"[INCREMENTAL] range={from_date}→{until_date} "
                    f"scanned={scanned:,} inserted={inserted:,} "
                    f"max_durable_ts={durable_max_ts.isoformat() if durable_max_ts else 'none'} "
                    f"lag={lag}s"
                )

                # Lag watchdog — if lag > 900s, trigger emergency recovery
                if lag > self.LAG_WATCHDOG_THRESHOLD:
                    log.critical(
                        f"🚨 WATCHDOG: lag {lag}s > {self.LAG_WATCHDOG_THRESHOLD}s — "
                        f"triggering immediate recovery sweep"
                    )
                    await self.recovery_sweep()

                # Recovery priority — if lag > 300s, skip sleep
                elif lag > self.LAG_RECOVERY_THRESHOLD:
                    log.warning(
                        f"⚡ Lag {lag}s > {self.LAG_RECOVERY_THRESHOLD}s — "
                        f"skipping sleep for aggressive catch-up"
                    )
                    continue

                await asyncio.sleep(self.INCREMENTAL_INTERVAL)

            except asyncio.CancelledError:
                log.info("Incremental loop cancelled")
                break
            except Exception as exc:
                log.error(f"❌ Incremental loop error: {exc}", exc_info=True)
                await asyncio.sleep(self.ERROR_BACKOFF)

    # ══════════════════════════════════════════════════════════════════════
    # Task 2 — Daily 3-day recovery sweep
    # ══════════════════════════════════════════════════════════════════════

    async def recovery_sweep(self) -> None:
        """
        Fetch NOW − 3 days → NOW.
        Does NOT update checkpoint. Relies on idempotent insert.
        """
        log.info("🔧 Starting daily 3-day recovery sweep")
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            from_ts = now_utc - datetime.timedelta(days=3)

            from_date = from_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            until_date = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

            async with self.api_lock:
                scanned, inserted, _ = await self.safe_fetch_and_insert(
                    from_date, until_date, "RECOVERY-3D"
                )

            self.stats["total_scanned"] += scanned
            self.stats["total_inserted"] += inserted
            self.stats["recovery_runs"] += 1

            lag = self._current_lag
            log.info(
                f"[RECOVERY-3D] range={from_date}→{until_date} "
                f"scanned={scanned:,} inserted={inserted:,} lag={lag}s"
            )

        except Exception as exc:
            log.error(f"❌ Recovery sweep failed: {exc}", exc_info=True)

    async def recovery_scheduler(self) -> None:
        """Run ``recovery_sweep`` once per calendar day. Persists state to DB."""
        log.info("📅 Daily recovery scheduler started")
        while not self.shutdown_event.is_set():
            try:
                today = datetime.date.today()
                if self._last_recovery_date != today:
                    await self.recovery_sweep()
                    self._last_recovery_date = today
                    await self.persist_scheduler_state()

                await asyncio.sleep(self.RECOVERY_CHECK_INTERVAL)

            except asyncio.CancelledError:
                log.info("Recovery scheduler cancelled")
                break
            except Exception as exc:
                log.error(f"❌ Recovery scheduler error: {exc}", exc_info=True)
                await asyncio.sleep(60)

    # ══════════════════════════════════════════════════════════════════════
    # Task 3 — Weekly 30-day integrity audit
    # ══════════════════════════════════════════════════════════════════════

    async def audit_sweep(self) -> None:
        """
        Fetch NOW − 30 days → NOW.
        Does NOT update checkpoint. Repairs any gaps.
        """
        log.info("🔍 Starting weekly 30-day integrity audit")
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            from_ts = now_utc - datetime.timedelta(days=30)

            from_date = from_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            until_date = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

            async with self.api_lock:
                scanned, inserted, _ = await self.safe_fetch_and_insert(
                    from_date, until_date, "AUDIT-30D"
                )

            self.stats["total_scanned"] += scanned
            self.stats["total_inserted"] += inserted
            self.stats["audit_runs"] += 1

            lag = self._current_lag
            log.info(
                f"[AUDIT-30D] range={from_date}→{until_date} "
                f"scanned={scanned:,} inserted={inserted:,} lag={lag}s"
            )

        except Exception as exc:
            log.error(f"❌ Audit sweep failed: {exc}", exc_info=True)

    async def audit_scheduler(self) -> None:
        """Run ``audit_sweep`` once per week (Monday). Persists state to DB."""
        log.info("📅 Weekly audit scheduler started")
        while not self.shutdown_event.is_set():
            try:
                today = datetime.date.today()
                if today.weekday() == 0 and self._last_audit_date != today:
                    await self.audit_sweep()
                    self._last_audit_date = today
                    await self.persist_scheduler_state()

                await asyncio.sleep(self.AUDIT_CHECK_INTERVAL)

            except asyncio.CancelledError:
                log.info("Audit scheduler cancelled")
                break
            except Exception as exc:
                log.error(f"❌ Audit scheduler error: {exc}", exc_info=True)
                await asyncio.sleep(60)

    # ══════════════════════════════════════════════════════════════════════
    # Orchestrator
    # ══════════════════════════════════════════════════════════════════════

    async def start(self) -> None:
        """Launch all concurrent tasks and run until shutdown."""
        log.info("=" * 70)
        log.info("🚀 MESSAGE WORKER STARTING (Enterprise Edition)")
        log.info("=" * 70)

        await self.ensure_checkpoint_row()
        await self.ensure_scheduler_state_row()
        await self.load_scheduler_state()
        await self.monitor_lag()

        await self.start_metrics_server()

        tasks = [
            asyncio.create_task(self.incremental_loop(), name="msg-incremental"),
            asyncio.create_task(self.recovery_scheduler(), name="msg-recovery"),
            asyncio.create_task(self.audit_scheduler(), name="msg-audit"),
        ]

        log.info("✓ Message worker tasks launched")
        log.info(f"  → Incremental : every 30 s (timeout {self.FETCH_TIMEOUT} s)")
        log.info("  → Recovery    : once/day  (3-day window)")
        log.info("  → Audit       : Mondays   (30-day window)")
        log.info(f"  → Metrics     : http://0.0.0.0:{self.METRICS_PORT}/metrics")
        log.info(f"  → Watchdog    : auto-recovery at lag > {self.LAG_WATCHDOG_THRESHOLD}s")
        log.info("")

        # Block until shutdown signal
        await self.shutdown_event.wait()

        # ── Clean shutdown guarantee ──────────────────────────────────────
        log.info("🛑 Message worker shutdown — cancelling tasks …")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await self.stop_metrics_server()

        # Log final checkpoint value explicitly
        final_checkpoint = await self.get_checkpoint()

        log.info("=" * 70)
        log.info("📊 MESSAGE WORKER FINAL STATISTICS")
        log.info("=" * 70)
        log.info(f"  Total scanned:     {self.stats['total_scanned']:,}")
        log.info(f"  Total inserted:    {self.stats['total_inserted']:,}")
        log.info(f"  Incremental runs:  {self.stats['incremental_runs']:,}")
        log.info(f"  Recovery runs:     {self.stats['recovery_runs']:,}")
        log.info(f"  Audit runs:        {self.stats['audit_runs']:,}")
        log.info(f"  Final checkpoint:  {final_checkpoint.isoformat()}")
        log.info(f"  Final lag:         {self._current_lag}s")
        log.info("=" * 70)
