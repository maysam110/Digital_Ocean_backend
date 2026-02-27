#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IngestionManager — Orchestrator for Message and Contact Workers

Responsibilities:
  1. Initialize shared DB pool
  2. Initialize TurnClient and TurnContactClient (shared rate limiter)
  3. Create MessageWorker and ContactWorker
  4. Launch both as asyncio tasks
  5. Crash isolation: one worker crashing does NOT stop the other
  6. No infinite restart loop — FastAPI container restart handles crash recovery
"""

import asyncio
import logging

from app.ingestion.base import (
    DB,
    TurnClient,
    TurnContactClient,
    AsyncRateLimiter,
    TURN_API_URL,
    TURN_BEARER_TOKEN,
    STARTUP_STAGGER_S,
    log,
)
from app.ingestion.messages_worker import MessageWorker
from app.ingestion.contacts_worker import ContactWorker

# Use a child logger for manager
log = logging.getLogger("ingestion.manager")


class IngestionManager:
    """
    Orchestrates both ingestion workers within a single asyncio event loop.

    Concurrency model:
        - Single process, single event loop
        - Two asyncio.create_task() — one per worker
        - Shared asyncpg pool (max_size=15)
        - Each worker gets its own httpx.AsyncClient
        - Shared AsyncRateLimiter to respect Turn.io's global 600 req/30s
        - No threads, no multiprocessing, no external queues

    Crash isolation:
        - Each worker is wrapped in try/except
        - One worker crashing does NOT stop the other
        - Exceptions are logged
        - No infinite restart loop (FastAPI container restart handles recovery)
    """

    def __init__(self):
        self.db: DB = DB()
        self._rate_limiter = AsyncRateLimiter(rate=20, period=1.0, burst=50)
        self.message_client: TurnClient = TurnClient(
            TURN_BEARER_TOKEN, TURN_API_URL, rate_limiter=self._rate_limiter
        )
        self.contact_client: TurnContactClient = TurnContactClient(
            TURN_BEARER_TOKEN, TURN_API_URL, rate_limiter=self._rate_limiter
        )
        self.message_worker: MessageWorker = None
        self.contact_worker: ContactWorker = None
        self._msg_task: asyncio.Task = None
        self._contact_task: asyncio.Task = None
        self._external_pool: bool = False  # True if pool was provided externally

    async def start(self, pool=None) -> None:
        """Initialize infrastructure and launch both workers."""
        log.info("=" * 70)
        log.info("🚀 INGESTION MANAGER STARTING")
        log.info("=" * 70)

        # 1. Connect to database (reuse external pool if provided)
        if pool is not None:
            self.db.accept_pool(pool)
            self._external_pool = True
        else:
            await self.db.connect()

        # 2. Create workers (shared DB pool, separate API clients)
        self.message_worker = MessageWorker(self.db, self.message_client)
        self.contact_worker = ContactWorker(self.db, self.contact_client)

        # 3. Launch both as isolated asyncio tasks with crash monitoring
        self._contact_task = asyncio.create_task(
            self._run_worker("ContactWorker", self.contact_worker),
            name="contact-worker"
        )
        self._contact_task.add_done_callback(
            lambda t: self._on_worker_done("ContactWorker", t)
        )

        # 4. Heartbeat task to show system health in logs even during long waits
        asyncio.create_task(self._heartbeat())

        # 3. Launch MessageWorker with a stagger delay to avoid resource contention
        # on startup (especially if both do backfills).
        log.info(f"⏳ MessageWorker will start after {STARTUP_STAGGER_S}s stagger delay...")
        self._msg_task = asyncio.create_task(
            self._run_staggered_message_worker(),
            name="message-worker-staggered"
        )

        log.info("✓ Both workers launched as background tasks")
        log.info("  → MessageWorker: incremental + recovery + audit + metrics")
        log.info("  → ContactWorker: continuous sync with DB checkpoint")
        log.info("=" * 70)

    @staticmethod
    def _on_worker_done(name: str, task: asyncio.Task) -> None:
        """Callback fired when a worker task completes (normally or via crash)."""
        try:
            exc = task.exception()
            if exc is not None:
                log.critical(
                    f"🚨 [{name}] WORKER TASK CRASHED: {exc}",
                    exc_info=exc,
                )
        except asyncio.CancelledError:
            log.info(f"[{name}] Worker task was cancelled")

    async def _run_worker(self, name: str, worker) -> None:
        """
        Wrapper that runs a worker with crash isolation.

        If the worker crashes, the exception is logged but NOT propagated.
        This ensures the other worker continues running.
        """
        try:
            log.info(f"[{name}] Starting...")
            await worker.start()
        except asyncio.CancelledError:
            log.info(f"[{name}] Cancelled")
        except Exception as exc:
            log.error(
                f"❌ [{name}] CRASHED: {exc}",
                exc_info=True
            )
            log.error(
                f"⚠️  [{name}] Worker has stopped. "
                f"Container restart will recover this worker."
            )

    async def _heartbeat(self) -> None:
        """Periodic log to confirm the manager is alive."""
        while True:
            await asyncio.sleep(300)  # 5 minutes
            log.info("💓 [HEARTBEAT] IngestionManager is running and monitoring workers...")

    async def _run_staggered_message_worker(self) -> None:
        """Wait for stagger delay then run MessageWorker."""
        try:
            await asyncio.sleep(STARTUP_STAGGER_S)
            if self.message_worker:
                await self._run_worker("MessageWorker", self.message_worker)
        except asyncio.CancelledError:
            log.info("[MessageWorker] Staggered start cancelled")
        except Exception as exc:
            log.error(f"❌ [MessageWorker] Staggered start failed: {exc}", exc_info=True)

    async def stop(self) -> None:
        """Graceful shutdown of all workers and resources."""
        log.info("🛑 Ingestion manager shutting down...")

        # Signal workers to stop
        if self.message_worker:
            self.message_worker.shutdown_event.set()
        if self.contact_worker:
            self.contact_worker.shutdown_event.set()

        # Give workers time to clean up
        await asyncio.sleep(2)

        # Close API clients
        try:
            await self.message_client.close()
        except Exception:
            pass
        try:
            await self.contact_client.close()
        except Exception:
            pass

        # Close DB pool ONLY if we created it ourselves.
        # If the pool was provided externally (shared with webhook handler),
        # closing it here would break active webhook requests.
        if not self._external_pool:
            try:
                await self.db.close()
            except Exception:
                pass

        log.info("✓ Ingestion manager stopped")

