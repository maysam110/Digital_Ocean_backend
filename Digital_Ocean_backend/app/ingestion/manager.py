#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IngestionManager — Orchestrator for Message and Contact Workers

Responsibilities:
  1. Reuse the single shared DB pool created by FastAPI lifespan
  2. Initialize TurnClient and TurnContactClient (shared rate limiter)
  3. Create MessageWorker and ContactWorker
  4. Launch both as asyncio tasks
  5. Surface worker failures (no silent exception swallowing)
  6. Clean async shutdown (await worker tasks + close HTTP clients)
"""

import asyncio
import logging

from app.ingestion.base import (
    TurnClient,
    TurnContactClient,
    AsyncRateLimiter,
    TURN_API_URL,
    TURN_BEARER_TOKEN,
    STARTUP_STAGGER_S,
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
        - Shared asyncpg pool (max_size=1)
        - Each worker gets its own httpx.AsyncClient
        - Shared AsyncRateLimiter to respect Turn.io's global 600 req/30s
        - No threads, no multiprocessing, no external queues

    Error visibility:
        - Worker failures are surfaced in task callbacks
        - Shutdown awaits worker task termination
    """

    def __init__(self, pool):
        # Single shared DB pool owned by FastAPI lifespan.
        self.pool = pool
        self._rate_limiter = AsyncRateLimiter(rate=20, period=1.0, burst=20)
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
        self._heartbeat_task: asyncio.Task = None
        self._started = False

    async def start(self) -> None:
        """Initialize infrastructure and launch both workers."""
        if self._started:
            return
        self._started = True

        log.info("=" * 70)
        log.info("🚀 INGESTION MANAGER STARTING")
        log.info("=" * 70)

        # 1) Create workers (single shared pool, separate HTTP clients).
        self.message_worker = MessageWorker(self.pool, self.message_client)
        self.contact_worker = ContactWorker(self.pool, self.contact_client)

        # 2) Launch both workers + heartbeat.
        self._contact_task = asyncio.create_task(
            self.contact_worker.start(),
            name="contact-worker"
        )
        self._contact_task.add_done_callback(lambda t: self._on_worker_done("ContactWorker", t))

        self._heartbeat_task = asyncio.create_task(self._heartbeat(), name="ingestion-heartbeat")
        self._heartbeat_task.add_done_callback(lambda t: self._on_worker_done("Heartbeat", t))

        # Start MessageWorker with a stagger to reduce startup contention.
        log.info(f"⏳ MessageWorker will start after {STARTUP_STAGGER_S}s stagger delay...")
        self._msg_task = asyncio.create_task(
            self._run_staggered_message_worker(),
            name="message-worker-staggered"
        )
        self._msg_task.add_done_callback(lambda t: self._on_worker_done("MessageWorker", t))

        log.info("✓ Ingestion workers launched")
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

    async def _heartbeat(self) -> None:
        """Periodic log to confirm the manager is alive."""
        while True:
            await asyncio.sleep(300)  # 5 minutes
            log.info("💓 [HEARTBEAT] IngestionManager is running and monitoring workers...")

    async def _run_staggered_message_worker(self) -> None:
        """Wait for stagger delay then run MessageWorker."""
        await asyncio.sleep(STARTUP_STAGGER_S)
        if self.message_worker and not self.message_worker.shutdown_event.is_set():
            await self.message_worker.start()

    async def stop(self) -> None:
        """Graceful shutdown of all workers and resources."""
        log.info("🛑 Ingestion manager shutting down...")

        # Signal workers to stop
        if self.message_worker:
            self.message_worker.shutdown_event.set()
        if self.contact_worker:
            self.contact_worker.shutdown_event.set()

        # Cancel heartbeat first.
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

        # Wait for worker tasks to complete.
        tasks_to_wait = []
        if self._msg_task:
            tasks_to_wait.append(self._msg_task)
        if self._contact_task:
            tasks_to_wait.append(self._contact_task)
        if self._heartbeat_task:
            tasks_to_wait.append(self._heartbeat_task)
        if tasks_to_wait:
            results = await asyncio.gather(*tasks_to_wait, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    log.error(f"Worker task ended with error during shutdown: {result}", exc_info=result)

        # Close API clients
        try:
            await self.message_client.close()
        except Exception as exc:
            log.error(f"Failed to close message client: {exc}", exc_info=True)
        try:
            await self.contact_client.close()
        except Exception as exc:
            log.error(f"Failed to close contact client: {exc}", exc_info=True)

        self._started = False
        log.info("✓ Ingestion manager stopped")

    def workers_running(self) -> bool:
        """Readiness helper for FastAPI endpoint."""
        contact_ok = self._contact_task is not None and not self._contact_task.done()
        msg_ok = self._msg_task is not None and not self._msg_task.done()
        return contact_ok and msg_ok
