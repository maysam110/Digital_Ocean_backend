#!/usr/bin/env python3
"""
Normalization Worker — Entry Point

Startup sequence:
    1. Load config from environment
    2. Configure JSON logger
    3. Initialize DB pool (with retry — crash if DB unreachable after 5 attempts)
    4. Create normalization_state table if not exists
    5. Create normalization_failures table if not exists
    6. Create idx_webhook_events_normalizer index if not exists
    7. Load checkpoint from normalization_state
    8. Apply START_FROM_ID if checkpoint = 0 and START_FROM_ID > 0
    9. Log worker_started event
   10. Start heartbeat background task
   11. Enter main batch loop
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from config import START_FROM_ID, setup_logging
from models import WorkerState
import db_writer
import state_manager
import worker

logger: logging.Logger | None = None


async def main() -> None:
    """Main entry point — initializes everything and runs the worker."""
    global logger

    # ── 1. Configure logging ──────────────────────────────────────────
    logger = setup_logging()

    # ── 2. Signal handlers ────────────────────────────────────────────
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info({
            "event": "signal_received",
            "message": "Shutdown initiated, finishing current event...",
        })
        shutdown_event.set()

    loop.add_signal_handler(signal.SIGTERM, handle_signal)
    loop.add_signal_handler(signal.SIGINT, handle_signal)

    # ── 3. Initialize DB pool ─────────────────────────────────────────
    pool = await db_writer.init_pool()

    try:
        # ── 4-6. Schema setup ─────────────────────────────────────────
        await state_manager.setup_schema(pool)

        # ── 7. Load checkpoint ────────────────────────────────────────
        async with pool.acquire() as conn:
            checkpoint = await state_manager.get_checkpoint(conn)

        # ── 8. Apply START_FROM_ID on first run ───────────────────────
        if checkpoint == 0 and START_FROM_ID > 0:
            logger.info({
                "event": "applying_start_from_id",
                "start_from_id": START_FROM_ID,
                "message": f"First run — skipping to event id {START_FROM_ID}",
            })
            await state_manager.update_checkpoint(pool, START_FROM_ID)
            checkpoint = START_FROM_ID

        # ── 9. Initialize worker state ────────────────────────────────
        state = WorkerState(last_event_id=checkpoint)

        logger.info({
            "event": "worker_started",
            "checkpoint": checkpoint,
            "start_from_id": START_FROM_ID,
            "message": "Normalization worker starting",
        })

        # ── 10. Start heartbeat ───────────────────────────────────────
        heartbeat_task = asyncio.create_task(
            worker.heartbeat_loop(state, shutdown_event),
            name="heartbeat",
        )

        # ── 11. Enter main batch loop ─────────────────────────────────
        await worker.run_worker(pool, state, shutdown_event)

        # ── Clean shutdown ────────────────────────────────────────────
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)

    finally:
        await pool.close()
        if logger:
            logger.info({
                "event": "worker_stopped",
                "message": "Clean shutdown complete",
            })


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        if logger:
            logger.critical({
                "event": "worker_crash",
                "error": str(e),
                "message": "Worker crashed — container will restart",
            })
        sys.exit(1)
