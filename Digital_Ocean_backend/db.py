import asyncio
import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("ingestion.db")


async def create_shared_pool() -> asyncpg.Pool:
    """
    Create the single shared asyncpg pool used by API + ingestion workers.

    Hard requirement:
      - one pool instance
      - max_size=1
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            pool = await asyncpg.create_pool(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", "5432")),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                min_size=1,
                max_size=1,
                command_timeout=60,
            )
            log.info("✓ Connected to PostgreSQL (shared pool, max_size=1)")
            return pool
        except Exception as exc:
            wait_time = min(5 * attempt, 60)
            log.error(
                f"🔌 DB pool creation failed (attempt {attempt}): {exc}. "
                f"Retrying in {wait_time}s..."
            )
            await asyncio.sleep(wait_time)


async def close_shared_pool(pool: asyncpg.Pool) -> None:
    """Close the shared pool gracefully."""
    if pool is None:
        return
    await pool.close()
    log.info("✓ Shared PostgreSQL pool closed")
