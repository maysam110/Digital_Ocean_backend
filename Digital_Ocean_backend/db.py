import asyncpg 
import os 
import logging
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("ingestion.db")
log.info("Database configuration loaded")
db_pool = None

async def connect_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        min_size=2,
        max_size=15,
        command_timeout=60,
    )
    log.info("✓ Connected to PostgreSQL")

async def get_db():
    async with db_pool.acquire() as connection:
        yield connection