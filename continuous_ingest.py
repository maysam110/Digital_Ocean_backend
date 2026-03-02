#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Continuous Turn.io Ingestion Daemon — Enterprise-Grade, Zero-Gap, Self-Contained

This file is 100% standalone — it does NOT import from ingest.py.
It contains its own DB, TurnClient, rate limiter, and config.

Architecture:
    1. Continuous 30-second incremental loop
    2. Daily 3-day recovery sweep
    3. Weekly 30-day integrity audit
    4. Prometheus-compatible metrics endpoint (:9102/metrics)

Safety guarantees:
    - Checkpoint = MAX(timestamp from actually fetched messages), NEVER NOW()
    - 2-second overlap window prevents timestamp boundary loss
    - ON CONFLICT DO NOTHING ensures idempotent inserts
    - Actual insert count via PostgreSQL command tag
    - Checkpoint only advances after successful commit
    - Crash mid-batch → checkpoint stays put → re-fetch on restart
    - Persistent scheduler state survives restarts
    - Minimum 1-second advance guard against clock jitter
    - 10-minute fetch timeout prevents infinite hangs
    - Lag watchdog triggers self-healing recovery
    - Queue backpressure prevents memory exhaustion

Usage:
    python continuous_ingest.py
"""

import asyncio
import datetime
import json
import logging
import os
import signal
import sys
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import asyncpg
import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------
load_dotenv()

TURN_API_URL = os.getenv("TURNIO_BASE_URL")
TURN_BEARER_TOKEN = os.getenv("TURNIO_BEARER_TOKEN")
if not TURN_BEARER_TOKEN:
    print("❌ TURNIO_BEARER_TOKEN not set in environment")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    handlers=[
        logging.FileHandler("continuous_ingest.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("continuous_ingest")


# ===========================================================================
# Async Rate Limiter (self-contained — copied from ingest.py)
# ===========================================================================
class AsyncRateLimiter:
    """
    Token bucket rate limiter for async operations.

    Turn.io API limit: 600 requests per 30 seconds.
    Allows bursts up to ``burst`` tokens, then throttles to sustain
    ``rate/period`` requests per second.
    """

    def __init__(self, rate: int = 20, period: float = 1.0, burst: int = 50):
        self.rate = rate
        self.period = period
        self.max_tokens = burst
        self.tokens = float(burst)
        self.updated_at = 0.0
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1):
        """Acquire *tokens* from the bucket, sleeping if necessary."""
        async with self.lock:
            while True:
                now = asyncio.get_event_loop().time()

                # Replenish tokens based on elapsed time
                if self.updated_at > 0:
                    elapsed = now - self.updated_at
                    self.tokens = min(
                        self.max_tokens,
                        self.tokens + (elapsed * self.rate / self.period),
                    )

                self.updated_at = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                tokens_needed = tokens - self.tokens
                wait_time = (tokens_needed * self.period) / self.rate

        # Wait outside the lock to let other coroutines proceed
        await asyncio.sleep(wait_time)


class CursorExpiredError(Exception):
    """Raised when Turn.io returns 400 'Cursor has expired'."""
    pass


# ===========================================================================
# Turn.io API Client (self-contained — with paging null-safety fix)
# ===========================================================================
class TurnClient:
    """Async HTTP client for Turn.io Data Export API with pagination."""

    def __init__(self, token: str, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.v1+json",
            "Content-Type": "application/json",
        }
        timeout_config = httpx.Timeout(60.0, connect=10.0)
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=timeout_config,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            follow_redirects=True,
        )
        self.rate_limiter = AsyncRateLimiter(rate=20, period=1.0, burst=50)
        self.semaphore = asyncio.Semaphore(10)

    # ── low-level helpers ─────────────────────────────────────────────────

    async def _create_data_export_cursor(
        self,
        data_type: str = "messages",
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
    ) -> str:
        """Create a cursor for the Turn.io Data Export API with retry logic.

        Retries on:
            - 429 rate limit errors
            - Connection errors (ConnectTimeout, ReadError, ConnectError, etc.)

        Never gives up permanently — uses exponential backoff up to 60 s.
        """
        from datetime import datetime, timedelta

        if not from_date:
            from_date = (datetime.utcnow() - timedelta(days=30)).replace(microsecond=0).isoformat() + "Z"
        if not until_date:
            until_date = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        endpoint = f"/v1/data/{data_type}/cursor"
        url = f"{self.base_url}{endpoint}"
        payload = {"from": from_date, "until": until_date, "ordering": "asc"}

        log.info(f"Creating data export cursor for {data_type} from {from_date} to {until_date}...")

        max_retries = 10
        base_delay = 2

        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire()
                resp = await self.client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                cursor = data.get("cursor")
                if not cursor:
                    raise ValueError(f"No cursor returned from API: {data}")
                log.info(f"✓ Cursor created: {cursor[:20]}...")
                return cursor

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait_time = min(base_delay * (2 ** attempt), 60)
                    log.warning(
                        f"⚠️  Rate limit hit (429). Waiting {wait_time}s "
                        f"before retry {attempt + 1}/{max_retries}..."
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    log.error(f"HTTP {e.response.status_code} error creating cursor: {e.response.text}")
                    raise

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError,
                    httpx.ReadError, httpx.TimeoutException, OSError) as e:
                wait_time = min(base_delay * (2 ** attempt), 60)
                log.warning(
                    f"🔌 Connection error creating cursor (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_time}s: {type(e).__name__}: {e}"
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    log.error(f"❌ Failed to create cursor after {max_retries} attempts")
                    raise

            except Exception as e:
                log.error(f"Failed to create cursor: {e}")
                raise

    async def _fetch_data_export_page(self, data_type: str, cursor: str) -> Dict:
        """Fetch a single page of data using a cursor, with retry logic.

        Retries on all connection/timeout errors with exponential backoff.
        """
        endpoint = f"/v1/data/{data_type}/cursor/{cursor}"
        url = f"{self.base_url}{endpoint}"

        max_retries = 5
        base_delay = 2
        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire()
                async with self.semaphore:
                    resp = await self.client.get(url)
                resp.raise_for_status()
                return resp.json()
            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError,
                    httpx.ReadError, httpx.TimeoutException, OSError) as e:
                wait_time = min(base_delay * (2 ** attempt), 60)
                if attempt < max_retries - 1:
                    log.warning(
                        f"🔌 Connection error fetching page (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {wait_time}s: {type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    log.error(f"❌ Failed to fetch page after {max_retries} attempts: {e}")
                    raise
            except httpx.HTTPStatusError as e:
                # ── Cursor expired → signal caller to re-create ───────
                if e.response.status_code == 400:
                    body = e.response.text.lower()
                    if "cursor has expired" in body or ("cursor" in body and "expired" in body):
                        log.warning("⏰ Cursor expired — will re-create from last offset")
                        raise CursorExpiredError("Cursor has expired") from e
                if e.response.status_code == 429:
                    wait_time = min(base_delay * (2 ** attempt), 60)
                    log.warning(
                        f"⚠️  Rate limit hit (429) fetching page, retrying in {wait_time}s..."
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(wait_time)
                        continue
                log.error(f"HTTP {e.response.status_code} error fetching data: {e.response.text}")
                raise
            except Exception as e:
                log.error(f"Request failed: {e}")
                raise

    # ── public generator ──────────────────────────────────────────────────

    async def get_messages(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
    ) -> AsyncGenerator[Dict, None]:
        """
        Fetch historical messages via the Data Export API.

        Creates ONE cursor for the entire date range and paginates until
        the ``paging.next`` cursor is absent or null.

        **FIX**: Uses ``data.get("paging") or {}`` instead of
        ``data.get("paging", {})`` because the Turn.io API can return
        ``"paging": null`` (explicit null), which bypasses the default
        value of ``dict.get``.

        Yields:
            Individual message dicts.
        """
        from datetime import datetime as _dt, timedelta as _td

        try:
            if not from_date:
                from_date = (_dt.utcnow() - _td(days=30)).replace(microsecond=0).isoformat() + "Z"
            if not until_date:
                until_date = _dt.utcnow().replace(microsecond=0).isoformat() + "Z"

            log.info("📥 Fetching historical messages using Data Export API...")
            log.info(f"   Date range: {from_date} → {until_date}")
            log.info(f"   Strategy: Single cursor with full pagination")

            cursor = await self._create_data_export_cursor("messages", from_date, until_date)

            page_count = 0
            total_messages = 0

            # Track the last message timestamp for cursor re-creation
            last_message_ts = from_date
            max_cursor_retries = 10  # prevent infinite re-creation loops
            cursor_retries = 0

            while cursor:
                page_count += 1
                log.info(f"📄 Fetching page {page_count}...")

                try:
                    data = await self._fetch_data_export_page("messages", cursor)
                except CursorExpiredError:
                    cursor_retries += 1
                    if cursor_retries > max_cursor_retries:
                        log.error(f"❌ Cursor expired {cursor_retries} times — giving up")
                        raise RuntimeError(
                            f"Cursor keeps expiring after {cursor_retries} re-creations"
                        )
                    log.warning(
                        f"🔄 Re-creating cursor from last offset: {last_message_ts} "
                        f"(attempt {cursor_retries}/{max_cursor_retries})"
                    )
                    cursor = await self._create_data_export_cursor(
                        "messages", last_message_ts, until_date,
                    )
                    continue

                # Reset cursor-retry counter on every successful page
                cursor_retries = 0

                data_items = data.get("data") or data.get("messages", [])

                if not data_items:
                    log.warning(f"⚠️  Page {page_count} returned 0 items")

                page_message_count = 0
                for data_item in data_items:
                    if isinstance(data_item, dict):
                        if "messages" in data_item:
                            for msg in data_item.get("messages", []):
                                yield msg
                                page_message_count += 1
                                total_messages += 1
                                # Track timestamp for cursor recovery
                                ts = msg.get("timestamp")
                                if ts:
                                    last_message_ts = ts
                        elif "id" in data_item:
                            yield data_item
                            page_message_count += 1
                            total_messages += 1
                            ts = data_item.get("timestamp")
                            if ts:
                                last_message_ts = ts

                log.info(f"✓ Page {page_count}: {page_message_count} messages (total: {total_messages})")

                # ── CRITICAL FIX ──────────────────────────────────────────
                # Turn.io API can return "paging": null (explicit JSON null).
                # dict.get("paging", {}) does NOT use the default when the
                # key exists but has value None — so we use `or {}`.
                paging = data.get("paging") or {}
                cursor = paging.get("next")

                if cursor:
                    log.debug(f"Next cursor: {cursor[:20]}...")
                else:
                    log.info(f"\n✅ Pagination complete!")
                    log.info(f"   Total pages: {page_count}")
                    log.info(f"   Total messages: {total_messages}")
                    break

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                log.error("⚠️  Rate limit hit (429)")
                raise
            elif e.response.status_code == 404:
                log.error("❌ Turn.io Data Export API not available (404)")
                raise RuntimeError("Turn.io Data Export API not accessible.")
            else:
                raise

    async def close(self):
        """Close the underlying HTTP client."""
        await self.client.aclose()


# ===========================================================================
# Database Connection Pool (self-contained)
# ===========================================================================
class DB:
    """PostgreSQL connection pool manager with infinite retry."""

    def __init__(self):
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        """Create connection pool. Retries forever until DB is reachable."""
        attempt = 0
        while True:
            attempt += 1
            try:
                self.pool = await asyncpg.create_pool(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                )
                log.info(f"✓ Connected to PostgreSQL: {DB_NAME}@{DB_HOST}:{DB_PORT}")
                return
            except Exception as exc:
                wait_time = min(5 * attempt, 60)
                log.error(
                    f"🔌 DB connection failed (attempt {attempt}): {exc}. "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            log.info("✓ Database connection closed")


# ===========================================================================
# Timestamp extraction — Turn.io export payload
# ===========================================================================

def extract_message_timestamp(msg: dict) -> Optional[datetime.datetime]:
    """
    Extract the best available timestamp from a Turn.io export message.

    Priority order:
        1. ``msg["timestamp"]``                          — Unix epoch (seconds or ms)
        2. ``msg["_vnd"]["v1"]["inserted_at"]``          — ISO string
        3. ``msg["_vnd"]["v1"]["last_status_timestamp"]`` — ISO string

    The returned datetime is always timezone-aware (UTC).
    If the parsed timestamp is in the future, it is clamped to NOW().
    Returns ``None`` if no valid timestamp can be extracted.
    """
    candidates = [
        msg.get("timestamp"),
    ]
    vnd = msg.get("_vnd", {})
    v1 = vnd.get("v1", {}) if isinstance(vnd, dict) else {}
    if isinstance(v1, dict):
        candidates.append(v1.get("inserted_at"))
        candidates.append(v1.get("last_status_timestamp"))

    for raw in candidates:
        parsed = _safe_parse_ts(raw)
        if parsed is not None:
            # Clamp future timestamps to NOW
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if parsed > now_utc:
                parsed = now_utc
            return parsed

    return None


def _safe_parse_ts(raw) -> Optional[datetime.datetime]:
    """
    Parse a raw timestamp value into a tz-aware UTC datetime.

    Handles:
        - Unix epoch in **seconds** (10-digit int/str)
        - Unix epoch in **milliseconds** (13-digit int/str)
        - ISO-8601 strings with ``Z`` or ``+00:00`` suffix

    Never raises.
    """
    if raw is None:
        return None

    # --- numeric (epoch seconds or milliseconds) ---
    try:
        epoch = int(raw) if not isinstance(raw, int) else raw
        if epoch > 1e12:
            epoch = epoch / 1000.0
        return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
    except (ValueError, TypeError, OSError, OverflowError):
        pass

    # --- ISO string ---
    try:
        s = str(raw).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except (ValueError, TypeError):
        pass

    return None


# ===========================================================================
# ContinuousIngestor
# ===========================================================================
class ContinuousIngestor:
    """
    Enterprise-grade continuous ingestion daemon — fully self-contained.

    Four concurrent tasks:
        1. incremental_loop    — every 30 s, fetch checkpoint-2s → NOW
        2. recovery_scheduler  — once/day, fetch NOW-3d → NOW
        3. audit_scheduler     — once/week (Monday), fetch NOW-30d → NOW
        4. metrics_server      — HTTP :9102/metrics (Prometheus-compatible)

    Critical invariants:
        • Checkpoint is ALWAYS the MAX(timestamp) of actually-seen messages.
        • Checkpoint is NEVER set to NOW() or any wall-clock value.
        • Every fetch overlaps by 2 seconds to guard against boundary races.
        • webhook_events UNIQUE(provider, external_event_id) prevents dupes.
        • If no new messages are returned, checkpoint does NOT move forward.
        • Scheduler state persists across restarts via DB table.
        • Minimum 1s advance prevents clock-jitter checkpoint thrashing.
        • 10-minute timeout prevents infinite API hangs.
        • Lag > 900s triggers automatic self-healing recovery.
    """

    # ── constants ──────────────────────────────────────────────────────────
    BATCH_SIZE: int = 1000
    QUEUE_SIZE: int = 10_000
    OVERLAP_SECONDS: int = 2
    INCREMENTAL_INTERVAL: int = 30
    ERROR_BACKOFF: int = 10
    RECOVERY_CHECK_INTERVAL: int = 600
    AUDIT_CHECK_INTERVAL: int = 3600
    FETCH_TIMEOUT: int = 3600              # 1 hour max per fetch (large catch-ups need time)
    MIN_CHECKPOINT_ADVANCE: int = 1        # minimum seconds to advance
    LAG_RECOVERY_THRESHOLD: int = 300      # aggressive catch-up at 5 min
    LAG_WATCHDOG_THRESHOLD: int = 900      # emergency recovery at 15 min
    QUEUE_BACKPRESSURE_PCT: float = 0.8    # warn at 80% queue capacity
    METRICS_PORT: int = 9102

    def __init__(self, db: DB, client: TurnClient) -> None:
        self.db = db
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

    async def ensure_checkpoint_table(self) -> None:
        """Create ``ingestion_state`` if it does not exist."""
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_state (
                    id   int PRIMARY KEY,
                    last_external_timestamp timestamptz,
                    updated_at              timestamptz DEFAULT now()
                )
            """)
            await conn.execute("""
                INSERT INTO ingestion_state (id, last_external_timestamp)
                VALUES (1, NOW() - INTERVAL '30 days')
                ON CONFLICT (id) DO NOTHING
            """)
        log.info("✓ ingestion_state table ready")

    # ══════════════════════════════════════════════════════════════════════
    # Persistent scheduler state
    # ══════════════════════════════════════════════════════════════════════

    async def ensure_scheduler_state_table(self) -> None:
        """
        Create ``ingestion_scheduler_state`` to persist recovery/audit dates
        across process restarts, preventing duplicate sweeps.
        """
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_scheduler_state (
                    id                 int PRIMARY KEY,
                    last_recovery_date date,
                    last_audit_date    date,
                    updated_at         timestamptz DEFAULT now()
                )
            """)
            await conn.execute("""
                INSERT INTO ingestion_scheduler_state (id)
                VALUES (1)
                ON CONFLICT (id) DO NOTHING
            """)
        log.info("✓ ingestion_scheduler_state table ready")

    async def load_scheduler_state(self) -> None:
        """Load persisted scheduler dates into memory on startup."""
        async with self.db.pool.acquire() as conn:
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
        async with self.db.pool.acquire() as conn:
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
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_external_timestamp FROM ingestion_state WHERE id = 1"
            )
            if row and row["last_external_timestamp"]:
                return row["last_external_timestamp"]
            return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)

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

        async with self.db.pool.acquire() as conn:
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
        except Exception:
            pass
        finally:
            writer.close()

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
            ``(scanned, inserted, max_timestamp_seen)``
        """
        scanned = 0
        inserted = 0
        max_ts: Optional[datetime.datetime] = None

        queue: asyncio.Queue = asyncio.Queue(maxsize=self.QUEUE_SIZE)
        producers_done = asyncio.Event()
        backpressure_threshold = int(self.QUEUE_SIZE * self.QUEUE_BACKPRESSURE_PCT)

        # ── consumer (×2) ─────────────────────────────────────────────────

        async def batch_consumer() -> None:
            """Drain queue in batches and INSERT to count real inserts."""
            nonlocal inserted

            batch: list = []
            last_flush = asyncio.get_event_loop().time()
            flush_interval = 2.0

            async with self.db.pool.acquire() as conn:
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=0.5)
                        batch.append(item)

                        if len(batch) >= self.BATCH_SIZE:
                            inserted += await self._flush_batch(conn, batch)
                            batch = []
                            last_flush = asyncio.get_event_loop().time()

                    except asyncio.TimeoutError:
                        now = asyncio.get_event_loop().time()
                        if batch and (now - last_flush >= flush_interval or producers_done.is_set()):
                            inserted += await self._flush_batch(conn, batch)
                            batch = []
                            last_flush = now

                        if producers_done.is_set() and queue.empty():
                            if batch:
                                inserted += await self._flush_batch(conn, batch)
                            break

        # ── producer (×1) ────────────────────────────────────────────────

        async def message_producer() -> None:
            """Fetch via TurnClient.get_messages and enqueue."""
            nonlocal scanned, max_ts

            try:
                async for msg in self.client.get_messages(from_date, until_date):
                    ext_id = msg.get("id")
                    if not ext_id:
                        continue

                    # Track MAX timestamp using priority chain
                    msg_ts = extract_message_timestamp(msg)
                    if msg_ts is not None and (max_ts is None or msg_ts > max_ts):
                        max_ts = msg_ts

                    # Queue backpressure — throttle producer when > 80%
                    if queue.qsize() > backpressure_threshold:
                        log.warning(
                            f"⚠️  [{mode}] Queue backpressure: {queue.qsize()}/{self.QUEUE_SIZE} "
                            f"({queue.qsize() * 100 // self.QUEUE_SIZE}%) — throttling producer"
                        )
                        await asyncio.sleep(0.01)

                    # Enqueue message event
                    await queue.put(("turn_io", "message", ext_id, msg))
                    scanned += 1

                    if scanned % 500 == 0:
                        log.info(f"  [{mode}] fetched {scanned:,} messages so far …")

            except Exception as exc:
                log.error(f"❌ [{mode}] Fetch failed: {exc}", exc_info=True)
                raise

        # ── orchestrate ──────────────────────────────────────────────────

        consumer_tasks = [
            asyncio.create_task(batch_consumer(), name=f"{mode}-consumer-1"),
            asyncio.create_task(batch_consumer(), name=f"{mode}-consumer-2"),
        ]
        producer_task = asyncio.create_task(message_producer(), name=f"{mode}-producer")

        try:
            await producer_task
            producers_done.set()
            await asyncio.gather(*consumer_tasks)
        except Exception as exc:
            log.error(f"❌ [{mode}] Ingestion pipeline error: {exc}")
            raise

        return scanned, inserted, max_ts

    # ── Timeout-protected fetch wrapper ───────────────────────────────────

    async def safe_fetch_and_insert(
        self,
        from_date: str,
        until_date: str,
        mode: str,
    ) -> Tuple[int, int, Optional[datetime.datetime]]:
        """
        Timeout-protected wrapper around ``fetch_and_insert``.

        If the fetch takes longer than FETCH_TIMEOUT (10 min), it is
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

    async def _flush_batch(self, conn, batch: list) -> int:
        """
        Insert a batch into ``webhook_events`` and return the **actual**
        number of rows inserted (after ON CONFLICT dedup).

        Uses a single multi-row INSERT … ON CONFLICT DO NOTHING.
        Row count from PostgreSQL command tag (e.g. ``INSERT 0 42``).
        """
        if not batch:
            return 0

        try:
            placeholders = []
            values = []
            idx = 1
            for provider, event_type, ext_id, payload in batch:
                placeholders.append(f"(${idx}, ${idx+1}, ${idx+2}, ${idx+3}::jsonb, FALSE)")
                values.extend([provider, event_type, ext_id, json.dumps(payload)])
                idx += 4

            sql = f"""
                INSERT INTO webhook_events
                    (provider, event_type, external_event_id, payload, processed)
                VALUES {', '.join(placeholders)}
                ON CONFLICT (provider, external_event_id) DO NOTHING
            """

            result = await conn.execute(sql, *values)

            # result looks like "INSERT 0 42" — last number is rows inserted
            actual_inserted = int(result.split()[-1])
            return actual_inserted

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
            2. from_date = checkpoint − 2 s  (overlap window)
            3. until_date = NOW()
            4. Fetch & insert (with 10-min timeout)
            5. If messages found → checkpoint = MAX(timestamp seen)
            6. If lag > 300 s → skip sleep (aggressive catch-up)
            7. If lag > 900 s → trigger emergency recovery
            8. Sleep 30 s
        """
        log.info("🔄 Starting incremental loop (every 30 s)")

        while not self.shutdown_event.is_set():
            try:
                checkpoint = await self.get_checkpoint()
                now_utc = datetime.datetime.now(datetime.timezone.utc)

                # Apply 2-second overlap window
                from_ts = checkpoint - datetime.timedelta(seconds=self.OVERLAP_SECONDS)
                from_date = from_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                until_date = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                async with self.api_lock:
                    scanned, inserted, max_ts = await self.safe_fetch_and_insert(
                        from_date, until_date, "INCREMENTAL"
                    )

                self.stats["total_scanned"] += scanned
                self.stats["total_inserted"] += inserted
                self.stats["incremental_runs"] += 1

                # Advance checkpoint ONLY to MAX(actual message timestamp)
                if max_ts is not None:
                    await self.update_checkpoint(max_ts)

                # Enhanced logging
                lag = await self.monitor_lag()
                log.info(
                    f"[INCREMENTAL] range={from_date}→{until_date} "
                    f"scanned={scanned:,} inserted={inserted:,} "
                    f"max_ts={max_ts.isoformat() if max_ts else 'none'} "
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
        """Launch all concurrent tasks and block until shutdown."""
        log.info("=" * 70)
        log.info("🚀 CONTINUOUS INGESTION DAEMON STARTING (Enterprise Edition)")
        log.info("=" * 70)

        await self.ensure_checkpoint_table()
        await self.ensure_scheduler_state_table()
        await self.load_scheduler_state()
        await self.monitor_lag()

        await self.start_metrics_server()

        tasks = [
            asyncio.create_task(self.incremental_loop(), name="incremental"),
            asyncio.create_task(self.recovery_scheduler(), name="recovery"),
            asyncio.create_task(self.audit_scheduler(), name="audit"),
        ]

        log.info("✓ Background tasks launched")
        log.info("  → Incremental : every 30 s (timeout 10 min)")
        log.info("  → Recovery    : once/day  (3-day window)")
        log.info("  → Audit       : Mondays   (30-day window)")
        log.info(f"  → Metrics     : http://0.0.0.0:{self.METRICS_PORT}/metrics")
        log.info(f"  → Watchdog    : auto-recovery at lag > {self.LAG_WATCHDOG_THRESHOLD}s")
        log.info("")

        # Block until shutdown signal
        await self.shutdown_event.wait()

        # ── Clean shutdown guarantee ──────────────────────────────────────
        log.info("🛑 Shutdown signal received — cancelling tasks …")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await self.stop_metrics_server()

        # Log final checkpoint value explicitly
        final_checkpoint = await self.get_checkpoint()

        log.info("=" * 70)
        log.info("📊 FINAL STATISTICS")
        log.info("=" * 70)
        log.info(f"  Total scanned:     {self.stats['total_scanned']:,}")
        log.info(f"  Total inserted:    {self.stats['total_inserted']:,}")
        log.info(f"  Incremental runs:  {self.stats['incremental_runs']:,}")
        log.info(f"  Recovery runs:     {self.stats['recovery_runs']:,}")
        log.info(f"  Audit runs:        {self.stats['audit_runs']:,}")
        log.info(f"  Final checkpoint:  {final_checkpoint.isoformat()}")
        log.info(f"  Final lag:         {self._current_lag}s")
        log.info("=" * 70)


# ===========================================================================
# Entry point
# ===========================================================================

async def main() -> None:
    """CLI entry point — never exits permanently.

    If any fatal error crashes the daemon, it logs the error, waits 30 s,
    and restarts the entire connection + ingestion cycle. Only Ctrl-C or
    SIGTERM will stop it for good.
    """
    shutdown_requested = False

    def _signal_handler(signum, _frame):
        nonlocal shutdown_requested
        sig_name = signal.Signals(signum).name
        log.warning(f"Received {sig_name} — initiating shutdown …")
        shutdown_requested = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    restart_count = 0

    while not shutdown_requested:
        db = DB()
        client = TurnClient(TURN_BEARER_TOKEN, TURN_API_URL)
        ingestor = ContinuousIngestor(db, client)

        # Wire shutdown signal to ingestor
        original_handler = signal.getsignal(signal.SIGINT)

        def _ingestor_signal_handler(signum, frame):
            nonlocal shutdown_requested
            shutdown_requested = True
            ingestor.shutdown_event.set()

        signal.signal(signal.SIGINT, _ingestor_signal_handler)
        signal.signal(signal.SIGTERM, _ingestor_signal_handler)

        try:
            if restart_count > 0:
                log.info(f"🔄 RESTARTING daemon (restart #{restart_count})...")

            await db.connect()
            await ingestor.start()

        except KeyboardInterrupt:
            log.warning("\n⚠️  Interrupted by user")
            shutdown_requested = True
        except Exception as exc:
            log.error(f"\n❌ Fatal error: {exc}", exc_info=True)
            if not shutdown_requested:
                restart_count += 1
                wait = min(30, 5 * restart_count)
                log.warning(
                    f"🔁 Daemon will restart in {wait}s (restart #{restart_count})..."
                )
                await asyncio.sleep(wait)
        finally:
            try:
                await client.close()
            except Exception:
                pass
            try:
                await db.close()
            except Exception:
                pass
            log.info("✓ Cleanup complete")

    log.info("👋 Daemon stopped.")


if __name__ == "__main__":
    asyncio.run(main())
