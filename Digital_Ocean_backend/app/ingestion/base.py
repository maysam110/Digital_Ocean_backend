#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared Infrastructure for Turn.io Ingestion Workers

This module contains shared components extracted from:
  - continuous_ingest.py (messages)
  - ingest_contacts.py (contacts)

Components:
  - AsyncRateLimiter   — token bucket rate limiter
  - CursorExpiredError — exception for expired API cursors
  - DB                 — asyncpg connection pool with infinite retry
  - TurnClient         — Turn.io Data Export API client (messages)
  - TurnContactClient  — Turn.io Data Export API client (contacts)
  - extract_message_timestamp / _safe_parse_ts — timestamp helpers

NO LOGIC WAS CHANGED — only moved here for shared use.
"""

import asyncio
import datetime
import json
import logging
import os
import sys
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import asyncpg
import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging (defined early so all module-level code can use `log`)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ingestion")

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------
load_dotenv()

TURN_API_URL = os.getenv("TURNIO_BASE_URL")
TURN_BEARER_TOKEN = os.getenv("TURNIO_BEARER_TOKEN")
if not TURN_BEARER_TOKEN:
    log.warning(
        "⚠️  TURNIO_BEARER_TOKEN not set — ingestion workers will fail to start, "
        "but webhook handler will remain available."
    )

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# Contact-specific ingestion constants
# ---------------------------------------------------------------------------
MAX_RETRIES = 8                  # universal retry limit
CURSOR_MAX_RETRIES = 10          # cursor-expiry retries per chunk
NUM_CHUNKS = int(os.getenv("INGEST_NUM_CHUNKS", "24"))
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "1000"))
NUM_CONSUMERS = int(os.getenv("INGEST_NUM_CONSUMERS", "5"))
CHUNK_LAUNCH_DELAY_S = 1         # delay between each chunk launch
PRODUCER_QUEUE_SIZE = int(os.getenv("INGEST_PRODUCER_QUEUE_SIZE", "50000"))
CONSUMER_QUEUE_SIZE = int(os.getenv("INGEST_CONSUMER_QUEUE_SIZE", "30000"))
SEQUENTIAL_RETRY_TIMEOUT_S = int(os.getenv("INGEST_RETRY_TIMEOUT", "600"))  # 10 min

# Continuous Mode Constants
ERROR_BACKOFF_S = 300  # 5 minutes wait on error
CONTACT_INTERVAL_S = int(os.getenv("CONTACT_INTERVAL_MINUTES", "60")) * 60
STARTUP_STAGGER_S = int(os.getenv("STARTUP_STAGGER_SECONDS", "10"))

# Fixed start date for the startup backfill and initial checkpoint.
# On every service start, data is fetched from this date to NOW.
def _get_startup_date() -> str:
    """Read and normalize the startup date from environment."""
    # Check for generic STARTUP_FROM_DATE first, then legacy CONTACT_STARTUP_FROM_DATE
    raw = os.getenv("STARTUP_FROM_DATE") or os.getenv("CONTACT_STARTUP_FROM_DATE", "2024-01-01")
    
    # CRITICAL: Strip out inline comments (e.g., "2024-01-01 # comment")
    if "#" in raw:
        raw = raw.split("#")[0]
        
    raw = raw.strip()
    # Handle YYYY-MM-DD
    if len(raw) == 10:
        return f"{raw}T00:00:00Z"
    return raw

STARTUP_FROM_DATE: str = _get_startup_date()


# ===========================================================================
# Async Rate Limiter
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
        while True:
            async with self.lock:
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
# Turn.io API Client — Messages (from continuous_ingest.py)
# ===========================================================================
class TurnClient:
    """Async HTTP client for Turn.io Data Export API with pagination."""

    def __init__(self, token: str, base_url: str, rate_limiter: Optional[AsyncRateLimiter] = None):
        if not token:
            raise RuntimeError(
                "TURNIO_BEARER_TOKEN is required for TurnClient. "
                "Set the TURNIO_BEARER_TOKEN environment variable."
            )
        if not base_url:
            raise RuntimeError(
                "TURNIO_BASE_URL is required for TurnClient. "
                "Set the TURNIO_BASE_URL environment variable."
            )
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
        self.rate_limiter = rate_limiter or AsyncRateLimiter(rate=20, period=1.0, burst=50)
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
# Turn.io API Client — Contacts (from ingest_contacts.py)
# ===========================================================================
class TurnContactClient:
    """Async HTTP client for Turn.io Data Export API — contacts endpoint."""

    def __init__(self, token: str, base_url: str, rate_limiter: Optional[AsyncRateLimiter] = None):
        if not token:
            raise RuntimeError(
                "TURNIO_BEARER_TOKEN is required for TurnContactClient. "
                "Set the TURNIO_BEARER_TOKEN environment variable."
            )
        if not base_url:
            raise RuntimeError(
                "TURNIO_BASE_URL is required for TurnContactClient. "
                "Set the TURNIO_BASE_URL environment variable."
            )
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.v1+json",
        }
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10
            ),
        )
        self.limiter = rate_limiter or AsyncRateLimiter(rate=20, period=1.0, burst=50)
        self.semaphore = asyncio.Semaphore(12)

    async def _create_contacts_cursor(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
    ) -> str:
        """
        Create a cursor for Turn.io Data Export API contacts endpoint.

        Args:
            from_date: ISO datetime string (e.g., "2024-01-01T00:00:00Z")
            until_date: ISO datetime string (e.g., "2024-12-31T23:59:59Z")

        Returns:
            cursor: Cursor ID to use for pagination
        """
        await self.limiter.acquire()

        url = f"{self.base_url}/v1/data/contacts/cursor"
        # Turn.io contacts cursor only accepts "ordering", "from", "until".
        # page_size and scrubbing_rules are NOT supported for the contacts
        # endpoint (they cause 400 Bad Request). Verified empirically.
        body = {"ordering": "asc"}
        if from_date:
            body["from"] = from_date
        if until_date:
            body["until"] = until_date

        max_retries = MAX_RETRIES
        for attempt in range(max_retries):
            try:
                response = await self.client.post(url, json=body)

                if response.status_code == 403:
                    if attempt < max_retries - 1:
                        wait = min(5 * (2 ** attempt), 60)
                        log.warning(
                            f"⚠️ 403 creating cursor (attempt {attempt + 1}/{max_retries}). "
                            f"Possible rate limit or transient issue. Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        await self.limiter.acquire()
                        continue
                    log.error("=" * 70)
                    log.error("❌ 403 FORBIDDEN — CONTACTS DATA EXPORT NOT PERMITTED")
                    log.error("=" * 70)
                    log.error("")
                    log.error("Your Turn.io API token does not have permission to")
                    log.error("access the contacts Data Export API.")
                    log.error("")
                    log.error("Possible reasons:")
                    log.error("1. Your Turn.io plan does not include Data Export for contacts")
                    log.error("2. The API token lacks the required scope/permissions")
                    log.error("3. Too many parallel cursor requests (rate limiting)")
                    log.error("4. Contacts export may need to be enabled by Turn.io support")
                    log.error("")
                    log.error("Next steps:")
                    log.error("→ Contact Turn.io support: https://www.turn.io/contact")
                    log.error("→ Ask about enabling contacts in the Data Export API")
                    log.error("→ Verify your API token has the correct permissions")
                    log.error("=" * 70)
                    raise RuntimeError(
                        "403 Forbidden — Turn.io contacts Data Export API not permitted "
                        f"after {max_retries} attempts."
                    )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "30"))
                    log.warning(
                        f"Rate limited creating cursor (attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {retry_after}s..."
                    )
                    await asyncio.sleep(retry_after)
                    await self.limiter.acquire()
                    continue

                response.raise_for_status()
                data = response.json()
                cursor = data.get("cursor")
                if not cursor:
                    raise ValueError(f"No cursor returned from contacts API: {data}")
                log.info(f"✓ Contacts cursor created: {cursor[:20]}...")
                return cursor

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403 and attempt < max_retries - 1:
                    wait = min(5 * (2 ** attempt), 60)
                    log.warning(
                        f"⚠️ 403 via HTTPStatusError (attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {wait}s..."
                    )
                    await asyncio.sleep(wait)
                    await self.limiter.acquire()
                    continue
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    retry_after = int(e.response.headers.get("Retry-After", "30"))
                    log.warning(f"Rate limited (attempt {attempt + 1}). Waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    await self.limiter.acquire()
                    continue
                raise
            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    log.warning(f"Connection error (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                raise

        raise RuntimeError("Failed to create contacts cursor after max retries")

    async def _fetch_contacts_page(self, cursor: str) -> Dict[str, Any]:
        """
        Fetch a page of contacts using Turn.io Data Export API cursor.

        Args:
            cursor: Cursor ID

        Returns:
            Response dict with data and optional next cursor
        """
        await self.limiter.acquire()

        url = f"{self.base_url}/v1/data/contacts/cursor/{cursor}"

        max_retries = 5
        base_delay = 2
        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await self.client.get(url)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "30"))
                    log.warning(
                        f"Rate limited fetching contacts page (attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {retry_after}s..."
                    )
                    await asyncio.sleep(retry_after)
                    await self.limiter.acquire()
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                # ── Cursor expired → signal caller to re-create ───────
                if e.response.status_code == 400:
                    body = e.response.text.lower()
                    if "cursor" in body and "expired" in body:
                        log.warning("⏰ Contacts cursor expired — will re-create")
                        raise CursorExpiredError("Cursor has expired") from e
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    retry_after = int(e.response.headers.get("Retry-After", "30"))
                    log.warning(f"Rate limited (attempt {attempt + 1}). Waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    await self.limiter.acquire()
                    continue
                elif e.response.status_code == 404:
                    log.error("❌ Turn.io contacts Data Export API not available (404)")
                    raise RuntimeError(
                        "Turn.io Data Export API (contacts) not accessible."
                    )
                else:
                    raise
            except (httpx.ConnectError, httpx.ReadTimeout,
                    httpx.ConnectTimeout, httpx.ReadError,
                    httpx.TimeoutException, OSError) as e:
                wait_time = min(base_delay * (2 ** attempt), 60)
                if attempt < max_retries - 1:
                    log.warning(
                        f"🔌 Connection error (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {wait_time}s: {type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    log.error(f"❌ Failed to fetch contacts page after {max_retries} attempts")
                    raise

        raise RuntimeError("Failed to fetch contacts page after max retries")

    async def get_contacts_parallel(
        self,
        from_date: str,
        until_date: str,
        num_chunks: int = NUM_CHUNKS,
    ) -> AsyncGenerator[Dict, None]:
        """
        PARALLEL MULTI-CURSOR FETCHING for contacts.

        Strategy:
        1. Split date range into N chunks
        2. Stagger cursor creation with per-chunk delay to avoid 403 floods
        3. Drain queue concurrently while tasks run
        4. Retry failed chunks sequentially as fallback
        5. Send sentinel after all retries complete

        Args:
            from_date: Start date (ISO format)
            until_date: End date (ISO format)
            num_chunks: Number of parallel cursors
        """

        start = datetime.datetime.fromisoformat(from_date.replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(until_date.replace("Z", "+00:00"))
        total_seconds = (end - start).total_seconds()
        chunk_seconds = total_seconds / num_chunks

        chunks = []
        for i in range(num_chunks):
            chunk_start = start + datetime.timedelta(seconds=chunk_seconds * i)
            chunk_end = start + datetime.timedelta(seconds=chunk_seconds * (i + 1))
            if i == num_chunks - 1:
                chunk_end = end
            chunks.append((
                chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ))

        log.info(f"\n📋 Splitting contact fetch into {num_chunks} parallel chunks:")
        for i, (cs, ce) in enumerate(chunks):
            log.info(f"   Chunk {i}: {cs} → {ce}")

        fetch_queue: asyncio.Queue = asyncio.Queue(maxsize=PRODUCER_QUEUE_SIZE)
        active_chunks = {"count": num_chunks}
        chunk_lock = asyncio.Lock()
        failed_chunks: List[Tuple[int, str, str]] = []  # (chunk_id, from, until)
        failed_lock = asyncio.Lock()

        async def fetch_chunk(chunk_id: int, chunk_from: str, chunk_until: str):
            """Fetch one chunk with cursor expiration recovery."""
            chunk_count = 0
            cursor_retries = 0
            current_from = chunk_from

            try:
                cursor = await self._create_contacts_cursor(current_from, chunk_until)
                page_count = 0

                while cursor:
                    try:
                        data = await self._fetch_contacts_page(cursor)
                    except CursorExpiredError:
                        cursor_retries += 1
                        if cursor_retries > CURSOR_MAX_RETRIES:
                            log.error(f"  ❌ Chunk-{chunk_id}: cursor expired {cursor_retries} times")
                            break
                        log.warning(
                            f"  🔄 Chunk-{chunk_id}: re-creating cursor from {current_from} "
                            f"(attempt {cursor_retries}/{CURSOR_MAX_RETRIES})"
                        )
                        cursor = await self._create_contacts_cursor(current_from, chunk_until)
                        continue

                    cursor_retries = 0  # reset on success

                    contacts = data.get("data", [])
                    if not contacts:
                        contacts = data.get("contacts", [])

                    for contact in contacts:
                        if fetch_queue.full():
                            log.warning(f"  ⚠️ Chunk-{chunk_id}: Queue full ({fetch_queue.qsize()} items), waiting for consumers...")
                        
                        await fetch_queue.put(contact)
                        chunk_count += 1
                        # Track latest timestamp for cursor recovery
                        ts = contact.get("updated_at") or contact.get("inserted_at")
                        if ts:
                            current_from = ts

                    page_count += 1

                    paging = data.get("paging") or {}
                    cursor = paging.get("next")

                    if chunk_count % 500 == 0 and chunk_count > 0:
                        log.info(
                            f"  Chunk-{chunk_id}: {chunk_count} contacts fetched ({page_count} pages)..."
                        )

                log.info(
                    f"  ✓ Chunk-{chunk_id} complete: {chunk_count} contacts in {page_count} pages"
                )

            except Exception as e:
                log.error(f"  ❌ Chunk-{chunk_id} failed: {e}")
                # Record failed chunk for sequential retry
                async with failed_lock:
                    failed_chunks.append((chunk_id, chunk_from, chunk_until))

            finally:
                # Use sys.stderr in finally to avoid NameError during shutdown
                try:
                    async with chunk_lock:
                        active_chunks["count"] -= 1
                except (RuntimeError, asyncio.CancelledError):
                    pass
                except Exception as e:
                    try:
                        sys.stderr.write(f"Error in chunk cleanup: {e}\n")
                    except:
                        pass

        # --- PHASE A: Stagger each chunk launch individually ---
        tasks = []
        for idx, (cs, ce) in enumerate(chunks):
            tasks.append(asyncio.create_task(fetch_chunk(idx, cs, ce)))
            if idx < len(chunks) - 1:
                await asyncio.sleep(CHUNK_LAUNCH_DELAY_S)

        # Drain queue concurrently while tasks are still running
        while True:
            try:
                contact = await asyncio.wait_for(fetch_queue.get(), timeout=2.0)
                if contact is None:  # sentinel
                    break
                yield contact
            except asyncio.TimeoutError:
                # Check if all parallel chunks are done and queue is empty
                async with chunk_lock:
                    if active_chunks["count"] == 0 and fetch_queue.empty():
                        break
                continue

        # Wait for all parallel tasks to finish cleanly
        await asyncio.gather(*tasks, return_exceptions=True)

        # Drain any remaining items left in the queue after gather
        while not fetch_queue.empty():
            contact = fetch_queue.get_nowait()
            if contact is not None:
                yield contact

        # --- PHASE B: Retry failed chunks sequentially (with timeout) ---
        if failed_chunks:
            log.warning(
                f"\n🔄 {len(failed_chunks)} chunk(s) failed. "
                f"Retrying sequentially (timeout: {SEQUENTIAL_RETRY_TIMEOUT_S}s)..."
            )
            retry_start = time.monotonic()

            for chunk_id, chunk_from, chunk_until in failed_chunks:
                # Check overall retry timeout
                elapsed = time.monotonic() - retry_start
                if elapsed > SEQUENTIAL_RETRY_TIMEOUT_S:
                    log.error(
                        f"  ⏱️ Sequential retry timeout ({SEQUENTIAL_RETRY_TIMEOUT_S}s) exceeded. "
                        f"Skipping remaining failed chunks."
                    )
                    break

                log.info(f"  🔄 Retrying Chunk-{chunk_id}: {chunk_from} → {chunk_until}")
                retry_count = 0
                current_from = chunk_from
                try:
                    # Wait before retry to let rate limits clear
                    await asyncio.sleep(5)
                    cursor = await self._create_contacts_cursor(current_from, chunk_until)
                    page_count = 0
                    chunk_contact_count = 0

                    while cursor:
                        # Check timeout inside retry loop too
                        if time.monotonic() - retry_start > SEQUENTIAL_RETRY_TIMEOUT_S:
                            log.warning(f"  ⏱️ Timeout during Chunk-{chunk_id} retry. Stopping.")
                            break

                        try:
                            data = await self._fetch_contacts_page(cursor)
                        except CursorExpiredError:
                            retry_count += 1
                            if retry_count > CURSOR_MAX_RETRIES:
                                log.error(f"  ❌ Chunk-{chunk_id} retry: cursor expired too many times")
                                break
                            cursor = await self._create_contacts_cursor(current_from, chunk_until)
                            continue

                        retry_count = 0
                        contacts = data.get("data", [])
                        if not contacts:
                            contacts = data.get("contacts", [])

                        for contact in contacts:
                            yield contact
                            chunk_contact_count += 1
                            ts = contact.get("updated_at") or contact.get("inserted_at")
                            if ts:
                                current_from = ts

                        page_count += 1
                        paging = data.get("paging") or {}
                        cursor = paging.get("next")

                        if chunk_contact_count % 500 == 0 and chunk_contact_count > 0:
                            log.info(
                                f"  Chunk-{chunk_id} (retry): {chunk_contact_count} contacts "
                                f"({page_count} pages)..."
                            )

                    log.info(
                        f"  ✓ Chunk-{chunk_id} (retry) complete: "
                        f"{chunk_contact_count} contacts in {page_count} pages"
                    )
                except Exception as e:
                    log.error(f"  ❌ Chunk-{chunk_id} retry also failed: {e}")

    async def get_contacts(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
    ) -> AsyncGenerator[Dict, None]:
        """
        Sequential single-cursor contact fetch (manual/debug fallback).

        Use get_contacts_parallel() for production ingestion.
        This method is useful for testing, debugging, or small date ranges.

        Args:
            from_date: ISO datetime string
            until_date: ISO datetime string (optional, defaults to now)

        Yields:
            Contact dictionaries
        """
        if not from_date:
            from_date = "2024-01-01T00:00:00Z"
        if not until_date:
            until_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        log.info(f"\n📅 Fetching contacts from {from_date} to {until_date}")

        try:
            cursor = await self._create_contacts_cursor(from_date, until_date)
        except Exception as e:
            log.error(f"Failed to create contacts cursor: {e}")
            raise

        page_count = 0
        total_contacts = 0

        last_contact_ts = from_date
        max_cursor_retries = 10
        cursor_retries = 0

        while cursor:
            try:
                data = await self._fetch_contacts_page(cursor)
            except CursorExpiredError:
                cursor_retries += 1
                if cursor_retries > max_cursor_retries:
                    log.error(f"❌ Cursor expired {cursor_retries} times — giving up")
                    break
                log.warning(
                    f"🔄 Re-creating contacts cursor from {last_contact_ts} "
                    f"(attempt {cursor_retries}/{max_cursor_retries})"
                )
                cursor = await self._create_contacts_cursor(last_contact_ts, until_date)
                continue

            cursor_retries = 0

            contacts = data.get("data", [])
            if not contacts:
                contacts = data.get("contacts", [])

            for contact in contacts:
                yield contact
                total_contacts += 1
                ts = contact.get("updated_at") or contact.get("inserted_at")
                if ts:
                    last_contact_ts = ts

            page_count += 1
            log.info(
                f"  ✓ Page {page_count}: {len(contacts)} contacts "
                f"(Total: {total_contacts})"
            )

            paging = data.get("paging") or {}
            cursor = paging.get("next")

            if cursor:
                log.debug(f"Next cursor: {cursor[:20]}...")
            else:
                log.info(f"\n✅ Pagination complete!")
                log.info(f"   Total pages: {page_count}")
                log.info(f"   Total contacts: {total_contacts}")
                break

    async def close(self):
        await self.client.aclose()


# ===========================================================================
# Database Connection Pool (self-contained)
# ===========================================================================
class DB:
    """PostgreSQL connection pool manager with infinite retry."""

    def __init__(self):
        self.pool: Optional[asyncpg.pool.Pool] = None

    def accept_pool(self, pool: asyncpg.pool.Pool) -> None:
        """Accept an external connection pool instead of creating a new one."""
        self.pool = pool
        log.info("✓ Accepted external DB connection pool")

    async def connect(self):
        """Create connection pool. Retries forever until DB is reachable."""
        if self.pool is not None:
            log.info("✓ DB pool already set (external), skipping connect")
            return
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
                    max_size=15,
                    command_timeout=60,
                )
                log.info("✓ Connected to PostgreSQL")
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
