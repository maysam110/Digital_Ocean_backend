#!/usr/bin/env python3
"""
Normalization Worker — Data Models

Typed dataclasses for all database row representations and worker state.
These are pure data containers — no database or I/O logic.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID


# ═══════════════════════════════════════════════════════════════════════
# Row Dataclasses
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class ContactRow:
    uuid: UUID
    number_id: int
    whatsapp_id: str
    whatsapp_profile_name: Optional[str] = None


@dataclass
class ChatRow:
    uuid: UUID
    number_id: int
    contact_id: int
    state: str
    state_reason: Optional[str] = None
    owner: Optional[str] = None
    assigned_to_uuid: Optional[UUID] = None
    unread_count: int = 0
    direction: str = "inbound"
    event_timestamp: Optional[datetime] = None
    chat_inserted_at: Optional[datetime] = None


@dataclass
class MessageRow:
    uuid: UUID
    number_id: int
    chat_id: int
    message_type: str
    author: Optional[str] = None
    author_type: Optional[str] = None
    from_addr: Optional[str] = None
    content: Optional[str] = None
    rendered_content: Optional[str] = None
    direction: str = "inbound"
    external_id: str = ""
    external_timestamp: Optional[datetime] = None
    last_status: Optional[str] = None
    last_status_timestamp: Optional[datetime] = None
    has_media: bool = False
    faq_uuid: Optional[UUID] = None
    card_uuid: Optional[UUID] = None
    message_metadata: Optional[dict] = None
    media_object: Optional[dict] = None
    campaign_id: Optional[UUID] = None


@dataclass
class StatusRow:
    message_id: int
    message_uuid: str
    number_id: int
    status: str
    status_timestamp: Optional[datetime] = None
    raw_body: Optional[dict] = None


@dataclass
class AttachmentRow:
    message_id: int
    chat_id: int
    number_id: int
    filename: Optional[str] = None
    caption: Optional[str] = None


@dataclass
class ChatStateRow:
    chat_id: int
    last_message_at: Optional[datetime] = None
    last_inbound_at: Optional[datetime] = None
    last_outbound_at: Optional[datetime] = None
    unread_count: int = 0
    current_status: str = "open"


# ═══════════════════════════════════════════════════════════════════════
# Worker State (runtime metrics)
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class WorkerState:
    """Tracks runtime metrics for heartbeat logging and monitoring."""

    last_event_id: int = 0
    total_processed: int = 0
    total_failures: int = 0
    start_time: float = field(default_factory=time.monotonic)

    # Rolling window for events-per-minute calculation (5-minute window)
    _throughput_samples: deque = field(
        default_factory=lambda: deque(maxlen=300)  # 5 min × 60 samples/min
    )

    def record_batch(self, count: int) -> None:
        """Record a batch completion for throughput tracking."""
        self._throughput_samples.append((time.monotonic(), count))

    def events_per_minute(self) -> float:
        """Calculate rolling 5-minute average events per minute."""
        if not self._throughput_samples:
            return 0.0

        now = time.monotonic()
        window_start = now - 300  # 5 minutes ago

        total = 0
        earliest = now
        for ts, count in self._throughput_samples:
            if ts >= window_start:
                total += count
                if ts < earliest:
                    earliest = ts

        elapsed_minutes = (now - earliest) / 60.0
        if elapsed_minutes < 0.01:  # avoid division by near-zero
            return 0.0
        return round(total / elapsed_minutes, 1)

    def uptime_seconds(self) -> int:
        """Return seconds since worker started."""
        return int(time.monotonic() - self.start_time)
