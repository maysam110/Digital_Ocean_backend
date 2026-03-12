#!/usr/bin/env python3
"""
Normalization Worker — Pure Payload Parsers

All functions in this module are PURE — no database calls, no side effects.
They accept a raw payload dict and return typed dataclass instances or
primitive values ready for DB insertion.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID
import config

from models import (
    AttachmentRow,
    ChatRow,
    ChatStateRow,
    ContactRow,
    MessageRow,
    StatusRow,
)


# ═══════════════════════════════════════════════════════════════════════
# Phone Number Normalization (E.164)
# ═══════════════════════════════════════════════════════════════════════

def normalize_phone(raw: str | None) -> str | None:
    """
    Normalize any phone representation to E.164 format.

    E.164: + followed by country code and number, no spaces/dashes/parens.
    Examples:
        "923141851055"   → "+923141851055"
        "+923252762447"  → "+923252762447"
        "92 324 4838555" → "+923244838555"
    """
    if not raw:
        return None
    digits_only = re.sub(r"[^\d+]", "", raw.strip())
    if not digits_only.startswith("+"):
        digits_only = "+" + digits_only
    return digits_only


# ═══════════════════════════════════════════════════════════════════════
# Timestamp Parsing
# ═══════════════════════════════════════════════════════════════════════

def parse_timestamp(value: str | int | float | None) -> datetime | None:
    """
    Parse a raw timestamp into a timezone-aware UTC datetime.

    Handles:
        - Unix epoch in seconds (10-digit int/str)
        - Unix epoch in milliseconds (13-digit int/str)
        - ISO-8601 strings with Z or +00:00 suffix
        - None → None

    Never raises.
    """
    if value is None:
        return None

    # Numeric values (epoch)
    if isinstance(value, (int, float)):
        try:
            v = float(value)
            if v > 1e12:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (ValueError, TypeError, OSError, OverflowError):
            return None

    if isinstance(value, str):
        # Try ISO-8601 first
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        # Try as numeric string (Unix epoch)
        try:
            v = float(value)
            if v > 1e12:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (ValueError, TypeError, OSError, OverflowError):
            return None

    return None


# ═══════════════════════════════════════════════════════════════════════
# VND Block Guard
# ═══════════════════════════════════════════════════════════════════════

def has_valid_vnd(payload: dict) -> bool:
    """
    Validate that the payload has a well-formed _vnd.v1 block
    with chat and author sub-objects.

    Returns False if any required nesting is missing or wrong type.
    """
    vnd = payload.get("_vnd")
    if not isinstance(vnd, dict):
        return False
    v1 = vnd.get("v1")
    if not isinstance(v1, dict):
        return False
    if not isinstance(v1.get("chat"), dict):
        return False
    if not isinstance(v1.get("author"), dict):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# Content Extraction
# ═══════════════════════════════════════════════════════════════════════

# Maps Turn.io message types to our message_type_enum values
TYPE_MAP: dict[str, str] = {
    "text": "text",
    "image": "image",
    "video": "video",
    "audio": "audio",
    "document": "document",
    "interactive": "interactive",
    "reaction": "reaction",
    "sticker": "image",        # stickers stored as image type
    "button": "interactive",   # button replies stored as interactive
    "template": "text",        # templates stored as text type
    "location": "text",
    "contacts": "text",
    "system": "system",
}

# Media types that produce has_media = True
MEDIA_TYPES: set[str] = {"image", "video", "audio", "document", "sticker"}


def extract_content(payload: dict) -> str | None:
    """
    Extract the human-readable text content from a message payload.

    Priority order:
        1. template → template name (for analytics visibility)
        2. text.body
        3. image.caption / video.caption / document.caption
        4. button.text
        5. interactive.body.text
        6. Placeholder strings for media-only types
    """
    msg_type = payload.get("type")

    # Template: store the template name for analytics visibility
    if msg_type == "template":
        template = payload.get("template")
        if isinstance(template, dict):
            return template.get("name")

    extractors = [
        lambda p: _safe_get(p, "text", "body"),
        lambda p: _safe_get(p, "image", "caption"),
        lambda p: _safe_get(p, "video", "caption"),
        lambda p: _safe_get(p, "document", "caption"),
        lambda p: _safe_get(p, "button", "text"),
        # interactive body text (outbound menus)
        lambda p: _safe_get_nested(p, "interactive", "body", "text"),
        # interactive button reply (user tapped a button)
        lambda p: _safe_get_nested(p, "interactive", "button_reply", "title"),
        # interactive list reply (user selected a list item)
        lambda p: _safe_get_nested(p, "interactive", "list_reply", "title"),
        lambda p: "[sticker]" if p.get("sticker") else None,
        lambda p: "[location]" if p.get("location") else None,
        lambda p: "[audio]" if p.get("audio") else None,
        lambda p: "[reaction]" if p.get("reaction") else None,
    ]

    for fn in extractors:
        try:
            value = fn(payload)
            if value:
                return value
        except (AttributeError, TypeError):
            continue

    return None


def _safe_get(payload: dict, key1: str, key2: str):
    """Safely get payload[key1][key2] where key1 value may not be a dict."""
    obj = payload.get(key1)
    if isinstance(obj, dict):
        return obj.get(key2)
    return None


def _safe_get_nested(payload: dict, key1: str, key2: str, key3: str):
    """Safely get payload[key1][key2][key3]."""
    obj1 = payload.get(key1)
    if not isinstance(obj1, dict):
        return None
    obj2 = obj1.get(key2)
    if not isinstance(obj2, dict):
        return None
    return obj2.get(key3)


# ═══════════════════════════════════════════════════════════════════════
# Attachment Extraction
# ═══════════════════════════════════════════════════════════════════════

def extract_attachment(payload: dict) -> dict | None:
    """
    Extract attachment metadata from a media message.

    Returns None for non-media types.
    Returns {"filename": ..., "caption": ...} for media types.
    """
    msg_type = payload.get("type")
    if msg_type not in MEDIA_TYPES:
        return None

    media = payload.get(msg_type)
    if not isinstance(media, dict):
        return None

    filename = media.get("filename")

    # Voice note: audio where audio.voice = True
    if msg_type == "audio" and media.get("voice") is True:
        filename = filename or "voice_note"

    return {
        "filename": filename,
        "caption": media.get("caption"),
    }


# ═══════════════════════════════════════════════════════════════════════
# Row Parsers — Each returns a typed dataclass
# ═══════════════════════════════════════════════════════════════════════

def _safe_uuid(value) -> UUID | None:
    """Convert a string to UUID, returning None if invalid."""
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# Enum Pre-Validation
# ═══════════════════════════════════════════════════════════════════════

VALID_DIRECTIONS = {"inbound", "outbound"}
VALID_STATUSES = {"queued", "sent", "delivered", "read", "failed"}


def validate_direction(value: str | None) -> str:
    """Validate and return a message_direction enum value."""
    if value not in VALID_DIRECTIONS:
        raise ValueError(
            f"invalid_direction: '{value}' not in {VALID_DIRECTIONS}"
        )
    return value


def validate_status(value: str | None) -> str:
    """Validate and return a message_status_enum value."""
    if value not in VALID_STATUSES:
        raise ValueError(
            f"invalid_status: '{value}' not in {VALID_STATUSES}"
        )
    return value


def parse_contact(payload: dict) -> ContactRow:
    """
    Extract a ContactRow from the message payload.

    Uses _vnd.v1.chat.contact_uuid as the stable contact identifier.
    Direction determines which phone field represents the contact.
    Author type OWNER = real contact name; anything else = bot (name set to None).
    """
    v1 = payload["_vnd"]["v1"]
    chat = v1["chat"]
    author = v1["author"]
    direction = v1["direction"]

    # Determine the contact's phone number
    if direction == "inbound":
        raw_phone = payload.get("from")
    else:
        raw_phone = chat.get("owner")

    whatsapp_id = normalize_phone(raw_phone)

    # Only use human contact names — never overwrite with bot names
    author_type = (author.get("type") or "").upper()
    if author_type == "OWNER":
        profile_name = author.get("name")
    else:
        profile_name = None

    return ContactRow(
        uuid=_safe_uuid(chat["contact_uuid"]),
        number_id=int(payload.get("number_id", config.BUSINESS_NUMBER_ID)),
        whatsapp_id=whatsapp_id,
        whatsapp_profile_name=profile_name,
    )


def parse_chat(payload: dict, contact_id: int) -> ChatRow:
    """
    Extract a ChatRow from the message payload.

    Uses _vnd.v1.chat.uuid as the Turn.io chat identifier.
    State is lowercased to match our chat_state_enum.
    """
    v1 = payload["_vnd"]["v1"]
    chat = v1["chat"]

    state_raw = chat.get("state") or "open"

    assigned_to_raw = chat.get("assigned_to")
    assigned_to_uuid = _safe_uuid(assigned_to_raw)

    return ChatRow(
        uuid=_safe_uuid(chat["uuid"]),
        number_id=int(payload.get("number_id", config.BUSINESS_NUMBER_ID)),
        contact_id=contact_id,
        state=state_raw.lower(),
        state_reason=chat.get("state_reason"),
        owner=normalize_phone(chat.get("owner")),
        assigned_to_uuid=assigned_to_uuid,
        unread_count=int(chat.get("unread_count", 0) or 0),
        direction=v1["direction"],
        event_timestamp=parse_timestamp(payload.get("timestamp")),
        chat_inserted_at=parse_timestamp(chat.get("inserted_at")),
    )


def build_message_metadata(payload: dict) -> dict | None:
    """
    Build a metadata dict containing journey and author context.
    Returns None if no relevant fields are present.
    """
    author = payload.get("_vnd", {}).get("v1", {}).get("author", {})
    metadata = {}

    fields = [
        ("journey_uuid",      author.get("journey_uuid")),
        ("journey_name",      author.get("journey_name")),
        ("journey_card_uuid", author.get("journey_card_uuid")),
        ("block_id",          author.get("block_id")),
        ("session_id",        author.get("session_id")),
        ("block_is_ai",       author.get("block_is_ai")),
        ("request_id",        author.get("request_id")),
        ("author_type",       author.get("type")),
    ]

    for key, value in fields:
        if value is not None:
            metadata[key] = value

    return metadata if metadata else None


def extract_media_object(payload: dict) -> dict | None:
    """
    Extract the raw media block from the payload as a dict.
    Returns None for non-media message types.
    """
    msg_type = payload.get("type")
    media_types = {"image", "video", "audio", "document", "sticker"}
    if msg_type not in media_types:
        return None
    media = payload.get(msg_type)
    return media if isinstance(media, dict) else None


def parse_message(
    payload: dict,
    chat_id: int,
    campaign_id: UUID | None,
) -> MessageRow:
    """
    Extract a MessageRow from the message payload.

    Maps Turn.io type to our message_type_enum.
    Content extracted using priority-ordered extractors.
    """
    v1 = payload["_vnd"]["v1"]
    author = v1["author"]
    direction = validate_direction(v1["direction"])
    raw_type = payload.get("type", "text")

    # from_addr: for inbound = sender, for outbound = recipient
    if direction == "inbound":
        from_addr = normalize_phone(payload.get("from"))
    else:
        from_addr = normalize_phone(payload.get("to"))

    # C4c: rendered_content
    rendered_content = v1.get("rendered_content")

    # C4d: faq_uuid and card_uuid
    faq_uuid = _safe_uuid(v1.get("faq_uuid"))
    card_uuid = _safe_uuid(v1.get("card_uuid"))

    # C4a: message_metadata
    message_metadata = build_message_metadata(payload)

    # C4b: media_object
    media_object = extract_media_object(payload)

    return MessageRow(
        uuid=_safe_uuid(v1.get("uuid")),
        number_id=int(payload.get("number_id", config.BUSINESS_NUMBER_ID)),
        chat_id=chat_id,
        message_type=TYPE_MAP.get(raw_type, "text"),
        author=author.get("name"),
        author_type=(author.get("type") or "").lower(),
        from_addr=from_addr,
        content=extract_content(payload),
        rendered_content=rendered_content,
        direction=direction,
        external_id=payload["id"],
        external_timestamp=parse_timestamp(payload.get("timestamp")),
        last_status=v1.get("last_status"),
        last_status_timestamp=parse_timestamp(v1.get("last_status_timestamp")),
        has_media=raw_type in MEDIA_TYPES,
        faq_uuid=faq_uuid,
        card_uuid=card_uuid,
        message_metadata=message_metadata,
        media_object=media_object,
        campaign_id=campaign_id,
    )


def parse_status_from_message(
    payload: dict,
    message_id: int,
) -> StatusRow | None:
    """
    Extract an inline StatusRow from a message payload's _vnd.v1 block.

    Returns None if last_status is not set.
    """
    v1 = payload["_vnd"]["v1"]
    status = v1.get("last_status")
    if not status:
        return None

    validated_status = validate_status(status)

    return StatusRow(
        message_id=message_id,
        message_uuid=payload["id"],
        number_id=int(payload["number_id"]),
        status=validated_status,
        status_timestamp=parse_timestamp(v1.get("last_status_timestamp")),
        raw_body=None,  # will be set by caller with payload dict directly
    )




def parse_attachment(
    payload: dict,
    message_id: int,
    chat_id: int,
) -> AttachmentRow | None:
    """
    Extract an AttachmentRow from the message payload.

    Returns None for non-media message types.
    """
    att = extract_attachment(payload)
    if att is None:
        return None

    return AttachmentRow(
        message_id=message_id,
        chat_id=chat_id,
        number_id=int(payload["number_id"]),
        filename=att["filename"],
        caption=att["caption"],
    )


def parse_chat_state(payload: dict, chat_id: int) -> ChatStateRow:
    """
    Extract a ChatStateRow from the message payload.

    last_message_at uses _vnd.v1.inserted_at (the Turn.io server timestamp).
    Sets last_inbound_at or last_outbound_at based on direction.
    """
    v1 = payload["_vnd"]["v1"]
    chat = v1["chat"]
    direction = v1["direction"]  # already validated upstream
    message_at = parse_timestamp(v1.get("inserted_at"))

    return ChatStateRow(
        chat_id=chat_id,
        last_message_at=message_at,
        last_inbound_at=message_at if direction == "inbound" else None,
        last_outbound_at=message_at if direction == "outbound" else None,
        unread_count=int(chat.get("unread_count", 0) or 0),
        current_status=(chat.get("state") or "open").lower(),
    )
