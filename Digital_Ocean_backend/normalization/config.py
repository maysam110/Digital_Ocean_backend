#!/usr/bin/env python3
"""
Normalization Worker — Configuration & Logging

Loads all environment variables with defaults and configures
JSON structured logging for container-friendly log output.
"""

import json
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


# ═══════════════════════════════════════════════════════════════════════
# Environment Configuration
# ═══════════════════════════════════════════════════════════════════════

DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", "500"))
POLL_INTERVAL_SECONDS: int = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
EMPTY_POLL_INTERVAL_SECONDS: int = int(os.environ.get("EMPTY_POLL_INTERVAL_SECONDS", "15"))
START_FROM_ID: int = int(os.environ.get("START_FROM_ID", "0"))
BUSINESS_NUMBER_ID: int = int(os.environ.get("BUSINESS_NUMBER_ID", "1"))
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()
DB_POOL_MIN: int = int(os.environ.get("DB_POOL_MIN", "2"))
DB_POOL_MAX: int = int(os.environ.get("DB_POOL_MAX", "10"))

# Retry delays for DB connection failures (seconds)
RETRY_DELAYS: list[int] = [2, 4, 8, 16, 32]


# ═══════════════════════════════════════════════════════════════════════
# JSON Structured Logging
# ═══════════════════════════════════════════════════════════════════════

class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON for container log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
        }

        # If the message is a dict, merge it into the log entry
        if isinstance(record.msg, dict):
            log_entry.update(record.msg)
            # Ensure a "message" key exists (some dict logs may not include one)
            if "message" not in log_entry and "event" not in log_entry:
                log_entry["message"] = ""
        else:
            log_entry["message"] = record.getMessage()

        return json.dumps(log_entry, default=str)


def setup_logging() -> logging.Logger:
    """Configure and return the root logger with JSON formatting."""
    logger = logging.getLogger("normalization")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Remove existing handlers to avoid duplicates on re-init
    logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoid duplicate lines)
    logger.propagate = False

    return logger
