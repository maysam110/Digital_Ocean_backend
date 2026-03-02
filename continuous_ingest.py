#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Legacy compatibility entrypoint.

The ingestion runtime now lives under FastAPI lifespan (`app.main:app`) and
uses one shared DB pool (`max_size=1`) for both webhook and ingestion paths.
"""

import sys


def main() -> int:
    print(
        "continuous_ingest.py is deprecated. Run the service with:\n"
        "  uvicorn app.main:app --host 0.0.0.0 --port 8000"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
