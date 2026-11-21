"""Injective clock so tests can advance time without sleeping."""

from __future__ import annotations

import time


def now() -> int:
    return int(time.time())
