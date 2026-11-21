"""PostgreSQL LSN helpers.

An LSN (log sequence number) is a 64-bit WAL position rendered by PostgreSQL
as ``HIGH/LOW`` hexadecimal 32-bit words, for example ``0/16B6C50``.
"""

from __future__ import annotations

import math
import re

_LSN_RE = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")
# PostgreSQL uses uint64 LSNs, but SQLite INTEGER is signed 64-bit.  Drill
# databases are far below this ceiling, so reject it explicitly instead of
# letting SQLite silently store a float.
_MAX_LSN = (1 << 63) - 1


def parse_lsn(value: str | int) -> int:
    """Parse a PostgreSQL LSN string (or plain integer) into an int."""
    if isinstance(value, bool):
        raise ValueError("LSN 不能是布尔值")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("LSN 不能为空")
        match = _LSN_RE.match(text)
        if match:
            high = int(match.group(1), 16)
            low = int(match.group(2), 16)
            result = (high << 32) | low
        elif text.isdigit():
            # Plain decimal is accepted for convenience in drills.
            result = int(text, 10)
        else:
            raise ValueError(
                f"LSN 格式不合法: {value!r}，应形如 '0/16B6C50'（十六进制 HIGH/LOW）"
            )
    else:
        raise ValueError(f"LSN 类型不合法: {type(value).__name__}")
    if not 0 <= result <= _MAX_LSN:
        raise ValueError("LSN 超出 64 位无符号整数范围")
    return result


def format_lsn(value: int) -> str:
    """Render an integer as PostgreSQL's canonical ``HIGH/LOW`` form."""
    if not 0 <= value <= _MAX_LSN:
        raise ValueError("LSN 超出 64 位无符号整数范围")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def normalize_lsn(value: str | int) -> str:
    return format_lsn(parse_lsn(value))


def human_bytes(value: int) -> str:
    """Binary byte formatting used in operator-facing responses."""
    if value < 0:
        raise ValueError("字节数不能为负")
    if value == 0:
        return "0 B"
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    exponent = min(int(math.log(value, 1024)), len(units) - 1)
    amount = value / (1024**exponent)
    if exponent == 0:
        return f"{value} B"
    return f"{amount:.1f} {units[exponent]}"
