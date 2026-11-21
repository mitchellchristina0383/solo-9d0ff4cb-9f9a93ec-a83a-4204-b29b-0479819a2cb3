from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 2**31 - 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数，当前为 {raw!r}") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 必须在 {minimum}..{maximum} 之间，当前为 {value}")
    return value


@dataclass(frozen=True)
class Settings:
    database_path: Path
    preview_ttl_seconds: int
    seed_demo: bool
    demo_enabled: bool
    busy_timeout_ms: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=Path(os.getenv("DATABASE_PATH", "./data/drill.db")).expanduser(),
            preview_ttl_seconds=_env_int("PREVIEW_TTL_SECONDS", 900, minimum=1, maximum=86400),
            seed_demo=_env_bool("SEED_DEMO", True),
            demo_enabled=_env_bool("ENABLE_DEMO", True),
            busy_timeout_ms=_env_int("SQLITE_BUSY_TIMEOUT_MS", 5000, minimum=100),
        )

    def ensure_parent(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
