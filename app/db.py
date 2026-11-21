from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import clock
from .config import Settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS cluster (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    wal_boundary_lsn INTEGER NOT NULL CHECK (wal_boundary_lsn >= 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS slots (
    name TEXT PRIMARY KEY,
    slot_type TEXT NOT NULL CHECK (slot_type IN ('logical', 'physical')),
    restart_lsn INTEGER NOT NULL CHECK (restart_lsn >= 0),
    confirmed_flush_lsn INTEGER CHECK (confirmed_flush_lsn >= 0),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    disabled INTEGER NOT NULL CHECK (disabled IN (0, 1)),
    wal_status TEXT NOT NULL CHECK (wal_status IN ('reserved', 'extended', 'unreserved', 'lost')),
    plugin TEXT,
    note TEXT,
    updated_at INTEGER NOT NULL,
    CHECK (NOT (active = 1 AND disabled = 1)),
    CHECK (slot_type = 'physical' OR confirmed_flush_lsn IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    name TEXT PRIMARY KEY,
    slot_name TEXT REFERENCES slots(name) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('online', 'stalled', 'disabled')),
    confirmed_lsn INTEGER NOT NULL CHECK (confirmed_lsn >= 0),
    note TEXT,
    updated_at INTEGER NOT NULL,
    CHECK (status = 'disabled' OR slot_name IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS previews (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (
        status IN ('preview', 'applying', 'completed', 'failed', 'rejected', 'expired', 'cancelled')
    ),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    revision_at_preview INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    request_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    result_json TEXT,
    reject_reason TEXT,
    reject_detail_json TEXT,
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_previews_status ON previews(status);
CREATE INDEX IF NOT EXISTS idx_previews_created ON previews(created_at DESC);

CREATE TABLE IF NOT EXISTS demo_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    preview_id TEXT,
    detail_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_at ON events(at DESC);
"""


def utc_now() -> int:
    return clock.now()


def connect(settings: Settings) -> sqlite3.Connection:
    settings.ensure_parent()
    conn = sqlite3.connect(
        str(settings.database_path),
        timeout=max(settings.busy_timeout_ms / 1000, 0.1),
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {settings.busy_timeout_ms}")
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)


def close(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA optimize")
    except sqlite3.Error:
        pass
    conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    else:
        conn.execute("COMMIT")


def record_event(
    conn: sqlite3.Connection,
    event_type: str,
    *,
    preview_id: str | None = None,
    detail: dict | None = None,
    at: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO events (at, event_type, preview_id, detail_json) VALUES (?, ?, ?, ?)",
        (at if at is not None else utc_now(), event_type, preview_id, json.dumps(detail or {}, ensure_ascii=False)),
    )


# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

WAL_BOUNDARY = 0x2000000  # 32 MiB

SEED_SLOTS = [
    # The oldest slot pins WAL and is explicitly disabled/inactive: cleanable.
    {
        "name": "slot_old_metrics",
        "slot_type": "logical",
        "restart_lsn": 0x1000000,  # boundary - 16 MiB
        "confirmed_flush_lsn": 0x1F00000,
        "active": 0,
        "disabled": 1,
        "wal_status": "reserved",
        "plugin": "pgoutput",
        "note": "旧监控管线，已完成迁移，等待演练清理",
    },
    # Active logical slot used by an online subscription: blocked twice over.
    {
        "name": "slot_orders",
        "slot_type": "logical",
        "restart_lsn": 0x1500000,  # boundary - 11 MiB
        "confirmed_flush_lsn": 0x1F80000,
        "active": 1,
        "disabled": 0,
        "wal_status": "reserved",
        "plugin": "pgoutput",
        "note": "订单 CDC，在线订阅",
    },
    # Active slot with subscriber lag; target of the stall/recover injection.
    {
        "name": "slot_audit",
        "slot_type": "logical",
        "restart_lsn": 0x1400000,  # boundary - 12 MiB
        "confirmed_flush_lsn": 0x1900000,
        "active": 1,
        "disabled": 0,
        "wal_status": "reserved",
        "plugin": "pgoutput",
        "note": "审计日志，订阅有滞后",
    },
    # Disabled/inactive slot referenced only by a disabled subscription.
    # Permitted cleanup; the reference is detached atomically.
    {
        "name": "slot_legacy",
        "slot_type": "logical",
        "restart_lsn": 0x1800000,  # boundary - 8 MiB
        "confirmed_flush_lsn": 0x1F00000,
        "active": 0,
        "disabled": 1,
        "wal_status": "reserved",
        "plugin": "pgoutput",
        "note": "遗留回填，已停止，订阅已停用",
    },
    # Inactive physical slot that was never explicitly disabled: blocked.
    {
        "name": "slot_standby",
        "slot_type": "physical",
        "restart_lsn": 0x1A00000,  # boundary - 6 MiB
        "confirmed_flush_lsn": None,
        "active": 0,
        "disabled": 0,
        "wal_status": "reserved",
        "plugin": None,
        "note": "只读备库物理槽，仍在使用",
    },
]

SEED_SUBSCRIPTIONS = [
    {
        "name": "sub_orders",
        "slot_name": "slot_orders",
        "status": "online",
        "confirmed_lsn": 0x1F80000,
        "note": "订单 CDC 在线订阅",
    },
    {
        "name": "sub_audit",
        "slot_name": "slot_audit",
        "status": "online",
        "confirmed_lsn": 0x1900000,
        "note": "审计订阅（可注入停顿/恢复）",
    },
    {
        "name": "sub_legacy",
        "slot_name": "slot_legacy",
        "status": "disabled",
        "confirmed_lsn": 0x1F00000,
        "note": "已停用；清理 slot_legacy 时会被解绑",
    },
]


def ensure_cluster(conn: sqlite3.Connection, wal_boundary_lsn: int = WAL_BOUNDARY) -> None:
    """Guarantee the singleton cluster row exists, without touching inventory."""
    now = utc_now()
    with transaction(conn):
        conn.execute(
            "INSERT OR IGNORE INTO cluster (id, wal_boundary_lsn, revision, updated_at) VALUES (1, ?, 1, ?)",
            (wal_boundary_lsn, now),
        )


def seed(conn: sqlite3.Connection, *, reset: bool = False) -> None:
    now = utc_now()
    with transaction(conn):
        if reset:
            conn.execute("DELETE FROM previews")
            conn.execute("DELETE FROM events")
            conn.execute("DELETE FROM subscriptions")
            conn.execute("DELETE FROM slots")
            conn.execute("DELETE FROM demo_state")
            conn.execute("DELETE FROM cluster")
        else:
            # Normal startup: only seed a fresh database, never resurrect
            # slots an operator deliberately cleaned.  Existence of the
            # cluster row is not a good signal (it is created eagerly by
            # ensure_cluster); the meta flag is.
            seeded = conn.execute(
                "SELECT value FROM demo_state WHERE key = 'seeded'"
            ).fetchone()
            if seeded is not None:
                return
        conn.execute(
            "INSERT OR IGNORE INTO cluster (id, wal_boundary_lsn, revision, updated_at) VALUES (1, ?, 1, ?)",
            (WAL_BOUNDARY, now),
        )
        for slot in SEED_SLOTS:
            conn.execute(
                """
                INSERT INTO slots
                    (name, slot_type, restart_lsn, confirmed_flush_lsn, active, disabled,
                     wal_status, plugin, note, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slot["name"],
                    slot["slot_type"],
                    slot["restart_lsn"],
                    slot["confirmed_flush_lsn"],
                    slot["active"],
                    slot["disabled"],
                    slot["wal_status"],
                    slot["plugin"],
                    slot["note"],
                    now,
                ),
            )
        for sub in SEED_SUBSCRIPTIONS:
            conn.execute(
                """
                INSERT INTO subscriptions (name, slot_name, status, confirmed_lsn, note, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    sub["name"],
                    sub["slot_name"],
                    sub["status"],
                    sub["confirmed_lsn"],
                    sub["note"],
                    now,
                ),
            )
        write_demo_state(conn, "seeded", "1")
        record_event(conn, "demo_seeded" if reset else "seeded", detail={"slots": len(SEED_SLOTS)})


def bump_revision(conn: sqlite3.Connection, now: int | None = None) -> int:
    conn.execute(
        "UPDATE cluster SET revision = revision + 1, updated_at = ? WHERE id = 1",
        (now if now is not None else utc_now(),),
    )
    row = conn.execute("SELECT revision FROM cluster WHERE id = 1").fetchone()
    return int(row["revision"])


def read_demo_state(conn: sqlite3.Connection) -> dict[str, str]:
    return {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM demo_state")}


def write_demo_state(conn: sqlite3.Connection, key: str, value: str | None) -> None:
    if value is None:
        conn.execute("DELETE FROM demo_state WHERE key = ?", (key,))
    else:
        conn.execute(
            "INSERT INTO demo_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

