from __future__ import annotations

import sqlite3

import pytest

from app import db
from app.analysis import (
    build_cleanup_plan,
    build_inventory_view,
    fingerprint_snapshot,
    load_snapshot,
)
from app.config import Settings
from app.lsn import format_lsn, human_bytes, parse_lsn


@pytest.mark.parametrize(
    "text,expected",
    [("0/0", 0), ("0/1", 1), ("0/16B6C50", 23817296), ("1/0", 1 << 32), (" 0/A ", 10)],
)
def test_parse_lsn_valid(text, expected):
    assert parse_lsn(text) == expected
    assert parse_lsn(format_lsn(expected)) == expected


@pytest.mark.parametrize("text", ["", "nope", "1/2/3", "0x1/2", "GG/1"])
def test_parse_lsn_invalid(text):
    with pytest.raises(ValueError):
        parse_lsn(text)


def test_parse_lsn_rejects_bool_and_overflow():
    with pytest.raises(ValueError):
        parse_lsn(True)
    with pytest.raises(ValueError):
        parse_lsn("FFFFFFFF/FFFFFFFF")


def test_human_bytes():
    assert human_bytes(0) == "0 B"
    assert human_bytes(1536) == "1.5 KiB"
    assert human_bytes(16 * 1024 * 1024) == "16.0 MiB"


@pytest.fixture()
def conn(tmp_path):
    settings = Settings(
        database_path=tmp_path / "unit.db",
        preview_ttl_seconds=60,
        seed_demo=True,
        demo_enabled=True,
        busy_timeout_ms=1000,
    )
    connection = db.connect(settings)
    db.initialize(connection)
    db.seed(connection, reset=True)
    yield connection
    connection.close()


def test_occupancy_and_earliest_retention(conn):
    snapshot = load_snapshot(conn)
    view = build_inventory_view(snapshot)
    assert view["wal_boundary_lsn"] == "0/2000000"
    by_name = {slot["name"]: slot for slot in view["slots"]}

    oldest = by_name["slot_old_metrics"]
    assert oldest["occupancy"]["retention_lsn"] == "0/1000000"
    assert oldest["occupancy"]["retained_bytes"] == 16 * 1024 * 1024
    assert view["summary"]["earliest_retained"]["slot_name"] == "slot_old_metrics"
    assert view["summary"]["total_retained_bytes"] == 16 * 1024 * 1024

    # subscriber lag comes from confirmed_flush_lsn
    assert by_name["slot_audit"]["subscriber_lag"]["bytes"] == 0x2000000 - 0x1900000


def test_eligibility_rules(conn):
    slots = {s["name"]: s for s in build_inventory_view(load_snapshot(conn))["slots"]}
    assert slots["slot_old_metrics"]["eligible"] is True
    assert slots["slot_legacy"]["eligible"] is True

    orders = slots["slot_orders"]
    assert orders["eligible"] is False
    codes = {reason["code"] for reason in orders["blocking_reasons"]}
    assert {"slot_active", "not_disabled", "referenced_by_online_subscription"} <= codes

    standby = slots["slot_standby"]
    assert [r["code"] for r in standby["blocking_reasons"]] == ["not_disabled"]


def test_reclaim_estimate_only_counts_eligible(conn):
    snapshot = load_snapshot(conn)
    plan = build_cleanup_plan(snapshot, ["slot_old_metrics"])
    # next earliest pin is slot_audit at 0/1400000
    assert plan["reclaimable_range"] == {
        "from_lsn": "0/1000000",
        "to_lsn": "0/1400000",
        "bytes": 0x400000,
        "human": "4.0 MiB",
    }

    # cleaning every slot cannot move the floor past the boundary
    all_slots = [s["name"] for s in snapshot["slots"]]
    plan_all = build_cleanup_plan(snapshot, all_slots)
    assert parse_lsn(plan_all["after_cleanup_earliest"]["lsn"]) == snapshot["wal_boundary_lsn"]


def test_estimate_ignores_blocked_slots(conn):
    """Deleting a blocked slot must not be credited with reclaimable WAL."""
    snapshot = load_snapshot(conn)
    plan = build_cleanup_plan(snapshot, ["slot_orders"])
    assert plan["reclaimable_range"]["bytes"] == 0
    assert plan["after_cleanup_earliest"]["lsn"] == "0/1000000"


def test_fingerprint_ignores_cosmetic_fields(conn):
    snapshot = load_snapshot(conn)
    first = fingerprint_snapshot(snapshot)
    with db.transaction(conn):
        conn.execute("UPDATE slots SET note = 'changed note', updated_at = 1 WHERE name = 'slot_audit'")
        conn.execute("UPDATE cluster SET revision = revision + 1 WHERE id = 1")
    assert fingerprint_snapshot(load_snapshot(conn)) == first

    with db.transaction(conn):
        conn.execute("UPDATE slots SET confirmed_flush_lsn = 0 WHERE name = 'slot_audit'")
    assert fingerprint_snapshot(load_snapshot(conn)) != first


def test_subscription_reference_must_exist(conn):
    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction(conn):
            conn.execute(
                "INSERT INTO subscriptions (name, slot_name, status, confirmed_lsn, updated_at) "
                "VALUES ('bad', 'missing_slot', 'online', 1, 1)"
            )
