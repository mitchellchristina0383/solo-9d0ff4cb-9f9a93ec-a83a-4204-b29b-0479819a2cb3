"""Write-side services: inventory edits, fault injection, previews, recovery."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from typing import Any

from . import db
from .analysis import (
    build_cleanup_plan,
    diff_snapshots,
    fingerprint_snapshot,
    load_snapshot,
    pending_cleanup_map,
)
from .config import Settings
from .errors import Conflict, DrillError, NotFound
from .lsn import format_lsn, parse_lsn

# Server-side guard: the drill service owns the SQLite file, so every request
# is serialized behind this lock in addition to BEGIN IMMEDIATE.  Tests and
# the crash-recovery path exercise the transactional guarantees directly.
_WRITE_LOCK = threading.RLock()

# Only a fresh preview can be applied; one attempt per preview.
CONFIRMABLE_STATUSES = {"preview"}


def _touch(conn: sqlite3.Connection, now: int, reason: str, event_type: str) -> None:
    """Bump the inventory revision and note what changed.

    Pending previews are deliberately *not* cancelled here.  They stay
    confirmable only if the inventory still matches the preview fingerprint;
    otherwise the confirm returns a 409 with the exact field-level drift.
    That is what makes the "changed after preview" drill realistic.
    """
    db.bump_revision(conn, now)
    db.record_event(conn, event_type, detail={"reason": reason}, at=now)


# ---------------------------------------------------------------------------
# Cluster / inventory edits
# ---------------------------------------------------------------------------


def update_cluster_boundary(conn: sqlite3.Connection, lsn: str, *, actor: str = "operator") -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT wal_boundary_lsn FROM cluster WHERE id = 1").fetchone()
        before = int(row["wal_boundary_lsn"])
        new_value = parse_lsn(lsn)
        conn.execute(
            "UPDATE cluster SET wal_boundary_lsn = ?, updated_at = ? WHERE id = 1",
            (new_value, now),
        )
        _touch(conn, now, "WAL 边界已被更新，旧预览失效", "wal_boundary_updated")
        return {
            "previous": format_lsn(before),
            "wal_boundary_lsn": format_lsn(new_value),
            "revision": conn.execute("SELECT revision FROM cluster WHERE id = 1").fetchone()["revision"],
            "actor": actor,
        }


def _validate_slot_consistency(conn: sqlite3.Connection, slot: dict[str, Any]) -> None:
    if slot["slot_type"] == "logical" and slot["confirmed_flush_lsn"] is None:
        raise Conflict("invalid_slot", "logical 槽必须有 confirmed_flush_lsn")
    if slot["slot_type"] == "physical" and slot["confirmed_flush_lsn"] is not None:
        raise Conflict("invalid_slot", "physical 槽不能有 confirmed_flush_lsn")
    if slot["active"] and slot["disabled"]:
        raise Conflict("invalid_slot", "槽不能同时 active 和 disabled")


def create_slot(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        existing = conn.execute("SELECT name FROM slots WHERE name = ?", (payload["name"],)).fetchone()
        if existing is not None:
            raise Conflict("slot_exists", f"槽 {payload['name']} 已存在")
        _validate_slot_consistency(conn, payload)
        conn.execute(
            """
            INSERT INTO slots (name, slot_type, restart_lsn, confirmed_flush_lsn, active, disabled,
                               wal_status, plugin, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["name"],
                payload["slot_type"],
                parse_lsn(payload["restart_lsn"]),
                parse_lsn(payload["confirmed_flush_lsn"])
                if payload["confirmed_flush_lsn"] is not None
                else None,
                int(payload["active"]),
                int(payload["disabled"]),
                payload["wal_status"],
                payload["plugin"],
                payload["note"],
                now,
            ),
        )
        _touch(conn, now, f"槽 {payload['name']} 已录入，旧预览失效", "slot_created")
        return get_slot(conn, payload["name"])


def update_slot(conn: sqlite3.Connection, name: str, patch: dict[str, Any]) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT * FROM slots WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"槽 {name} 不存在")
        merged = dict(row)
        for key, value in patch.items():
            if key in {"restart_lsn", "confirmed_flush_lsn"} and value is not None:
                value = parse_lsn(value)
            if value is not None or key in {"confirmed_flush_lsn", "plugin", "note"}:
                merged[key] = value
        _validate_slot_consistency(conn, merged)
        conn.execute(
            """
            UPDATE slots SET slot_type = ?, restart_lsn = ?, confirmed_flush_lsn = ?, active = ?,
                disabled = ?, wal_status = ?, plugin = ?, note = ?, updated_at = ?
            WHERE name = ?
            """,
            (
                merged["slot_type"],
                merged["restart_lsn"],
                merged["confirmed_flush_lsn"],
                int(merged["active"]),
                int(merged["disabled"]),
                merged["wal_status"],
                merged["plugin"],
                merged["note"],
                now,
                name,
            ),
        )
        _touch(conn, now, f"槽 {name} 已更新，旧预览失效", "slot_updated")
        return get_slot(conn, name)


def delete_slot_direct(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    """Direct inventory correction, guarded by the same safety rules as previews."""
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT * FROM slots WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"槽 {name} 不存在")
        if row["active"]:
            raise Conflict("slot_active", "槽仍为 active，不能删除")
        if not row["disabled"]:
            raise Conflict("not_disabled", "槽未被明确停用，不能删除")
        subs = conn.execute(
            "SELECT name, status FROM subscriptions WHERE slot_name = ?", (name,)
        ).fetchall()
        blocking = [s["name"] for s in subs if s["status"] != "disabled"]
        if blocking:
            raise Conflict(
                "referenced_by_subscription",
                "槽仍被未停用的订阅引用，不能删除",
                subscriptions=blocking,
            )
        conn.execute("UPDATE subscriptions SET slot_name = NULL WHERE slot_name = ?", (name,))
        conn.execute("DELETE FROM slots WHERE name = ?", (name,))
        _touch(conn, now, f"槽 {name} 已直接删除", "slot_deleted")
        return {"deleted": name, "detached_subscriptions": [s["name"] for s in subs]}


def get_slot(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM slots WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise NotFound(f"槽 {name} 不存在")
    from .analysis import build_inventory_view

    view = next(v for v in build_inventory_view(load_snapshot(conn))["slots"] if v["name"] == name)
    return view


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


def create_subscription(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        existing = conn.execute(
            "SELECT name FROM subscriptions WHERE name = ?", (payload["name"],)
        ).fetchone()
        if existing is not None:
            raise Conflict("subscription_exists", f"订阅 {payload['name']} 已存在")
        if payload["slot_name"] is not None:
            slot = conn.execute(
                "SELECT name FROM slots WHERE name = ?", (payload["slot_name"],)
            ).fetchone()
            if slot is None:
                raise Conflict("slot_missing", f"槽 {payload['slot_name']} 不存在")
        conn.execute(
            """
            INSERT INTO subscriptions (name, slot_name, status, confirmed_lsn, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["name"],
                payload["slot_name"],
                payload["status"],
                parse_lsn(payload["confirmed_lsn"]),
                payload["note"],
                now,
            ),
        )
        _touch(conn, now, f"订阅 {payload['name']} 已录入，旧预览失效", "subscription_created")
        return get_subscription(conn, payload["name"])


def get_subscription(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM subscriptions WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise NotFound(f"订阅 {name} 不存在")
    return subscription_view(conn, row)


def subscription_view(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    boundary = int(conn.execute("SELECT wal_boundary_lsn FROM cluster WHERE id = 1").fetchone()["wal_boundary_lsn"])
    confirmed = int(row["confirmed_lsn"])
    return {
        "name": row["name"],
        "slot_name": row["slot_name"],
        "status": row["status"],
        "confirmed_lsn": format_lsn(confirmed),
        "lag_bytes": max(0, boundary - confirmed),
        "blocks_slot_cleanup": row["status"] != "disabled",
        "note": row["note"],
    }


def update_subscription(conn: sqlite3.Connection, name: str, patch: dict[str, Any]) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT * FROM subscriptions WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"订阅 {name} 不存在")
        merged = dict(row)
        for key in ("slot_name", "status", "confirmed_lsn", "note"):
            if key in patch:
                value = patch[key]
                if key == "confirmed_lsn" and value is not None:
                    value = parse_lsn(value)
                merged[key] = value
        if merged["status"] != "disabled" and merged["slot_name"] is None:
            raise Conflict("invalid_subscription", "非 disabled 订阅必须绑定槽")
        if merged["status"] == "disabled" and merged["slot_name"] is not None:
            raise Conflict("invalid_subscription", "disabled 订阅必须解除槽绑定")
        if merged["slot_name"] is not None:
            slot = conn.execute("SELECT name FROM slots WHERE name = ?", (merged["slot_name"],)).fetchone()
            if slot is None:
                raise Conflict("slot_missing", f"槽 {merged['slot_name']} 不存在")
        conn.execute(
            """
            UPDATE subscriptions SET slot_name = ?, status = ?, confirmed_lsn = ?, note = ?, updated_at = ?
            WHERE name = ?
            """,
            (merged["slot_name"], merged["status"], merged["confirmed_lsn"], merged["note"], now, name),
        )
        _touch(conn, now, f"订阅 {name} 已更新，旧预览失效", "subscription_updated")
        return get_subscription(conn, name)


# ---------------------------------------------------------------------------
# Fault injection
# ---------------------------------------------------------------------------

CLEANUP_FAIL_KEY = "cleanup_fail_slots"
STALL_KEY_PREFIX = "stall_before_commit"


def set_cleanup_failure(conn: sqlite3.Connection, slot_names: list[str]) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        known = {r["name"] for r in conn.execute("SELECT name FROM slots").fetchall()}
        unknown = [n for n in slot_names if n not in known]
        if unknown:
            raise Conflict("slot_missing", "注入目标槽不存在", slots=unknown)
        db.write_demo_state(conn, CLEANUP_FAIL_KEY, json.dumps(sorted(set(slot_names))))
        db.record_event(conn, "fault_injected", detail={"cleanup_failure_slots": slot_names}, at=now)
        return {"cleanup_failure_slots": sorted(set(slot_names))}


def clear_cleanup_failure(conn: sqlite3.Connection) -> None:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        db.write_demo_state(conn, CLEANUP_FAIL_KEY, None)
        db.record_event(conn, "fault_cleared", at=now)


def pause_subscription(conn: sqlite3.Connection, name: str, note: str | None = None) -> dict[str, Any]:
    """Simulate a subscriber stall: marks the subscription stalled, does not
    make the slot deletable.  A stall is the classic cause of WAL retention."""
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT * FROM subscriptions WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"订阅 {name} 不存在")
        if row["status"] == "disabled":
            raise Conflict("subscription_disabled", "已停用的订阅不能注入停顿")
        conn.execute(
            "UPDATE subscriptions SET status = 'stalled', note = COALESCE(?, note), updated_at = ? WHERE name = ?",
            (note, now, name),
        )
        _touch(conn, now, f"订阅 {name} 注入停顿，旧预览失效", "subscription_stalled")
        return get_subscription(conn, name)


def resume_subscription(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = conn.execute("SELECT * FROM subscriptions WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"订阅 {name} 不存在")
        if row["status"] == "disabled":
            raise Conflict("subscription_disabled", "已停用的订阅不能恢复")
        conn.execute(
            "UPDATE subscriptions SET status = 'online', updated_at = ? WHERE name = ?", (now, name)
        )
        _touch(conn, now, f"订阅 {name} 已恢复，旧预览失效", "subscription_resumed")
        return get_subscription(conn, name)


def arm_crash_before_commit(conn: sqlite3.Connection) -> None:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        db.write_demo_state(conn, STALL_KEY_PREFIX, "1")
        db.record_event(conn, "crash_armed", at=now)


def clear_crash_arm(conn: sqlite3.Connection) -> None:
    with _WRITE_LOCK, db.transaction(conn):
        db.write_demo_state(conn, STALL_KEY_PREFIX, None)


def _crash_armed(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM demo_state WHERE key = ?", (STALL_KEY_PREFIX,)).fetchone()
    return row is not None


def _cleanup_failure_slots(conn: sqlite3.Connection) -> set[str]:
    row = conn.execute("SELECT value FROM demo_state WHERE key = ?", (CLEANUP_FAIL_KEY,)).fetchone()
    if row is None:
        return set()
    try:
        return set(json.loads(row["value"]))
    except (TypeError, ValueError):
        return set()


# ---------------------------------------------------------------------------
# Preview lifecycle
# ---------------------------------------------------------------------------


def _preview_view(row: sqlite3.Row, *, include_snapshot: bool = False) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "id": row["id"],
        "status": row["status"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "revision_at_preview": row["revision_at_preview"],
        "fingerprint": row["fingerprint"],
        "plan": json.loads(row["plan_json"]),
        "reject_reason": row["reject_reason"],
        "reject_detail": json.loads(row["reject_detail_json"]) if row["reject_detail_json"] else None,
        "result": json.loads(row["result_json"]) if row["result_json"] else None,
        "note": row["note"],
    }
    if include_snapshot:
        doc["snapshot"] = json.loads(row["snapshot_json"])
    return doc


def _get_preview_row(conn: sqlite3.Connection, preview_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM previews WHERE id = ?", (preview_id,)).fetchone()
    if row is None:
        raise NotFound(f"预览 {preview_id} 不存在")
    return row


def expire_if_needed(conn: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row:
    """Lazily mark a stale pending preview as expired.  No deletion happens."""
    if row["status"] != "preview":
        return row
    now = db.utc_now()
    if now < row["expires_at"]:
        return row
    with _WRITE_LOCK, db.transaction(conn):
        conn.execute(
            """
            UPDATE previews
            SET status = 'expired', finished_at = ?,
                reject_reason = '预览已过期，必须重新生成', reject_detail_json = ?
            WHERE id = ? AND status = 'preview'
            """,
            (now, json.dumps({"expired_at": now}), row["id"]),
        )
        db.record_event(conn, "preview_expired", preview_id=row["id"], at=now)
        return _get_preview_row(conn, row["id"])


def create_preview(
    conn: sqlite3.Connection, settings: Settings, slot_names: list[str], note: str | None
) -> dict[str, Any]:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        snapshot = load_snapshot(conn)
        known = {s["name"] for s in snapshot["slots"]}
        missing = [n for n in slot_names if n not in known]
        if missing:
            raise NotFound("部分槽不存在，无法生成预览", slots=missing)
        plan = build_cleanup_plan(snapshot, slot_names)
        # A preview may *include* blocked slots: the response explains why each
        # one cannot be deleted, which is the point of the drill.
        fingerprint = fingerprint_snapshot(snapshot)
        preview_id = uuid.uuid4().hex
        expires_at = now + settings.preview_ttl_seconds
        conn.execute(
            """
            INSERT INTO previews
                (id, status, created_at, expires_at, revision_at_preview, fingerprint,
                 request_json, snapshot_json, plan_json, note)
            VALUES (?, 'preview', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                preview_id,
                now,
                expires_at,
                snapshot["revision"],
                fingerprint,
                json.dumps({"slot_names": slot_names, "note": note}, ensure_ascii=False),
                json.dumps(snapshot, ensure_ascii=False),
                json.dumps(plan, ensure_ascii=False),
                note,
            ),
        )
        db.record_event(conn, "preview_created", preview_id=preview_id, detail={"slot_names": slot_names}, at=now)
        return _preview_view(_get_preview_row(conn, preview_id), include_snapshot=True)


def list_previews(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM previews ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [_preview_view(row) for row in rows]


def get_preview(conn: sqlite3.Connection, preview_id: str) -> dict[str, Any]:
    """Fetch and lazily expire a preview; used by both API and demo flows."""
    row = expire_if_needed(conn, get_preview_row(conn, preview_id))
    return _preview_view(row, include_snapshot=True)


def get_preview_row(conn: sqlite3.Connection, preview_id: str) -> sqlite3.Row:
    return _get_preview_row(conn, preview_id)


def _mark_rejected(
    conn: sqlite3.Connection,
    preview_id: str,
    *,
    reason: str,
    detail: dict[str, Any],
    now: int,
) -> None:
    conn.execute(
        """
        UPDATE previews SET status = 'rejected', finished_at = ?, reject_reason = ?, reject_detail_json = ?
        WHERE id = ? AND status IN ('preview','applying')
        """,
        (now, reason, json.dumps(detail, ensure_ascii=False), preview_id),
    )
    db.record_event(conn, "preview_rejected", preview_id=preview_id, detail={"reason": reason, **detail}, at=now)


def _eligibility_for(conn: sqlite3.Connection, snapshot: dict[str, Any], slot_names: list[str]) -> list[dict[str, Any]]:
    from .analysis import slot_view, subscriptions_by_slot

    grouped = subscriptions_by_slot(snapshot)
    blocked: list[dict[str, Any]] = []
    for slot in snapshot["slots"]:
        if slot["name"] not in slot_names:
            continue
        view = slot_view(slot, snapshot["wal_boundary_lsn"], grouped.get(slot["name"], []))
        if not view["eligible"]:
            blocked.append({"slot": slot["name"], "blocking_reasons": view["blocking_reasons"]})
    return blocked


def _detach_subscriptions(conn: sqlite3.Connection, snapshot: dict[str, Any], slot_names: list[str], now: int) -> list[str]:
    detached: list[str] = []
    for sub in snapshot["subscriptions"]:
        if sub["slot_name"] in slot_names and sub["status"] == "disabled":
            conn.execute(
                "UPDATE subscriptions SET slot_name = NULL, updated_at = ? WHERE name = ?",
                (now, sub["name"]),
            )
            detached.append(sub["name"])
    return detached


def confirm_preview(
    conn: sqlite3.Connection,
    preview_id: str,
    fingerprint: str,
    note: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Confirm and apply a preview in two write transactions.

    Phase 1 validates (expiry, echoed fingerprint, live fingerprint, slot
    eligibility) and then *claims* the preview by committing
    ``status='applying'``.  Validation failures are recorded as ``rejected``
    in the same transaction and nothing is deleted.

    Phase 2 re-validates the fingerprint, deletes the slots, detaches disabled
    subscriptions, bumps the revision and marks the preview ``completed`` --
    all in one transaction.  Because phase 2 is atomic, a failure or process
    death can never leave a partially deleted inventory: the worst case is an
    ``applying`` row with no deletes, which startup recovery resolves to
    ``failed``.

    A preview can be confirmed once; a failed attempt requires a new preview.
    """
    with _WRITE_LOCK:
        row = _get_preview_row(conn, preview_id)
        if row["status"] == "completed":
            return _preview_view(row), True
        if row["status"] not in CONFIRMABLE_STATUSES:
            raise Conflict("preview_not_confirmable", f"预览状态为 {row['status']}，不能确认")

        rejection: DrillError | None = None
        with db.transaction(conn):
            row = _get_preview_row(conn, preview_id)
            now = db.utc_now()
            if row["status"] == "completed":
                return _preview_view(row), True
            if row["status"] != "preview":
                raise Conflict("preview_not_confirmable", f"预览状态为 {row['status']}，不能确认")
            if now >= row["expires_at"]:
                _mark_rejected(
                    conn,
                    preview_id,
                    reason="预览已过期，必须重新生成",
                    detail={"expired_at": row["expires_at"], "now": now},
                    now=now,
                )
                rejection = Conflict("preview_expired", "预览已过期，整次拒绝，未做任何改动")
            else:
                current = load_snapshot(conn)
                if fingerprint.lower() != row["fingerprint"] or fingerprint_snapshot(current) != row["fingerprint"]:
                    changes = diff_snapshots(json.loads(row["snapshot_json"]), current)
                    _mark_rejected(
                        conn,
                        preview_id,
                        reason="预览后位点/槽状态/WAL 边界发生变化，整次拒绝",
                        detail={"changes": changes, "expected_fingerprint": row["fingerprint"]},
                        now=now,
                    )
                    rejection = Conflict(
                        "preview_stale",
                        "预览后位点、槽状态或 WAL 边界发生变化，整次拒绝，未做任何改动",
                        changes=changes,
                    )
                else:
                    plan = json.loads(row["plan_json"])
                    blocked = _eligibility_for(conn, current, plan["slot_names"])
                    if blocked:
                        _mark_rejected(
                            conn,
                            preview_id,
                            reason="存在不可清理的槽，整次拒绝",
                            detail={"blocked": blocked},
                            now=now,
                        )
                        rejection = Conflict(
                            "slots_not_eligible",
                            "选中的槽中存在不可清理项，整次拒绝，未做任何改动",
                            blocked=blocked,
                        )
            crash = False
            if rejection is None:
                conn.execute(
                    "UPDATE previews SET status = 'applying', started_at = ? WHERE id = ? AND status = 'preview'",
                    (now, preview_id),
                )
                if _crash_armed(conn):
                    # The claim commits, then the "process" dies before phase 2.
                    # No slot is touched; recovery resolves the claim to failed.
                    db.record_event(
                        conn,
                        "crash_simulated",
                        preview_id=preview_id,
                        detail={"stage": "after_claim_before_delete", "transaction": "rolled_back"},
                        at=now,
                    )
                    db.write_demo_state(conn, STALL_KEY_PREFIX, None)
                    crash = True
                else:
                    crash = False

        if rejection is not None:
            raise rejection
        if crash:
            raise _SimulatedCrash("注入的进程崩溃发生在删除事务提交前；未删除任何槽")

        # ---- phase 2: atomically delete + complete ---------------------------
        with db.transaction(conn):
            row = _get_preview_row(conn, preview_id)
            now = db.utc_now()
            if row["status"] != "applying":
                raise Conflict("preview_not_confirmable", f"预览状态为 {row['status']}，不能确认")
            current = load_snapshot(conn)
            if fingerprint_snapshot(current) != row["fingerprint"]:
                changes = diff_snapshots(json.loads(row["snapshot_json"]), current)
                _mark_rejected(
                    conn,
                    preview_id,
                    reason="确认执行前位点/槽状态/WAL 边界发生变化，整次拒绝",
                    detail={"changes": changes},
                    now=now,
                )
                rejection = Conflict(
                    "preview_stale",
                    "确认执行前位点、槽状态或 WAL 边界发生变化，整次拒绝，未做任何改动",
                    changes=changes,
                )
                slot_names = []
            else:
                slot_names = json.loads(row["plan_json"])["slot_names"]
                blocked = _eligibility_for(conn, current, slot_names)
                if blocked:
                    _mark_rejected(
                        conn,
                        preview_id,
                        reason="存在不可清理的槽，整次拒绝",
                        detail={"blocked": blocked},
                        now=now,
                    )
                    rejection = Conflict(
                        "slots_not_eligible",
                        "选中的槽中存在不可清理项，整次拒绝，未做任何改动",
                        blocked=blocked,
                    )
                    slot_names = []

            if rejection is None:
                fail_slots = sorted(_cleanup_failure_slots(conn) & set(slot_names))
                if fail_slots:
                    raise _InjectedFailure(
                        f"注入的清理失败触发于槽 {fail_slots}；删除事务已回滚，未改动任何数据",
                        fail_slots,
                    )
                detached = _detach_subscriptions(conn, current, slot_names, now)
                for name in slot_names:
                    cur = conn.execute(
                        "DELETE FROM slots WHERE name = ? AND active = 0 AND disabled = 1", (name,)
                    )
                    if cur.rowcount != 1:
                        raise _InjectedFailure(
                            f"槽 {name} 删除时前置条件不成立；删除事务已回滚，未改动任何数据", [name]
                        )
                result = {
                    "deleted_slots": sorted(slot_names),
                    "detached_subscriptions": detached,
                    "reclaimed_estimate": json.loads(row["plan_json"])["reclaimable_range"],
                    "completed_at": now,
                    "note": note,
                }
                conn.execute(
                    """
                    UPDATE previews SET status = 'completed', finished_at = ?, result_json = ?,
                        note = COALESCE(?, note)
                    WHERE id = ?
                    """,
                    (now, json.dumps(result, ensure_ascii=False), note, preview_id),
                )
                conn.execute(
                    "UPDATE cluster SET revision = revision + 1, updated_at = ? WHERE id = 1", (now,)
                )
                db.record_event(
                    conn,
                    "cleanup_completed",
                    preview_id=preview_id,
                    detail={"deleted_slots": sorted(slot_names), "detached": detached},
                    at=now,
                )
                completed = _preview_view(_get_preview_row(conn, preview_id), include_snapshot=True)

        if rejection is not None:
            raise rejection
        return completed, False


class _InjectedFailure(Exception):
    def __init__(self, message: str, slots: list[str]) -> None:
        super().__init__(message)
        self.message = message
        self.slots = slots


class _SimulatedCrash(BaseException):
    """BaseException so no generic ``except Exception`` swallows it."""


def _mark_failed(conn: sqlite3.Connection, preview_id: str, message: str, *, slots: list[str] | None = None) -> None:
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        row = _get_preview_row(conn, preview_id)
        if row["status"] != "applying":
            return
        result = {"error": message, "failed_slots": slots or [], "failed_at": now}
        conn.execute(
            "UPDATE previews SET status = 'failed', finished_at = ?, result_json = ? WHERE id = ?",
            (now, json.dumps(result, ensure_ascii=False), preview_id),
        )
        db.record_event(
            conn,
            "cleanup_failed",
            preview_id=preview_id,
            detail={"error": message, "failed_slots": slots or []},
            at=now,
        )


def confirm_with_faults(
    conn: sqlite3.Connection,
    preview_id: str,
    fingerprint: str,
    note: str | None = None,
) -> dict[str, Any]:
    """Turn simulated faults into recorded, atomic outcomes."""
    try:
        doc, already = confirm_preview(conn, preview_id, fingerprint, note)
    except _InjectedFailure as exc:
        _mark_failed(conn, preview_id, exc.message, slots=exc.slots)
        return {
            "ok": False,
            "status": "failed",
            "preview": get_preview(conn, preview_id),
            "error": {"code": "cleanup_failed", "message": exc.message, "failed_slots": exc.slots},
        }
    except _SimulatedCrash as exc:
        return {
            "ok": False,
            "status": "interrupted",
            "preview": get_preview(conn, preview_id),
            "error": {"code": "simulated_crash", "message": str(exc)},
            "recovery": "删除事务未提交，库存未改变；执行 POST /api/demo/recover（或重启服务）后预览标记 failed",
        }
    return {
        "ok": True,
        "status": doc["status"],
        "already_applied": already,
        "preview": doc,
    }


def recover_interrupted(conn: sqlite3.Connection) -> dict[str, Any]:
    """Startup recovery for a process that died mid-confirm.

    The delete itself never needs undoing (single transaction), but an
    ``applying`` row left by a hard kill must be resolved so the UI does not
    show a stuck in-flight state.  Because the delete and the status update
    share one transaction, an ``applying`` row proves the delete did not
    commit; the preview is marked failed and remains re-confirmable.
    """
    now = db.utc_now()
    with _WRITE_LOCK, db.transaction(conn):
        rows = conn.execute("SELECT * FROM previews WHERE status IN ('applying')").fetchall()
        for row in rows:
            result = {
                "error": "进程在清理事务提交前退出；事务已回滚，未删除任何槽",
                "recovered_at": now,
            }
            conn.execute(
                "UPDATE previews SET status = 'failed', finished_at = ?, result_json = ? WHERE id = ?",
                (now, json.dumps(result, ensure_ascii=False), row["id"]),
            )
            db.record_event(
                conn,
                "recovered_interrupted",
                preview_id=row["id"],
                detail={"action": "marked_failed_no_changes"},
                at=now,
            )
        expired = conn.execute(
            "SELECT id FROM previews WHERE status = 'preview' AND expires_at <= ?", (now,)
        ).fetchall()
        for row in expired:
            conn.execute(
                """
                UPDATE previews SET status = 'expired', finished_at = ?,
                    reject_reason = '预览已过期，必须重新生成', reject_detail_json = ?
                WHERE id = ?
                """,
                (now, json.dumps({"expired_at": now}), row["id"]),
            )
            db.record_event(conn, "preview_expired", preview_id=row["id"], at=now)
        # A stale crash arm must not poison the next drill.
        db.write_demo_state(conn, STALL_KEY_PREFIX, None)
        return {"interrupted": len(rows), "expired": len(expired)}


def reseed(conn: sqlite3.Connection) -> dict[str, Any]:
    with _WRITE_LOCK:
        db.seed(conn, reset=True)
        return {"ok": True}


def state_document(conn: sqlite3.Connection) -> dict[str, Any]:
    state = db.read_demo_state(conn)
    cluster = conn.execute("SELECT wal_boundary_lsn, revision FROM cluster WHERE id = 1").fetchone()
    return {
        "cluster": {
            "wal_boundary_lsn": format_lsn(int(cluster["wal_boundary_lsn"])),
            "revision": int(cluster["revision"]),
        },
        "faults": {
            "cleanup_failure_slots": sorted(_cleanup_failure_slots(conn)),
            "crash_before_commit_armed": STALL_KEY_PREFIX in state,
        },
        "pending_cleanup": pending_cleanup_map(conn),
        "events": [
            {
                "at": row["at"],
                "event_type": row["event_type"],
                "preview_id": row["preview_id"],
                "detail": json.loads(row["detail_json"]),
            }
            for row in conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ],
    }
