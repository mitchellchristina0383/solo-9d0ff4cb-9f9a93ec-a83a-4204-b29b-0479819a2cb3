from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Iterable

from .db import utc_now
from .lsn import format_lsn, human_bytes

# Subscription states that count as "still in use".  Only an explicitly
# disabled subscription stops blocking deletion of its slot.
BLOCKING_SUBSCRIPTION_STATES = {"online", "stalled"}

_LSN_FIELDS = {"restart_lsn", "confirmed_flush_lsn", "confirmed_lsn", "wal_boundary_lsn"}


# ---------------------------------------------------------------------------
# Snapshot / fingerprint
# ---------------------------------------------------------------------------


def load_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    cluster = conn.execute(
        "SELECT wal_boundary_lsn, revision, updated_at FROM cluster WHERE id = 1"
    ).fetchone()
    if cluster is None:
        raise RuntimeError("cluster 行缺失，数据库未初始化")
    slots = [
        dict(row)
        for row in conn.execute(
            """
            SELECT name, slot_type, restart_lsn, confirmed_flush_lsn, active, disabled,
                   wal_status, plugin, note, updated_at
            FROM slots ORDER BY name
            """
        )
    ]
    subscriptions = [
        dict(row)
        for row in conn.execute(
            """
            SELECT name, slot_name, status, confirmed_lsn, note, updated_at
            FROM subscriptions ORDER BY name
            """
        )
    ]
    return {
        "wal_boundary_lsn": int(cluster["wal_boundary_lsn"]),
        "revision": int(cluster["revision"]),
        "updated_at": int(cluster["updated_at"]),
        "slots": slots,
        "subscriptions": subscriptions,
    }


def _fingerprint_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Only fields that materially affect cleanup safety.

    Notes, timestamps and the revision counter are intentionally excluded:
    they cannot change what a confirm would delete.
    """
    return {
        "wal_boundary_lsn": snapshot["wal_boundary_lsn"],
        "slots": [
            {
                "name": s["name"],
                "slot_type": s["slot_type"],
                "restart_lsn": s["restart_lsn"],
                "confirmed_flush_lsn": s["confirmed_flush_lsn"],
                "active": int(s["active"]),
                "disabled": int(s["disabled"]),
                "wal_status": s["wal_status"],
            }
            for s in snapshot["slots"]
        ],
        "subscriptions": [
            {
                "name": s["name"],
                "slot_name": s["slot_name"],
                "status": s["status"],
                "confirmed_lsn": s["confirmed_lsn"],
            }
            for s in snapshot["subscriptions"]
        ],
    }


def fingerprint_snapshot(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(
        _fingerprint_payload(snapshot), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _render_value(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field in _LSN_FIELDS and isinstance(value, int):
        return format_lsn(value)
    return value


def diff_snapshots(old: dict[str, Any], new: dict[str, Any]) -> list[dict[str, Any]]:
    """Human-readable field-level drift between two inventory snapshots."""
    changes: list[dict[str, Any]] = []
    if old["wal_boundary_lsn"] != new["wal_boundary_lsn"]:
        changes.append(
            {
                "kind": "cluster",
                "name": "wal_boundary",
                "field": "wal_boundary_lsn",
                "before": format_lsn(old["wal_boundary_lsn"]),
                "after": format_lsn(new["wal_boundary_lsn"]),
            }
        )

    for kind, key, fields in (
        (
            "slot",
            "slots",
            ("slot_type", "restart_lsn", "confirmed_flush_lsn", "active", "disabled", "wal_status"),
        ),
        ("subscription", "subscriptions", ("slot_name", "status", "confirmed_lsn")),
    ):
        old_map = {item["name"]: item for item in old[key]}
        new_map = {item["name"]: item for item in new[key]}
        for name in sorted(set(old_map) | set(new_map)):
            before, after = old_map.get(name), new_map.get(name)
            if before is None:
                changes.append({"kind": kind, "name": name, "field": None, "before": None, "after": "added"})
                continue
            if after is None:
                changes.append({"kind": kind, "name": name, "field": None, "before": "removed", "after": None})
                continue
            for field in fields:
                left, right = before[field], after[field]
                if left != right:
                    changes.append(
                        {
                            "kind": kind,
                            "name": name,
                            "field": field,
                            "before": _render_value(field, left),
                            "after": _render_value(field, right),
                        }
                    )
    return changes


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def subscriptions_by_slot(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for sub in snapshot["subscriptions"]:
        if sub["slot_name"]:
            grouped.setdefault(sub["slot_name"], []).append(sub)
    return grouped


def slot_view(slot: dict[str, Any], boundary: int, subs: list[dict[str, Any]]) -> dict[str, Any]:
    restart = int(slot["restart_lsn"])
    confirmed = slot["confirmed_flush_lsn"]
    confirmed = int(confirmed) if confirmed is not None else None

    invalid = restart > boundary or (confirmed is not None and confirmed > boundary)
    retained_bytes = max(0, boundary - restart)
    subscriber_lag = (
        max(0, boundary - confirmed) if confirmed is not None else None
    )

    blocking: list[dict[str, str]] = []
    if int(slot["active"]) == 1:
        blocking.append(
            {"code": "slot_active", "message": "槽仍为 active，有会话正在使用", "source": "slots.active"}
        )
    if int(slot["disabled"]) != 1:
        blocking.append(
            {
                "code": "not_disabled",
                "message": "槽未被明确标记为停用（disabled=false）",
                "source": "slots.disabled",
            }
        )
    for sub in subs:
        if sub["status"] in BLOCKING_SUBSCRIPTION_STATES:
            if sub["status"] == "online":
                message = f"被在线订阅 {sub['name']} 引用"
                code = "referenced_by_online_subscription"
            else:
                message = f"被停顿但未停用的订阅 {sub['name']} 引用"
                code = "referenced_by_stalled_subscription"
            blocking.append({"code": code, "message": message, "source": f"subscriptions.{sub['name']}"})
    if invalid:
        blocking.append(
            {
                "code": "invalid_position",
                "message": "位点超出当前 WAL 边界，数据不一致，禁止清理",
                "source": "slots.restart_lsn/confirmed_flush_lsn",
            }
        )

    eligible = not blocking
    return {
        "name": slot["name"],
        "slot_type": slot["slot_type"],
        "plugin": slot["plugin"],
        "note": slot["note"],
        "wal_status": slot["wal_status"],
        "active": bool(slot["active"]),
        "disabled": bool(slot["disabled"]),
        "restart_lsn": format_lsn(restart),
        "confirmed_flush_lsn": format_lsn(confirmed) if confirmed is not None else None,
        "occupancy": {
            "source": "restart_lsn",
            "retention_lsn": format_lsn(restart),
            "retained_bytes": retained_bytes,
            "retained_human": human_bytes(retained_bytes),
        },
        "subscriber_lag": (
            {
                "source": "confirmed_flush_lsn",
                "bytes": subscriber_lag,
                "human": human_bytes(subscriber_lag or 0),
                "lagging": bool(subscriber_lag and subscriber_lag > 0),
            }
            if subscriber_lag is not None
            else None
        ),
        "subscriptions": [
            {"name": s["name"], "status": s["status"], "confirmed_lsn": format_lsn(int(s["confirmed_lsn"]))}
            for s in subs
        ],
        "eligible": eligible,
        "blocking_reasons": blocking,
        "invalid_position": invalid,
        "_restart_raw": restart,
        "_confirmed_raw": confirmed,
    }


def reclaim_estimate(
    slot_views: list[dict[str, Any]], boundary: int, selected: Iterable[str] | None
) -> dict[str, Any]:
    selected_set = set(selected or [])
    all_retentions = [(v["_restart_raw"], v["name"]) for v in slot_views]
    if all_retentions:
        floor, floor_slot = min(all_retentions)
    else:
        floor, floor_slot = boundary, None

    if selected is None:
        candidates = {v["name"] for v in slot_views if v["eligible"]}
    else:
        candidates = selected_set
    remaining = [v for v in slot_views if v["name"] not in candidates]
    if remaining:
        new_floor, new_floor_slot = min((v["_restart_raw"], v["name"]) for v in remaining)
    else:
        new_floor, new_floor_slot = boundary, None
    new_floor = max(new_floor, floor)  # deleting slots can never move the floor backwards
    reclaimable = max(0, new_floor - floor)

    return {
        "earliest_retained": {
            "lsn": format_lsn(floor),
            "slot_name": floor_slot,
            "retained_bytes": max(0, boundary - floor),
            "retained_human": human_bytes(max(0, boundary - floor)),
        },
        "after_cleanup_earliest": {
            "lsn": format_lsn(new_floor),
            "slot_name": new_floor_slot,
        },
        "reclaimable_range": {
            "from_lsn": format_lsn(floor),
            "to_lsn": format_lsn(new_floor),
            "bytes": reclaimable,
            "human": human_bytes(reclaimable),
        },
        "total_retained_bytes": max(0, boundary - floor),
        "total_retained_human": human_bytes(max(0, boundary - floor)),
        "note": "按 LSN 区间估算，未计入 WAL 段（通常 16MiB）对齐与实际检查点回收差异",
    }


def build_inventory_view(
    snapshot: dict[str, Any],
    pending_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    boundary = snapshot["wal_boundary_lsn"]
    grouped = subscriptions_by_slot(snapshot)
    pending_map = pending_map or {}
    views = []
    for slot in snapshot["slots"]:
        view = slot_view(slot, boundary, grouped.get(slot["name"], []))
        view["pending_cleanup"] = pending_map.get(slot["name"])
        views.append(view)
    estimate = reclaim_estimate(views, boundary, selected=None)
    for view in views:
        view.pop("_restart_raw", None)
        view.pop("_confirmed_raw", None)
    return {
        "revision": snapshot["revision"],
        "wal_boundary_lsn": format_lsn(boundary),
        "slots": views,
        "summary": {
            "slot_count": len(views),
            "eligible_count": sum(1 for v in views if v["eligible"]),
            "blocked_count": sum(1 for v in views if not v["eligible"]),
            "earliest_retained": estimate["earliest_retained"],
            "total_retained_bytes": estimate["total_retained_bytes"],
            "total_retained_human": estimate["total_retained_human"],
            "reclaimable_if_all_eligible_cleaned": estimate["reclaimable_range"],
            "note": estimate["note"],
        },
        "subscriptions": [
            {
                "name": s["name"],
                "slot_name": s["slot_name"],
                "status": s["status"],
                "confirmed_lsn": format_lsn(int(s["confirmed_lsn"])),
                "note": s["note"],
            }
            for s in snapshot["subscriptions"]
        ],
        "generated_at": utc_now(),
    }


def build_cleanup_plan(
    snapshot: dict[str, Any], selected_names: list[str]
) -> dict[str, Any]:
    boundary = snapshot["wal_boundary_lsn"]
    grouped = subscriptions_by_slot(snapshot)
    by_name = {s["name"]: s for s in snapshot["slots"]}
    views = [
        slot_view(by_name[name], boundary, grouped.get(name, []))
        for name in selected_names
        if name in by_name
    ]
    estimate = reclaim_estimate(
        [slot_view(s, boundary, grouped.get(s["name"], [])) for s in snapshot["slots"]],
        boundary,
        selected=set(selected_names),
    )
    detach = []
    for sub in snapshot["subscriptions"]:
        if sub["slot_name"] in selected_names and sub["status"] == "disabled":
            detach.append({"name": sub["name"], "slot_name": sub["slot_name"], "status": sub["status"]})
    return {
        "slot_names": list(selected_names),
        "slots": [
            {
                "name": v["name"],
                "restart_lsn": v["restart_lsn"],
                "confirmed_flush_lsn": v["confirmed_flush_lsn"],
                "wal_status": v["wal_status"],
                "retained_bytes": v["occupancy"]["retained_bytes"],
                "retained_human": v["occupancy"]["retained_human"],
            }
            for v in views
        ],
        "detach_subscriptions": detach,
        "reclaimable_range": estimate["reclaimable_range"],
        "earliest_retained": estimate["earliest_retained"],
        "after_cleanup_earliest": estimate["after_cleanup_earliest"],
        "note": estimate["note"],
    }


def pending_cleanup_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Latest unfinished or failed preview touching each slot.

    ``rejected`` is the terminal state for a preview that refused to apply
    (drift, expiry, blocked slots), so it belongs here: the operator must see
    which slot's cleanup was refused and why.  Completed previews are not
    pending and are excluded.
    """
    rows = conn.execute(
        """
        SELECT id, status, created_at, expires_at, plan_json, result_json, reject_reason
        FROM previews
        WHERE status IN ('preview', 'applying', 'failed', 'rejected', 'expired')
        ORDER BY created_at DESC, id DESC
        """
    ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        plan = json.loads(row["plan_json"])
        for slot_name in plan.get("slot_names", []):
            if slot_name in result:
                continue
            entry: dict[str, Any] = {
                "preview_id": row["id"],
                "status": row["status"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
            }
            if row["status"] == "failed" and row["result_json"]:
                result_doc = json.loads(row["result_json"])
                entry["error"] = result_doc.get("error")
            if row["status"] in {"rejected", "expired"} and row["reject_reason"]:
                entry["reason"] = row["reject_reason"]
            result[slot_name] = entry
    return result
