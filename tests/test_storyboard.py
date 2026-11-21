"""Demo storyboard tests: the exact flows documented for the operator."""

from __future__ import annotations

from tests.conftest import confirm, error_code, make_preview


def test_story_drift_then_repreview_then_success(demo):
    """Preview -> WAL boundary moves -> confirm rejected -> re-preview -> clean."""
    preview = make_preview(demo, ["slot_old_metrics"])
    demo.put("/api/cluster", json={"wal_boundary_lsn": "0/2200000"})
    rejected = confirm(demo, preview)
    assert rejected.status_code == 409
    assert error_code(rejected) == "preview_stale"

    fresh = make_preview(demo, ["slot_old_metrics"])
    assert fresh["fingerprint"] != preview["fingerprint"]
    assert confirm(demo, fresh).json()["preview"]["status"] == "completed"


def test_story_expiry_then_success(demo):
    preview = make_preview(demo, ["slot_legacy"])
    demo.clock[0] += 1200
    assert error_code(confirm(demo, preview)) == "preview_expired"

    fresh = make_preview(demo, ["slot_legacy"])
    assert confirm(demo, fresh).json()["preview"]["status"] == "completed"
    assert demo.get("/api/subscriptions/sub_legacy").json()["slot_name"] is None


def test_story_cleanup_failure_then_recovery(demo):
    demo.post("/api/demo/cleanup-failure?slot_names=slot_old_metrics")
    preview = make_preview(demo, ["slot_old_metrics"])
    failed = confirm(demo, preview).json()
    assert failed["status"] == "failed"
    inventory = demo.get("/api/inventory").json()
    slot = next(s for s in inventory["slots"] if s["name"] == "slot_old_metrics")
    assert slot["eligible"] is True
    assert slot["pending_cleanup"]["status"] == "failed"

    demo.delete("/api/demo/cleanup-failure")
    fresh = make_preview(demo, ["slot_old_metrics"])
    assert confirm(demo, fresh).json()["ok"] is True
    assert "slot_old_metrics" not in [s["name"] for s in demo.get("/api/inventory").json()["slots"]]


def test_story_subscription_stall_blocks_after_disable(demo):
    demo.patch("/api/slots/slot_audit", json={"active": False, "disabled": True})
    demo.post("/api/demo/subscriptions/sub_audit/stall", json={"note": "network partition"})
    inventory = demo.get("/api/inventory").json()
    audit = next(s for s in inventory["slots"] if s["name"] == "slot_audit")
    assert audit["eligible"] is False
    assert [r["code"] for r in audit["blocking_reasons"]] == [
        "referenced_by_stalled_subscription"
    ]

    preview = make_preview(demo, ["slot_audit"])
    rejected = confirm(demo, preview)
    assert rejected.status_code == 409
    assert error_code(rejected) == "slots_not_eligible"


def test_story_blocked_slot_state_is_kept(demo):
    """A blocked slot keeps showing its source and reason; nothing is deleted."""
    preview = make_preview(demo, ["slot_orders"])
    assert preview["plan"]["reclaimable_range"]["bytes"] == 0
    confirm(demo, preview)
    inventory = demo.get("/api/inventory").json()
    orders = next(s for s in inventory["slots"] if s["name"] == "slot_orders")
    assert orders["occupancy"]["retained_bytes"] > 0
    assert orders["occupancy"]["source"] == "restart_lsn"
    assert len(orders["blocking_reasons"]) >= 2
    stored = demo.get(f"/api/previews/{preview['id']}").json()
    assert stored["status"] == "rejected"
    # the inventory still points at the refused preview so the operator sees why
    assert orders["pending_cleanup"]["status"] == "rejected"
    assert "整次拒绝" in orders["pending_cleanup"]["reason"]
