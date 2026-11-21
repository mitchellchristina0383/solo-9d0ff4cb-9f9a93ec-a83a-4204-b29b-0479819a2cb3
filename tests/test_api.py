from __future__ import annotations

import sqlite3

from tests.conftest import confirm, error_code, make_preview


def slot_names(client):
    return [s["name"] for s in client.get("/api/inventory").json()["slots"]]


# ---------------------------------------------------------------------------
# Inventory entry
# ---------------------------------------------------------------------------


def test_docs_and_demo_endpoints(demo):
    assert demo.get("/docs").status_code == 200
    assert demo.get("/demo").status_code == 200
    followed = demo.get("/", follow_redirects=False)
    assert followed.status_code in (307, 308)
    assert followed.headers["location"] == "/docs"
    assert demo.get("/openapi.json").status_code == 200
    schema = demo.get("/openapi.json").json()
    assert "/api/previews/{preview_id}/confirm" in schema["paths"]


def test_health_reports_recovery(demo):
    body = demo.get("/api/health").json()
    assert body["ok"] is True
    assert body["ttl_seconds"] == 900


def test_cluster_boundary_update_and_validation(demo):
    assert demo.put("/api/cluster", json={"wal_boundary_lsn": "0/2100000"}).status_code == 200
    assert demo.get("/api/cluster").json()["wal_boundary_lsn"] == "0/2100000"
    bad = demo.put("/api/cluster", json={"wal_boundary_lsn": "not-an-lsn"})
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "validation_failed"
    extra = demo.put("/api/cluster", json={"wal_boundary_lsn": "0/1", "oops": 1})
    assert extra.status_code == 422


def test_slot_crud_and_invariants(demo):
    created = demo.post(
        "/api/slots",
        json={
            "name": "slot_new",
            "slot_type": "logical",
            "restart_lsn": "0/1F00000",
            "confirmed_flush_lsn": "0/1F80000",
            "disabled": True,
            "note": "inserted",
        },
    )
    assert created.status_code == 201
    assert created.json()["eligible"] is True

    dup = demo.post(
        "/api/slots",
        json={
            "name": "slot_new",
            "slot_type": "logical",
            "restart_lsn": "0/1",
            "confirmed_flush_lsn": "0/2",
        },
    )
    assert dup.status_code == 409
    assert error_code(dup) == "slot_exists"

    patched = demo.patch("/api/slots/slot_new", json={"active": True, "disabled": False})
    assert patched.status_code == 200
    assert patched.json()["eligible"] is False
    assert {r["code"] for r in patched.json()["blocking_reasons"]} == {"slot_active", "not_disabled"}

    # logical slot without confirmed_flush_lsn is rejected by schema
    bad = demo.post("/api/slots", json={"name": "s", "slot_type": "logical", "restart_lsn": "0/1"})
    assert bad.status_code == 422

    # physical slot with confirmed_flush_lsn is rejected
    bad2 = demo.post(
        "/api/slots",
        json={
            "name": "p",
            "slot_type": "physical",
            "restart_lsn": "0/1",
            "confirmed_flush_lsn": "0/2",
        },
    )
    assert bad2.status_code == 422


def test_direct_delete_honours_safety_gates(demo):
    assert demo.delete("/api/slots/slot_audit").status_code == 409
    assert error_code(demo.delete("/api/slots/slot_audit")) != ""
    assert error_code(demo.delete("/api/slots/slot_standby")) == "not_disabled"
    ok = demo.delete("/api/slots/slot_legacy")
    assert ok.status_code == 200
    assert demo.get("/api/subscriptions/sub_legacy").json()["slot_name"] is None


def test_subscription_validation(demo):
    ok = demo.post(
        "/api/subscriptions",
        json={"name": "sub_x", "slot_name": "slot_audit", "status": "online", "confirmed_lsn": "0/1000000"},
    )
    assert ok.status_code == 201
    bad = demo.post(
        "/api/subscriptions",
        json={"name": "sub_y", "status": "online", "confirmed_lsn": "0/1000000"},
    )
    assert bad.status_code == 422


def test_inventory_exposes_blocking_reasons_and_occupancy(demo):
    inventory = demo.get("/api/inventory").json()
    assert inventory["summary"]["slot_count"] == 5
    assert inventory["summary"]["eligible_count"] == 2
    assert inventory["summary"]["blocked_count"] == 3
    for slot in inventory["slots"]:
        assert slot["occupancy"]["source"] == "restart_lsn"
        if not slot["eligible"]:
            assert slot["blocking_reasons"]
            for reason in slot["blocking_reasons"]:
                assert reason["code"] and reason["message"] and reason["source"]


# ---------------------------------------------------------------------------
# Preview / confirm happy path
# ---------------------------------------------------------------------------


def test_preview_confirm_happy_path(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    plan = preview["plan"]
    assert plan["reclaimable_range"]["human"] == "4.0 MiB"
    assert preview["status"] == "preview"

    response = confirm(demo, preview)
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["preview"]["status"] == "completed"
    assert body["preview"]["result"]["deleted_slots"] == ["slot_old_metrics"]

    assert "slot_old_metrics" not in slot_names(demo)
    # inventory still shows occupancy of remaining slots
    assert demo.get("/api/slots/slot_old_metrics").status_code == 404


def test_confirm_is_idempotent_for_completed_preview(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    first = confirm(demo, preview)
    assert first.status_code == 200
    second = confirm(demo, preview)
    assert second.status_code == 200
    assert second.json()["already_applied"] is True


def test_preview_detaches_only_disabled_subscriptions(demo):
    preview = make_preview(demo, ["slot_legacy"])
    assert preview["plan"]["detach_subscriptions"] == [
        {"name": "sub_legacy", "slot_name": "slot_legacy", "status": "disabled"}
    ]
    body = confirm(demo, preview).json()
    assert body["preview"]["result"]["detached_subscriptions"] == ["sub_legacy"]
    assert demo.get("/api/subscriptions/sub_legacy").json()["slot_name"] is None


def test_confirm_requires_echoed_fingerprint(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    response = confirm(demo, preview, fingerprint="0" * 64)
    assert response.status_code == 409
    assert error_code(response) == "preview_stale"
    assert "slot_old_metrics" in slot_names(demo)


def test_unknown_preview_returns_404_and_unknown_slot_rejected(demo):
    assert demo.post("/api/previews/nope/confirm", json={"fingerprint": "a" * 64}).status_code == 404
    missing = demo.post("/api/previews", json={"slot_names": ["ghost"]})
    assert missing.status_code == 404
    duplicate = demo.post("/api/previews", json={"slot_names": ["slot_legacy", "slot_legacy"]})
    assert duplicate.status_code == 422


# ---------------------------------------------------------------------------
# Rejection: drift, expiry, blocked slots
# ---------------------------------------------------------------------------


def test_boundary_drift_rejects_whole_batch(demo):
    preview = make_preview(demo, ["slot_old_metrics", "slot_legacy"])
    demo.put("/api/cluster", json={"wal_boundary_lsn": "0/2100000"})

    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "preview_stale"
    detail = response.json()["error"]["detail"]
    assert detail["changes"][0]["field"] == "wal_boundary_lsn"

    # nothing deleted, preview recorded as rejected with the diff
    assert {"slot_old_metrics", "slot_legacy"} <= set(slot_names(demo))
    stored = demo.get(f"/api/previews/{preview['id']}").json()
    assert stored["status"] == "rejected"
    assert stored["reject_detail"]["changes"][0]["before"] == "0/2000000"


def test_slot_state_drift_rejects_whole_batch(demo):
    preview = make_preview(demo, ["slot_old_metrics", "slot_legacy"])
    demo.patch("/api/slots/slot_legacy", json={"confirmed_flush_lsn": "0/1F80000"})
    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "preview_stale"
    assert "slot_old_metrics" in slot_names(demo)


def test_subscription_confirmation_drift_rejects(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    demo.patch("/api/subscriptions/sub_orders", json={"confirmed_lsn": "0/1F00000"})
    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "preview_stale"
    assert "slot_old_metrics" in slot_names(demo)


def test_expired_preview_is_rejected(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    demo.clock[0] += 901
    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "preview_expired"
    assert "slot_old_metrics" in slot_names(demo)
    assert demo.get(f"/api/previews/{preview['id']}").json()["status"] == "rejected"


def test_blocked_slot_cannot_be_cleaned(demo):
    preview = make_preview(demo, ["slot_audit"])
    assert preview["plan"]["slot_names"] == ["slot_audit"]
    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "slots_not_eligible"
    blocked = response.json()["error"]["detail"]["blocked"]
    assert blocked[0]["slot"] == "slot_audit"
    assert "slot_audit" in slot_names(demo)


def test_mixed_batch_with_blocked_slot_rejected_entirely(demo):
    preview = make_preview(demo, ["slot_old_metrics", "slot_orders"])
    response = confirm(demo, preview)
    assert response.status_code == 409
    assert error_code(response) == "slots_not_eligible"
    # even the eligible member survives: whole-batch rejection
    assert "slot_old_metrics" in slot_names(demo)


def test_stalled_subscription_blocks_cleanup(demo):
    demo.patch("/api/slots/slot_audit", json={"active": False, "disabled": True})
    inventory = demo.get("/api/inventory").json()
    audit = next(s for s in inventory["slots"] if s["name"] == "slot_audit")
    assert audit["eligible"] is False
    assert {r["code"] for r in audit["blocking_reasons"]} == {"referenced_by_online_subscription"}

    stalled = demo.post("/api/demo/subscriptions/sub_audit/stall", json={"note": "subscriber hung"})
    assert stalled.status_code == 200 and stalled.json()["status"] == "stalled"
    inventory = demo.get("/api/inventory").json()
    audit = next(s for s in inventory["slots"] if s["name"] == "slot_audit")
    assert {r["code"] for r in audit["blocking_reasons"]} == {
        "referenced_by_stalled_subscription"
    }

    preview = make_preview(demo, ["slot_audit"])
    assert confirm(demo, preview).status_code == 409

    # Resuming makes it online again -- still a hard blocker.  Only an
    # explicitly disabled subscription stops blocking.
    demo.post("/api/demo/subscriptions/sub_audit/resume")
    preview2 = make_preview(demo, ["slot_audit"])
    response = confirm(demo, preview2)
    assert response.status_code == 409
    assert error_code(response) == "slots_not_eligible"

    demo.patch("/api/subscriptions/sub_audit", json={"status": "disabled", "slot_name": None})
    preview3 = make_preview(demo, ["slot_audit"])
    assert confirm(demo, preview3).status_code == 200


# ---------------------------------------------------------------------------
# Fault injection and crash safety
# ---------------------------------------------------------------------------


def test_injected_cleanup_failure_rolls_back_and_keeps_state(demo):
    assert demo.post("/api/demo/cleanup-failure?slot_names=slot_legacy").status_code == 200
    preview = make_preview(demo, ["slot_legacy"])

    response = confirm(demo, preview)
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["status"] == "failed"
    assert body["error"]["code"] == "cleanup_failed"

    # slot and subscription binding untouched
    assert "slot_legacy" in slot_names(demo)
    assert demo.get("/api/subscriptions/sub_legacy").json()["slot_name"] == "slot_legacy"
    stored = demo.get(f"/api/previews/{preview['id']}").json()
    assert stored["status"] == "failed"
    assert stored["result"]["failed_slots"] == ["slot_legacy"]

    # inventory still surfaces the pending/failed preview and the slot itself
    inventory = demo.get("/api/inventory").json()
    legacy = next(s for s in inventory["slots"] if s["name"] == "slot_legacy")
    assert legacy["pending_cleanup"]["status"] == "failed"
    assert legacy["pending_cleanup"]["preview_id"] == preview["id"]

    # a failed preview is single-use: regenerate after clearing the injection
    assert confirm(demo, preview).status_code == 409
    demo.delete("/api/demo/cleanup-failure")
    preview2 = make_preview(demo, ["slot_legacy"])
    assert confirm(demo, preview2).status_code == 200
    assert "slot_legacy" not in slot_names(demo)


def test_crash_before_commit_leaves_no_partial_delete(demo):
    preview = make_preview(demo, ["slot_old_metrics", "slot_legacy"])
    demo.post("/api/demo/crash-before-commit")

    response = confirm(demo, preview)
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["status"] == "interrupted"
    assert body["preview"]["status"] == "applying"

    # nothing deleted, subscription still attached
    assert {"slot_old_metrics", "slot_legacy"} <= set(slot_names(demo))
    assert demo.get("/api/subscriptions/sub_legacy").json()["slot_name"] == "slot_legacy"

    # startup-equivalent recovery resolves the claim to failed, still no deletes
    recovered = demo.post("/api/demo/recover").json()
    assert recovered["interrupted"] == 1
    stored = demo.get(f"/api/previews/{preview['id']}").json()
    assert stored["status"] == "failed"
    assert {"slot_old_metrics", "slot_legacy"} <= set(slot_names(demo))

    # preview cannot be retried; new preview works
    assert confirm(demo, preview).status_code == 409
    preview2 = make_preview(demo, ["slot_old_metrics", "slot_legacy"])
    assert confirm(demo, preview2).json()["preview"]["status"] == "completed"


def test_recovery_marks_expired_previews(demo):
    preview = make_preview(demo, ["slot_old_metrics"])
    demo.clock[0] += 1000
    result = demo.post("/api/demo/recover").json()
    assert result["expired"] == 1
    assert demo.get(f"/api/previews/{preview['id']}").json()["status"] == "expired"


def test_restart_recovery_from_applying_row(demo, tmp_path):
    """Simulate a hard kill that left an 'applying' claim: no deletes happened."""
    preview = make_preview(demo, ["slot_legacy"])
    demo.post("/api/demo/crash-before-commit")
    confirm(demo, preview)
    # second request proves the service still has consistent state
    inventory = demo.get("/api/inventory").json()
    legacy = next(s for s in inventory["slots"] if s["name"] == "slot_legacy")
    assert legacy["pending_cleanup"]["status"] == "applying"


def test_demo_disabled_blocks_injection(client):
    import dataclasses

    client.app.state.settings = dataclasses.replace(client.app.state.settings, demo_enabled=False)
    response = client.post("/api/demo/cleanup-failure?slot_names=slot_legacy")
    assert response.status_code == 409
    assert error_code(response) == "demo_disabled"


def test_state_endpoint_exposes_events_and_faults(demo):
    demo.put("/api/cluster", json={"wal_boundary_lsn": "0/2100000"})
    demo.post("/api/demo/cleanup-failure?slot_names=slot_legacy")
    state = demo.get("/api/state").json()
    assert state["cluster"]["wal_boundary_lsn"] == "0/2100000"
    assert state["faults"]["cleanup_failure_slots"] == ["slot_legacy"]
    event_types = [event["event_type"] for event in state["events"]]
    assert "wal_boundary_updated" in event_types
    assert "fault_injected" in event_types
    assert state["inventory"]["summary"]["slot_count"] == 5


# ---------------------------------------------------------------------------
# Transactionality at the SQLite level
# ---------------------------------------------------------------------------


def test_delete_requires_disabled_and_inactive(demo):
    """The DELETE statement itself re-checks the safety predicate."""
    from app import db

    settings = demo.app.state.settings
    conn = db.connect(settings)
    try:
        db.initialize(conn)
        with db.transaction(conn):
            cursor = conn.execute(
                "DELETE FROM slots WHERE name = 'slot_orders' AND active = 0 AND disabled = 1"
            )
        assert cursor.rowcount == 0
    finally:
        conn.close()


def test_foreign_key_blocks_slot_delete_with_bound_subscription(demo):
    from app import db

    conn = db.connect(demo.app.state.settings)
    try:
        db.initialize(conn)
        try:
            with db.transaction(conn):
                conn.execute("DELETE FROM slots WHERE name = 'slot_orders'")
        except sqlite3.IntegrityError:
            pass
        else:  # pragma: no cover
            raise AssertionError("bound slot should not be deletable")
    finally:
        conn.close()
    assert "slot_orders" in slot_names(demo)
