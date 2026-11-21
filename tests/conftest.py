from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import clock
from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Fresh app + SQLite file per test, with a controllable clock."""
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "drill.db"))
    monkeypatch.setenv("PREVIEW_TTL_SECONDS", "900")
    monkeypatch.setenv("SEED_DEMO", "true")
    monkeypatch.setenv("ENABLE_DEMO", "true")
    fake_now = [1_800_000_000]

    def fake_clock() -> int:
        return fake_now[0]

    monkeypatch.setattr(clock, "now", fake_clock)
    with TestClient(app) as test_client:
        test_client.clock = fake_now  # type: ignore[attr-defined]
        yield test_client


@pytest.fixture()
def demo(client):
    """Seed + reset so each scenario starts from documented demo state."""
    assert client.post("/api/demo/reset").status_code == 200
    return client


def make_preview(client, names, note=None):
    response = client.post("/api/previews", json={"slot_names": names, "note": note})
    assert response.status_code == 201, response.text
    return response.json()


def confirm(client, preview, fingerprint=None):
    return client.post(
        f"/api/previews/{preview['id']}/confirm",
        json={"fingerprint": fingerprint or preview["fingerprint"]},
    )


def error_code(response) -> str:
    return response.json()["error"]["code"]
