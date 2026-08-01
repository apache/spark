import os
from fastapi.testclient import TestClient

from codejt.api import app


def test_create_and_get_source(monkeypatch):
    # Ensure API key used by the app
    monkeypatch.setenv("CODEJT_API_KEY", "test-key")
    client = TestClient(app)

    headers = {"X-API-Key": "test-key", "Content-Type": "application/json"}
    payload = {"title": "unit-smoke", "content": "pytest test"}

    # Create
    resp = client.post("/sources", json=payload, headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == payload["title"]
    assert "id" in data

    src_id = data["id"]

    # Retrieve
    resp2 = client.get(f"/sources/{src_id}", headers={"X-API-Key": "test-key"})
    assert resp2.status_code == 200
    got = resp2.json()
    assert got["id"] == src_id
