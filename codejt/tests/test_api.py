from pathlib import Path

from fastapi.testclient import TestClient

import db
from api import app


def test_create_and_get_source(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEJT_API_KEY", "test-key")
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "codejt_test.db")
    with TestClient(app) as client:
        headers = {"X-API-Key": "test-key", "Content-Type": "application/json"}
        payload = {"title": "unit-smoke", "content": "pytest test"}

        resp = client.post("/sources", json=payload, headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == payload["title"]
        assert "id" in data

        src_id = data["id"]
        resp2 = client.get(f"/sources/{src_id}", headers={"X-API-Key": "test-key"})
        assert resp2.status_code == 200
        got = resp2.json()
        assert got["id"] == src_id


def test_root_health_reports_api_key_configured(monkeypatch):
    monkeypatch.delenv("CODEJT_API_KEY", raising=False)
    client = TestClient(app)

    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["api_key_configured"] is False


def test_missing_api_key_config_returns_500(monkeypatch, tmp_path):
    monkeypatch.delenv("CODEJT_API_KEY", raising=False)
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "codejt_test.db")
    with TestClient(app) as client:
        resp = client.post("/sources", json={"title": "x", "content": "y"})
        assert resp.status_code == 500
        assert resp.json()["detail"] == "CODEJT_API_KEY is not configured"


def test_invalid_api_key_returns_401(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEJT_API_KEY", "test-key")
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "codejt_test.db")
    with TestClient(app) as client:
        resp = client.post("/sources", json={"title": "x", "content": "y"}, headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Unauthorized"
