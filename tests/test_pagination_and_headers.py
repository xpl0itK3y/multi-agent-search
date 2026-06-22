"""AUD-020 (pagination), AUD-023 (cache cleanup), SEC-009 (security headers).

Uses the in-memory task store + a sync TestClient, so no live DB/Redis is touched."""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.config import settings
from src.repositories import InMemoryTaskStore


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)
    monkeypatch.setattr(settings, "allow_memory_task_store", True, raising=False)
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "use_redis_broker", False, raising=False)
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


# ---- SEC-009 ----
def test_security_headers_present(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["referrer-policy"] == "no-referrer"


# ---- AUD-020 (route-level caps) ----
def test_list_research_limit_capped(client):
    assert client.get("/v1/research?limit=99999").status_code == 422
    assert client.get("/v1/research?limit=5").status_code == 200


def test_list_tasks_limit_capped(client):
    assert client.get("/v1/tasks?limit=0").status_code == 422
    assert client.get("/v1/tasks?limit=5").status_code == 200


# ---- AUD-020 (store pagination) ----
def test_get_all_tasks_paginates():
    store = InMemoryTaskStore()
    for i in range(10):
        store.add_task({"id": f"t{i}", "description": "d", "queries": ["q"], "status": "pending"})
    assert [t.id for t in store.get_all_tasks(limit=2, offset=0)] == ["t0", "t1"]
    assert len(store.get_all_tasks(limit=3, offset=9)) == 1
    assert len(store.get_all_tasks(limit=100, offset=0)) == 10


# ---- AUD-023 (cache cleanup) ----
def test_cleanup_search_cache_removes_only_stale():
    store = InMemoryTaskStore()
    store.put_cached_search("stale", [{"a": 1}])
    _, payload = store.search_cache["stale"]
    store.search_cache["stale"] = (datetime.now(timezone.utc) - timedelta(hours=2), payload)
    store.put_cached_search("fresh", [{"b": 2}])

    removed = store.cleanup_search_cache(datetime.now(timezone.utc) - timedelta(hours=1))

    assert removed == 1
    assert store.get_cached_search("stale", 10_000_000) is None
    assert store.get_cached_search("fresh", 10_000_000) is not None
