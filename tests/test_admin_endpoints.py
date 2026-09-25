import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.auth.security import create_token
from src.config import settings


@pytest.fixture
def admin_client(monkeypatch):
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)
    monkeypatch.setattr(settings, "allow_memory_task_store", True, raising=False)
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "", raising=False)
    monkeypatch.setattr(settings, "use_redis_broker", False, raising=False)
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def test_admin_overview_endpoint(admin_client):
    res = admin_client.get("/v1/admin/overview")
    assert res.status_code == 200
    data = res.json()
    assert "system_health" in data
    assert "workers" in data
    assert "active_researches_count" in data


def test_admin_tokens_endpoint(admin_client):
    res = admin_client.get("/v1/admin/tokens?page=1&page_size=10")
    assert res.status_code == 200
    data = res.json()
    assert "total_prompt_tokens" in data
    assert "total_tokens" in data
    assert "by_model" in data
    assert "researches" in data


def test_admin_tokens_export_endpoint(admin_client):
    res = admin_client.get("/v1/admin/tokens/export")
    assert res.status_code == 200
    assert "text/csv" in res.headers["content-type"]
    assert "research_id,prompt,depth,status" in res.text


def test_admin_agents_catalog_endpoint(admin_client):
    res = admin_client.get("/v1/admin/agents")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 20
    agent_names = {a["name"] for a in data}
    assert "OrchestratorAgent" in agent_names
    assert "AnalyzerAgent" in agent_names
    assert "SearchAgent" in agent_names


def test_admin_audit_endpoint(admin_client):
    res = admin_client.get("/v1/admin/audit")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_admin_operations_preview_and_execute(admin_client):
    # Preview (dry run)
    prev = admin_client.post(
        "/v1/admin/operations/preview",
        json={"action": "cleanup_old_jobs", "params": {"days": 7}},
    )
    assert prev.status_code == 200
    p_data = prev.json()
    assert p_data["dry_run"] is True
    assert p_data["action"] == "cleanup_old_jobs"

    # Execute
    exec_res = admin_client.post(
        "/v1/admin/operations/execute",
        json={"action": "cleanup_old_jobs", "params": {"days": 7}},
    )
    assert exec_res.status_code == 200
    e_data = exec_res.json()
    assert e_data["dry_run"] is False

    # Check audit log recorded this execution
    audit = admin_client.get("/v1/admin/audit")
    assert audit.status_code == 200
    logs = audit.json()
    assert len(logs) >= 1
    assert logs[0]["action"] == "cleanup_old_jobs"


def test_admin_endpoints_require_admin_when_auth_enabled(monkeypatch):
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)
    monkeypatch.setattr(settings, "allow_memory_task_store", True, raising=False)
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "s" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@super.com", raising=False)
    monkeypatch.setattr(settings, "use_redis_broker", False, raising=False)

    app = create_app()
    with TestClient(app) as test_client:
        # Anonymous is rejected with 401
        res = test_client.get("/v1/admin/overview")
        assert res.status_code == 401

        # Register users in task_store
        app.state.research_service.task_store.create_user("u1", "regular@user.com", "hash")
        app.state.research_service.task_store.create_user("a1", "admin@super.com", "hash", admin_provisioned=True)

        # Non-admin user is rejected with 403
        user_token = create_token("u1", email="regular@user.com")
        res2 = test_client.get(
            "/v1/admin/overview",
            headers={"Authorization": f"Bearer {user_token}"},
        )
        assert res2.status_code == 403

        # Admin user is accepted with 200
        admin_token = create_token("a1", email="admin@super.com")
        res3 = test_client.get(
            "/v1/admin/overview",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert res3.status_code == 200
