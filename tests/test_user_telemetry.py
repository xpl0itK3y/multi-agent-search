import uuid
import pytest
from src.config import settings


@pytest.mark.anyio
async def test_telemetry_event_ingestion(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)

    # 1. Anonymous event
    anon_resp = await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": "anon-sess-123",
            "event_name": "page_view",
            "event_category": "ui",
            "details": {"path": "/"},
        },
    )
    assert anon_resp.status_code == 200
    assert anon_resp.json()["status"] == "ok"
    assert "event_id" in anon_resp.json()

    # 2. Authenticated user with device info
    unique_email = f"telem_{uuid.uuid4().hex[:8]}@example.com"
    reg = await client.post(
        "/v1/auth/register",
        json={"email": unique_email, "password": "Password123!"},
    )
    assert reg.status_code == 200
    token = reg.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    auth_resp = await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": "auth-sess-456",
            "event_name": "research.create",
            "event_category": "research",
            "details": {"prompt": "quantum computing", "depth": "medium"},
            "device_info": {
                "device_type": "desktop",
                "browser": "Chrome",
                "os": "macOS",
                "screen_res": "1920x1080",
                "language": "ru-RU",
                "timezone": "Europe/Moscow",
            },
        },
        headers=headers,
    )
    assert auth_resp.status_code == 200
    assert auth_resp.json()["status"] == "ok"

    client._transport.app.state.research_service.task_store.delete_user(reg.json()["user"]["id"])


@pytest.mark.anyio
async def test_admin_user_telemetry_endpoints(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 10000, raising=False)
    from src.auth.login_rate_limit import reset_auth_rate_limiter
    reset_auth_rate_limiter()
    admin_email = f"admin_{uuid.uuid4().hex[:8]}@example.com"
    monkeypatch.setattr(settings, "admin_emails", [admin_email])

    # Provision the admin out of band (sign-up refuses ADMIN_EMAILS), then log in.
    client._transport.app.state.research_service.provision_admin_account(admin_email, "AdminPassword123!")
    admin_reg = await client.post(
        "/v1/auth/login",
        json={"email": admin_email, "password": "AdminPassword123!"},
    )
    assert admin_reg.status_code == 200
    admin_token = admin_reg.json()["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # Register normal user and post telemetry
    user_email = f"user_{uuid.uuid4().hex[:8]}@example.com"
    user_reg = await client.post(
        "/v1/auth/register",
        json={"email": user_email, "password": "UserPassword123!"},
    )
    user_token = user_reg.json()["access_token"]
    user_id = user_reg.json()["user"]["id"]

    # Post telemetry from user
    await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": f"sess-{user_id}",
            "event_name": "theme_toggle",
            "event_category": "ui",
            "details": {"theme": "midnight"},
            "device_info": {
                "device_type": "desktop",
                "browser": "Safari",
                "os": "macOS",
            },
        },
        headers={"Authorization": f"Bearer {user_token}"},
    )

    # 1. GET /v1/admin/users
    users_resp = await client.get("/v1/admin/users", headers=admin_headers)
    assert users_resp.status_code == 200
    data = users_resp.json()
    assert "users" in data
    assert data["total_users"] >= 2
    found_user = next((u for u in data["users"] if u["id"] == user_id), None)
    assert found_user is not None
    assert found_user["email"] == user_email
    assert found_user["last_browser"] == "Safari"
    assert found_user["last_os"] == "macOS"
    assert found_user["is_online"] is True

    # 2. GET /v1/admin/users/{user_id}
    detail_resp = await client.get(f"/v1/admin/users/{user_id}", headers=admin_headers)
    assert detail_resp.status_code == 200
    detail_data = detail_resp.json()
    assert detail_data["user"]["id"] == user_id
    assert detail_data["user"]["is_online"] is True
    assert len(detail_data["sessions"]) >= 1
    assert len(detail_data["recent_events"]) >= 1
    assert detail_data["recent_events"][0]["event_name"] == "theme_toggle"

    # 3. GET /v1/admin/users/analytics/summary
    summary_resp = await client.get("/v1/admin/users/analytics/summary", headers=admin_headers)
    assert summary_resp.status_code == 200
    summary_data = summary_resp.json()
    assert summary_data["total_users"] >= 2
    assert summary_data["online_now"] >= 1
    assert summary_data["online_users_now"] >= 1
    assert "by_os" in summary_data
    assert "by_browser" in summary_data
    assert "by_device" in summary_data
    assert "os_breakdown" in summary_data
    assert "browser_breakdown" in summary_data
    assert "device_breakdown" in summary_data

    # 4. GET /v1/admin/users/events
    events_resp = await client.get("/v1/admin/users/events", headers=admin_headers)
    assert events_resp.status_code == 200
    events_data = events_resp.json()
    assert "events" in events_data
    assert events_data["total_count"] >= 1

    # 5. GET /v1/admin/users/export
    export_resp = await client.get("/v1/admin/users/export", headers=admin_headers)
    assert export_resp.status_code == 200
    assert "text/csv" in export_resp.headers["content-type"]
    assert "user_id,email,name" in export_resp.text

    # 6. GET /v1/admin/prompts
    prompts_resp = await client.get("/v1/admin/prompts", headers=admin_headers)
    assert prompts_resp.status_code == 200
    prompts_data = prompts_resp.json()
    assert "prompts" in prompts_data
    assert "total_count" in prompts_data

    # 7. GET /v1/admin/prompts/export
    prompts_exp = await client.get("/v1/admin/prompts/export", headers=admin_headers)
    assert prompts_exp.status_code == 200
    assert "text/csv" in prompts_exp.headers["content-type"]
    assert "prompt_type" in prompts_exp.text

    # 8. DELETE /v1/admin/users/{user_id}
    # Self-delete should fail
    admin_id = admin_reg.json()["user"]["id"]
    self_del = await client.delete(f"/v1/admin/users/{admin_id}", headers=admin_headers)
    assert self_del.status_code == 400

    # Delete other user succeeds
    del_resp = await client.delete(f"/v1/admin/users/{user_id}", headers=admin_headers)
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "ok"

    # Deleting again returns 404
    del_again = await client.delete(f"/v1/admin/users/{user_id}", headers=admin_headers)
    assert del_again.status_code == 404

    client._transport.app.state.research_service.task_store.delete_user(admin_id)


