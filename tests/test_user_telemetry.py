import uuid
import pytest
from src.config import settings


async def _register(client, prefix="telem"):
    reg = await client.post(
        "/v1/auth/register",
        json={"email": f"{prefix}_{uuid.uuid4().hex[:8]}@example.com", "password": "Password123!"},
    )
    assert reg.status_code == 200
    return reg.json()


@pytest.mark.anyio
async def test_telemetry_event_ingestion(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    store = client._transport.app.state.research_service.task_store

    # Anonymous ingestion is refused: telemetry is authenticated-only. With no credential
    # at all the CSRF check answers first (403); a bogus bearer reaches auth (401).
    anon_event = {"session_id": "anon-sess-123", "event_name": "session_start", "event_category": "system"}
    no_credentials = await client.post("/v1/telemetry/event", json=anon_event)
    bogus_bearer = await client.post(
        "/v1/telemetry/event", json=anon_event, headers={"Authorization": "Bearer not.a.token"}
    )
    assert no_credentials.status_code == 403
    assert bogus_bearer.status_code == 401
    assert store.user_events == []

    session = await _register(client)
    headers = {"Authorization": f"Bearer {session['access_token']}"}
    auth_resp = await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": "auth-sess-456",
            "event_name": "session_start",
            "event_category": "system",
            "details": {"path": "/"},
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
    assert store.user_events[-1]["user_id"] == session["user"]["id"]

    store.delete_user(session["user"]["id"])


@pytest.mark.anyio
async def test_cookie_authenticated_telemetry_requires_csrf_token(client, monkeypatch):
    """The endpoint is no longer CSRF-exempt: a cross-site form POST riding the session
    cookie is rejected, the SPA's double-submit header is accepted."""
    monkeypatch.setattr(settings, "auth_disabled", False)
    session = await _register(client, "csrf")  # sets the session + csrf cookies
    event = {"session_id": "cookie-sess", "event_name": "tab_focus", "event_category": "ui"}

    forged = await client.post("/v1/telemetry/event", json=event)
    assert forged.status_code == 403

    accepted = await client.post(
        "/v1/telemetry/event",
        json=event,
        headers={"X-CSRF-Token": client.cookies.get("csrf_token") or ""},
    )
    assert accepted.status_code == 200

    client._transport.app.state.research_service.task_store.delete_user(session["user"]["id"])


@pytest.mark.anyio
async def test_telemetry_is_rate_limited_per_user(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "telemetry_rate_limit_per_minute", 2)
    store = client._transport.app.state.research_service.task_store
    first, second = await _register(client, "flood_a"), await _register(client, "flood_b")
    event = {"session_id": "flood-sess", "event_name": "tab_blur", "event_category": "ui"}

    def auth(session):
        return {"Authorization": f"Bearer {session['access_token']}"}

    statuses = [
        (await client.post("/v1/telemetry/event", json=event, headers=auth(first))).status_code
        for _ in range(3)
    ]
    other = await client.post("/v1/telemetry/event", json=event, headers=auth(second))

    assert statuses == [200, 200, 429]
    assert other.status_code == 200  # the budget is per user, not global
    assert len([e for e in store.user_events if e["user_id"] == first["user"]["id"]]) == 2

    store.delete_user(first["user"]["id"])
    store.delete_user(second["user"]["id"])


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
            "event_name": "session_start",
            "event_category": "system",
            "details": {"path": "/"},
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
    assert detail_data["recent_events"][0]["event_name"] == "session_start"

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




@pytest.mark.anyio
async def test_revoked_token_attributes_no_activity_or_telemetry(client):
    """Auth disabled: telemetry is still attributed only to a current token's account;
    a revoked token (or none) records nothing instead of an anonymous row."""
    service = client._transport.app.state.research_service
    store = service.task_store
    reg = await client.post(
        "/v1/auth/register",
        json={"email": f"revoked_{uuid.uuid4().hex[:8]}@example.com", "password": "secret123"},
    )
    user_id = reg.json()["user"]["id"]
    stale = {"Authorization": f"Bearer {reg.json()['access_token']}"}
    # A password change bumps token_version, revoking every token minted before it.
    service.set_user_password(user_id, "rotated-pass1", current_password="secret123")

    await client.get("/v1/auth/config", headers=stale)
    event = await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": "stale-sess",
            "event_name": "session_start",
            "event_category": "system",
            "device_info": {"browser": "Firefox"},
        },
        headers=stale,
    )

    assert event.status_code == 200
    assert event.json()["status"] == "ignored"
    assert user_id not in store.user_telemetry
    assert store.user_events == []
    assert not [s for s in store.user_sessions if s["user_id"] == user_id]


@pytest.mark.anyio
async def test_revoked_token_is_rejected_when_auth_enabled(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    service = client._transport.app.state.research_service
    session = await _register(client, "revoked_on")
    service.set_user_password(session["user"]["id"], "rotated-pass1", current_password="Password123!")

    event = await client.post(
        "/v1/telemetry/event",
        json={"session_id": "stale-sess", "event_name": "tab_focus", "event_category": "ui"},
        headers={"Authorization": f"Bearer {session['access_token']}"},
    )

    assert event.status_code == 401
    assert service.task_store.user_events == []
    service.task_store.delete_user(session["user"]["id"])


@pytest.mark.anyio
async def test_heartbeat_bumps_the_session_instead_of_adding_an_event(client):
    store = client._transport.app.state.research_service.task_store
    session = await _register(client, "heartbeat")
    user_id = session["user"]["id"]
    headers = {
        "Authorization": f"Bearer {session['access_token']}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0) Chrome/130.0 Safari/537.36",
    }
    await client.post(
        "/v1/telemetry/event",
        json={
            "session_id": "hb-sess",
            "event_name": "session_start",
            "event_category": "system",
            "device_info": {"browser": "Chrome", "os": "Windows", "screen_res": "1920x1080"},
        },
        headers=headers,
    )
    started = next(s for s in store.user_sessions if s["user_id"] == user_id)
    started_active_at = started["last_active_at"]
    events_before = len(store.user_events)

    beats = [
        await client.post(
            "/v1/telemetry/event",
            json={"session_id": "hb-sess", "event_name": "heartbeat", "event_category": "system"},
            headers=headers,
        )
        for _ in range(3)
    ]

    assert [b.status_code for b in beats] == [200, 200, 200]
    assert len(store.user_events) == events_before  # no heartbeat rows
    sessions = [s for s in store.user_sessions if s["user_id"] == user_id]
    assert len(sessions) == 1
    assert sessions[0]["last_active_at"] >= started_active_at
    assert sessions[0]["screen_res"] == "1920x1080"  # session_start details kept

    # A heartbeat whose session_start never arrived still records the session.
    await client.post(
        "/v1/telemetry/event",
        json={"session_id": "hb-orphan", "event_name": "heartbeat", "event_category": "system"},
        headers=headers,
    )
    orphan = next(s for s in store.user_sessions if s["session_id"] == "hb-orphan")
    assert (orphan["user_id"], orphan["browser"], orphan["os"]) == (user_id, "Chrome", "Windows")
    assert len(store.user_events) == events_before

    store.delete_user(user_id)
