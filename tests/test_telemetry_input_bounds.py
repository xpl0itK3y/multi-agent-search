"""INPUT-BOUNDS: client telemetry is allowlisted and size-capped, server-captured values
are clipped to their columns, and a failed prompt-log write is logged, not swallowed.

Store reads go through TaskStore methods (or the backend's own rows), so these also run
in postgres-smoke, where the app's store is SQLAlchemyTaskStore on a shared database."""
import logging
import uuid

import pytest
from sqlalchemy import select

from src.api.schemas import ResearchRequest, SearchDepth
from src.config import settings
from src.db.models import UserORM, UserSessionORM
from src.repositories.in_memory_task_store import InMemoryTaskStore


async def _signed_in(client):
    reg = await client.post(
        "/v1/auth/register",
        json={"email": f"bounds_{uuid.uuid4().hex[:8]}@example.com", "password": "secret123"},
    )
    return reg.json()["user"]["id"], {"Authorization": f"Bearer {reg.json()['access_token']}"}


def _event(**overrides):
    return {"session_id": "bounds-sess", "event_name": "tab_focus", "event_category": "ui", **overrides}


def _stored_user_agents(store, user_id) -> tuple[list[str], str | None]:
    """(user_sessions.user_agent of the user's sessions, users.last_user_agent): neither
    is exposed by a TaskStore read, so this looks at the backend's own rows."""
    if isinstance(store, InMemoryTaskStore):
        sessions = [s["user_agent"] for s in store.user_sessions if s["user_id"] == user_id]
        return sessions, store.user_telemetry.get(user_id, {}).get("last_user_agent")
    with store.session_scope() as session:
        sessions = session.execute(
            select(UserSessionORM.user_agent).where(UserSessionORM.user_id == user_id)
        ).scalars().all()
        last = session.execute(select(UserORM.last_user_agent).where(UserORM.id == user_id)).scalar_one()
        return list(sessions), last


@pytest.mark.anyio
@pytest.mark.parametrize(
    "overrides",
    [
        # server-written prompt copies the admin Prompt log trusts
        {"event_name": "research_prompt", "details": {"prompt": "forged", "research_id": "r1"}},
        {"event_name": "chat_prompt", "details": {"prompt": "forged", "research_id": "r1"}},
        {"event_name": "page_view"},  # not something the SPA sends
        {"event_name": "x" * 65},
        {"event_category": "prompt"},
        {"session_id": "s" * 65},
        {"details": {f"k{i}": i for i in range(21)}},
        {"details": {"blob": "x" * 2048}},
        {"device_info": {"browser": "b" * 65}},
        {"device_info": {"language": "l" * 17}},
        {"device_info": {"unexpected": "field"}},
    ],
    ids=[
        "reserved-research_prompt",
        "reserved-chat_prompt",
        "unknown-name",
        "long-name",
        "unknown-category",
        "long-session-id",
        "too-many-detail-keys",
        "oversize-details",
        "long-device-field",
        "long-language",
        "unknown-device-key",
    ],
)
async def test_out_of_bounds_telemetry_is_rejected(client, overrides):
    store = client._transport.app.state.research_service.task_store
    user_id, headers = await _signed_in(client)
    events_before = store.get_admin_event_logs(limit=1).total_count

    response = await client.post("/v1/telemetry/event", json=_event(**overrides), headers=headers)

    assert response.status_code == 422
    assert store.get_admin_event_logs(limit=1).total_count == events_before
    assert store.get_admin_user_detail(user_id).sessions == []


@pytest.mark.anyio
async def test_every_client_event_name_is_accepted(client):
    """The allowlist is what web/src/lib/telemetry.ts sends; each one must still pass."""
    _, headers = await _signed_in(client)
    for name, category in [
        ("session_start", "system"),
        ("session_end", "system"),
        ("heartbeat", "system"),
        ("tab_focus", "ui"),
        ("tab_blur", "ui"),
    ]:
        response = await client.post(
            "/v1/telemetry/event",
            json=_event(event_name=name, event_category=category, details={"path": "/"}),
            headers=headers,
        )
        assert response.status_code == 200, name


@pytest.mark.anyio
async def test_long_user_agent_is_clipped_and_cf_geo_headers_ignored(client):
    store = client._transport.app.state.research_service.task_store
    user_id, headers = await _signed_in(client)
    headers = {
        **headers,
        "User-Agent": "Mozilla/5.0 " + "x" * 400,
        "CF-IPCountry": "ZZ",
        "CF-IPCity": "Forged City",
    }

    response = await client.post(
        "/v1/telemetry/event",
        json=_event(event_name="session_start", event_category="system", device_info={"browser": "Firefox"}),
        headers=headers,
    )

    assert response.status_code == 200
    [event] = store.get_admin_event_logs(user_id=user_id).events
    assert len(event.user_agent) == 255
    [session] = store.get_admin_user_detail(user_id).sessions
    assert session["country"] is None and session["city"] is None
    session_agents, last_user_agent = _stored_user_agents(store, user_id)
    assert [len(agent) for agent in session_agents] == [255]
    assert len(last_user_agent) == 255


@pytest.mark.anyio
async def test_failed_prompt_event_is_logged_not_swallowed(client, monkeypatch, caplog):
    service = client._transport.app.state.research_service

    def broken_record_user_event(**_kwargs):
        raise RuntimeError("user_events unavailable")

    monkeypatch.setattr(service.task_store, "record_user_event", broken_record_user_event)
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda *_args, **_kwargs: None)
    # Capacity is not under test: on a shared database earlier tests' researches count.
    monkeypatch.setattr(settings, "max_concurrent_researches", 0)
    monkeypatch.setattr(settings, "max_global_active_researches", 0)

    with caplog.at_level(logging.WARNING, logger="src.api.app"):
        response = await client.post(
            "/v1/research",
            json=ResearchRequest(prompt="telemetry failure topic", depth=SearchDepth.EASY).model_dump(mode="json"),
        )

    assert response.status_code == 200  # the research itself is unaffected
    failures = [r for r in caplog.records if r.getMessage().startswith("prompt_event_record_failed")]
    assert failures and failures[0].exc_info is not None
