"""INPUT-BOUNDS: client telemetry is allowlisted and size-capped, server-captured values
are clipped to their columns, and a failed prompt-log write is logged, not swallowed."""
import logging
import uuid

import pytest

from src.api.schemas import ResearchRequest, SearchDepth


async def _signed_in(client):
    reg = await client.post(
        "/v1/auth/register",
        json={"email": f"bounds_{uuid.uuid4().hex[:8]}@example.com", "password": "secret123"},
    )
    return reg.json()["user"]["id"], {"Authorization": f"Bearer {reg.json()['access_token']}"}


def _event(**overrides):
    return {"session_id": "bounds-sess", "event_name": "tab_focus", "event_category": "ui", **overrides}


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
    _, headers = await _signed_in(client)

    response = await client.post("/v1/telemetry/event", json=_event(**overrides), headers=headers)

    assert response.status_code == 422
    assert store.user_events == []
    assert store.user_sessions == []


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
    event = store.user_events[-1]
    assert len(event["user_agent"]) == 255
    session = next(s for s in store.user_sessions if s["user_id"] == user_id)
    assert len(session["user_agent"]) == 255
    assert session["country"] is None and session["city"] is None
    assert len(store.user_telemetry[user_id]["last_user_agent"]) == 255


@pytest.mark.anyio
async def test_failed_prompt_event_is_logged_not_swallowed(client, monkeypatch, caplog):
    service = client._transport.app.state.research_service

    def broken_record_user_event(**_kwargs):
        raise RuntimeError("user_events unavailable")

    monkeypatch.setattr(service.task_store, "record_user_event", broken_record_user_event)
    monkeypatch.setattr(service, "decompose_and_enqueue", lambda *_args, **_kwargs: None)

    with caplog.at_level(logging.WARNING, logger="src.api.app"):
        response = await client.post(
            "/v1/research",
            json=ResearchRequest(prompt="telemetry failure topic", depth=SearchDepth.EASY).model_dump(mode="json"),
        )

    assert response.status_code == 200  # the research itself is unaffected
    failures = [r for r in caplog.records if r.getMessage().startswith("prompt_event_record_failed")]
    assert failures and failures[0].exc_info is not None
