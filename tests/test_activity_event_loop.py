"""EVENT-LOOP: request activity is written at most once a minute per user, and every
telemetry DB call made from async code runs in the threadpool, not on the event loop."""
import threading
import uuid

import pytest

from src.api.app import reset_activity_touch_gate
from src.domain.errors import NotFoundError


async def _signed_in(client):
    reg = await client.post(
        "/v1/auth/register",
        json={"email": f"loop_{uuid.uuid4().hex[:8]}@example.com", "password": "secret123"},
    )
    return reg.json()["user"]["id"], {"Authorization": f"Bearer {reg.json()['access_token']}"}


def _count_calls(monkeypatch, target, name, calls):
    real = getattr(target, name)

    def counting(*args, **kwargs):
        calls[name] = calls.get(name, 0) + 1
        return real(*args, **kwargs)

    monkeypatch.setattr(target, name, counting)


@pytest.mark.anyio
async def test_activity_touch_hits_the_store_once_per_interval_per_user(client, monkeypatch):
    store = client._transport.app.state.research_service.task_store
    first_id, first = await _signed_in(client)
    _, second = await _signed_in(client)
    reset_activity_touch_gate()  # the second sign-up carried the first user's cookie
    calls: dict[str, int] = {}
    for name in ("touch_user_activity", "get_user_by_id"):
        _count_calls(monkeypatch, store, name, calls)

    # /v1/auth/config does no auth of its own: every store call here is the middleware's.
    for _ in range(5):
        assert (await client.get("/v1/auth/config", headers=first)).status_code == 200
    assert calls == {"touch_user_activity": 1, "get_user_by_id": 1}

    await client.get("/v1/auth/config", headers=second)  # a different user has its own gate
    assert calls == {"touch_user_activity": 2, "get_user_by_id": 2}

    reset_activity_touch_gate()  # the interval has passed
    await client.get("/v1/auth/config", headers=first)
    assert calls == {"touch_user_activity": 3, "get_user_by_id": 3}
    assert store.get_admin_user_detail(first_id).user.last_seen_at is not None


@pytest.mark.anyio
async def test_activity_touch_runs_off_the_event_loop(client, monkeypatch):
    store = client._transport.app.state.research_service.task_store
    _, headers = await _signed_in(client)
    threads: list[int] = []
    monkeypatch.setattr(store, "touch_user_activity", lambda **_kw: threads.append(threading.get_ident()))

    await client.get("/v1/auth/config", headers=headers)

    assert threads and threads[0] != threading.get_ident()


@pytest.mark.anyio
async def test_chat_stream_prompt_event_is_written_off_the_event_loop(client, monkeypatch, mocker):
    from src.auth import llm_rate_limit
    from src.auth.login_rate_limit import SlidingWindowLimiter

    monkeypatch.setattr(llm_rate_limit, "_llm_route_limiter", SlidingWindowLimiter())
    service = client._transport.app.state.research_service
    mocker.patch.object(service, "generate_research_answer", side_effect=NotFoundError("Research not found"))
    threads: list[int] = []
    monkeypatch.setattr(
        service.task_store, "record_user_event", lambda **_kw: threads.append(threading.get_ident())
    )

    response = await client.post("/v1/research/r-loop/messages/stream", json={"question": "still there?"})

    assert response.status_code == 200
    assert threads and threads[0] != threading.get_ident()
