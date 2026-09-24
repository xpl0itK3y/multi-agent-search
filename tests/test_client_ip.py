"""Recorded client IPs come from the proxy-resolved peer, never from client-sent headers."""
import pytest
from starlette.requests import Request

from src.api.app import extract_client_ip

SPOOFED = {
    "CF-Connecting-IP": "10.6.6.6",
    "X-Forwarded-For": "10.7.7.7, 198.51.100.1",
    "X-Real-IP": "10.8.8.8",
}
PEER = "127.0.0.1"  # httpx.ASGITransport's client address


def _request(headers: dict, client=("203.0.113.5", 4321)) -> Request:
    return Request({
        "type": "http",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": client,
    })


def test_extract_client_ip_ignores_forwarding_headers():
    assert extract_client_ip(_request(SPOOFED)) == "203.0.113.5"


def test_extract_client_ip_without_peer_is_none():
    assert extract_client_ip(_request(SPOOFED, client=None)) is None


@pytest.mark.anyio
async def test_admin_delete_audit_row_records_peer_ip(client):
    store = client._transport.app.state.research_service.task_store
    store.create_user("doomed-user", "doomed@example.com", None)

    response = await client.delete("/v1/admin/users/doomed-user", headers=SPOOFED)

    assert response.status_code == 200
    audit = next(item for item in store.get_admin_audit_logs() if item.action == "delete_user")
    assert audit.ip_address == PEER


@pytest.mark.anyio
async def test_admin_operations_execute_records_peer_ip(client):
    store = client._transport.app.state.research_service.task_store

    response = await client.post(
        "/v1/admin/operations/execute",
        json={"action": "cleanup_old_jobs", "params": {}},
        headers=SPOOFED,
    )

    assert response.status_code == 200
    audit = next(item for item in store.get_admin_audit_logs() if item.action == "cleanup_old_jobs")
    assert audit.ip_address == PEER


@pytest.mark.anyio
async def test_telemetry_and_last_ip_record_peer_ip(client):
    store = client._transport.app.state.research_service.task_store
    registered = await client.post(
        "/v1/auth/register", json={"email": "ip-owner@example.com", "password": "secret123"}
    )
    user_id = registered.json()["user"]["id"]
    headers = {"Authorization": f"Bearer {registered.json()['access_token']}", **SPOOFED}

    await client.get("/v1/auth/me", headers=headers)
    await client.post(
        "/v1/telemetry/event",
        json={"session_id": "ip-sess", "event_name": "page_view", "event_category": "ui"},
        headers=headers,
    )

    assert store.user_telemetry[user_id]["last_ip"] == PEER
    assert store.user_events[-1]["ip_address"] == PEER
