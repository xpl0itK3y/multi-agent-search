import uuid
import pytest
from src.config import settings


@pytest.mark.anyio
async def test_update_profile_and_token_stats(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)

    unique_email = f"profile_test_{uuid.uuid4().hex[:8]}@example.com"
    # Register test user
    reg_resp = await client.post(
        "/v1/auth/register",
        json={"email": unique_email, "password": "SecurePassword123!"},
    )
    assert reg_resp.status_code == 200
    token = reg_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Verify initial profile
    me_resp = await client.get("/v1/auth/me", headers=headers)
    assert me_resp.status_code == 200
    assert me_resp.json()["name"] is None

    # Update profile (name and avatar)
    patch_resp = await client.patch(
        "/v1/auth/profile",
        json={"name": "Denis Researcher", "avatar_url": "🧠"},
        headers=headers,
    )
    assert patch_resp.status_code == 200
    data = patch_resp.json()
    assert data["name"] == "Denis Researcher"
    assert data["avatar_url"] == "🧠"

    # Get token stats
    stats_resp = await client.get("/v1/auth/token-stats", headers=headers)
    assert stats_resp.status_code == 200
    stats = stats_resp.json()
    assert "total_tokens" in stats
    assert "prompt_tokens" in stats
    assert "completion_tokens" in stats
    assert "estimated_cost_usd" in stats
    assert "by_model" in stats
