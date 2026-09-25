"""SEC2-11: incoming-webhook URLs carry their credential in the path, query or userinfo,
so the webhook log lines name only scheme://host (plus the research id)."""
import logging

import httpx
import pytest

from src import net_safety
from src.config import settings
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

SECRET_URL = "https://user:pa55@hooks.example.com:8443/services/T000/B000/XXXXSECRET?token=abc123"


def _service():
    return ResearchService(task_store=InMemoryTaskStore())


def _assert_redacted(caplog):
    text = "\n".join(record.getMessage() for record in caplog.records)
    for secret in ("XXXXSECRET", "services/T000", "token=abc123", "pa55", "user:"):
        assert secret not in text
    return text


def test_pinned_webhook_success_logs_only_scheme_and_host(monkeypatch, caplog):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", False)
    monkeypatch.setattr(net_safety, "safe_post_json", lambda url, payload, timeout: True)
    caplog.set_level(logging.INFO, logger="src.services.research_service")

    _service()._fire_webhook(SECRET_URL, "research-1", {"status": "completed"})

    text = _assert_redacted(caplog)
    assert "webhook_fired target=https://hooks.example.com:8443 research_id=research-1" in text


@pytest.mark.parametrize("fails", [False, True])
def test_unpinned_webhook_logs_only_scheme_and_host(monkeypatch, caplog, fails):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", True)

    def fake_post(url, **kwargs):
        if fails:
            # httpx puts the request URL in its exception text.
            raise httpx.ConnectError(f"cannot connect to {url}")

    monkeypatch.setattr(httpx, "post", fake_post)
    caplog.set_level(logging.INFO, logger="src.services.research_service")

    _service()._fire_webhook(SECRET_URL, "research-1", {"status": "completed"})

    text = _assert_redacted(caplog)
    if fails:
        assert "webhook_failed target=https://hooks.example.com:8443 research_id=research-1 error=ConnectError" in text
    else:
        assert "webhook_fired target=https://hooks.example.com:8443 research_id=research-1" in text


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://hooks.slack.com/services/T/B/SECRET", "https://hooks.slack.com"),
        ("http://[2001:db8::1]:8080/hook?key=1", "http://[2001:db8::1]:8080"),
        ("not a url", "invalid-url"),
        ("https://host.example:99999/x", "invalid-url"),
    ],
)
def test_webhook_log_target(url, expected):
    assert ResearchService._webhook_log_target(url) == expected
