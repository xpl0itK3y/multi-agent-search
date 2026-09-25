"""SEC2-11: incoming-webhook URLs carry their credential in the path, query or userinfo,
so the webhook log lines name only scheme://host (plus the research id).

Every webhook_* line, from ResearchService and from net_safety's pinned POST, uses the one
helper (net_safety.log_safe_url) and the one field (url=), so a single Loki query finds a
webhook's lines whichever module wrote them."""
import logging
import re

import httpx
import pytest

from src import net_safety
from src.config import settings
from src.net_safety import log_safe_url
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

SECRET_URL = "https://user:pa55@hooks.example.com:8443/services/T000/B000/XXXXSECRET?token=abc123"
LOGGERS = ("src.services.research_service", net_safety.__name__)


def _service():
    return ResearchService(task_store=InMemoryTaskStore())


def _webhook_lines(caplog):
    lines = [
        record.getMessage()
        for record in caplog.records
        if record.name in LOGGERS and record.getMessage().startswith("webhook_")
    ]
    assert lines, [record.getMessage() for record in caplog.records]
    for line in lines:
        for secret in ("XXXXSECRET", "services/T000", "token=abc123", "pa55", "user:"):
            assert secret not in line, line
        assert "target=" not in line, line
        assert re.search(r"\burl=https://hooks\.example\.com(\s|$)", line), line
    return lines


def test_pinned_webhook_success_logs_only_scheme_and_host(monkeypatch, caplog):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", False)
    monkeypatch.setattr(net_safety, "safe_post_json", lambda url, payload, timeout: True)
    caplog.set_level(logging.INFO)

    _service()._fire_webhook(SECRET_URL, "research-1", {"status": "completed"})

    assert _webhook_lines(caplog) == ["webhook_fired url=https://hooks.example.com research_id=research-1"]


@pytest.mark.parametrize(
    ("resolved_ip", "expected"),
    [
        ("10.0.0.5", "webhook_blocked_unsafe_url url=https://hooks.example.com reason="),
        ("93.184.216.34", "webhook_failed url=https://hooks.example.com error=ConnectError"),
    ],
)
def test_pinned_webhook_blocked_or_failed_logs_the_same_field(monkeypatch, caplog, resolved_ip, expected):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", False)
    monkeypatch.setattr(
        net_safety.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", (resolved_ip, 8443))]
    )

    def failing_post(url, **kwargs):
        raise httpx.ConnectError(f"connect failed for {url}")

    monkeypatch.setattr(httpx, "post", failing_post)
    caplog.set_level(logging.INFO)

    _service()._fire_webhook(SECRET_URL, "research-1", {"status": "completed"})

    [line] = _webhook_lines(caplog)
    assert line.startswith(expected), line


@pytest.mark.parametrize("fails", [False, True])
def test_unpinned_webhook_logs_only_scheme_and_host(monkeypatch, caplog, fails):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", True)

    def fake_post(url, **kwargs):
        if fails:
            # httpx puts the request URL in its exception text.
            raise httpx.ConnectError(f"cannot connect to {url}")

    monkeypatch.setattr(httpx, "post", fake_post)
    caplog.set_level(logging.INFO)

    _service()._fire_webhook(SECRET_URL, "research-1", {"status": "completed"})

    [line] = _webhook_lines(caplog)
    if fails:
        assert line == "webhook_failed url=https://hooks.example.com research_id=research-1 error=ConnectError"
    else:
        assert line == "webhook_fired url=https://hooks.example.com research_id=research-1"


@pytest.mark.parametrize(
    "url",
    [
        "https://hooks.slack.com/services/T/B/SECRET",
        "http://[2001:db8::1]:8080/hook?key=1",
        "not a url",
        "https://host.example:99999/x",
    ],
)
def test_the_service_logs_webhooks_through_log_safe_url(monkeypatch, caplog, url):
    monkeypatch.setattr(settings, "webhook_allow_private_targets", True)
    monkeypatch.setattr(httpx, "post", lambda url, **kwargs: None)
    caplog.set_level(logging.INFO, logger="src.services.research_service")

    _service()._fire_webhook(url, "research-1", {"status": "completed"})

    fired = [record.getMessage() for record in caplog.records if record.getMessage().startswith("webhook_fired")]
    assert fired == [f"webhook_fired url={log_safe_url(url)} research_id=research-1"]
    assert not hasattr(ResearchService, "_webhook_log_target")  # one helper, not two
