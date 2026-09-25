"""SEC2-11: the SSRF guard's blocked/failed log lines show a URL as scheme://host only.

Incoming-webhook URLs (Slack, Discord, Teams, CI systems) carry their credential in the
path or query, and the log records are shipped whole to Loki."""
import logging

import httpx
import pytest

from src import net_safety
from src.net_safety import log_safe_url

SECRET = "T0SECRET/B0SECRET/XXSECRETXX"
HOOK = f"https://hooks.example.com/services/{SECRET}?token=tok-SECRET"


def _addrinfo(ip, port=443):
    return [(2, 1, 6, "", (ip, port))]


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (HOOK, "https://hooks.example.com"),
        ("https://user:pw@Example.COM:8443/p?q=1#f", "https://example.com"),
        ("http://[::1]:8080/x?k=v", "http://[::1]"),
        ("HTTP://HOST.example/Path", "http://host.example"),
        ("", "<invalid url>"),
        ("not a url", "<invalid url>"),
        ("mailto:secret@example.com", "<invalid url>"),
        ("http://[::1/x", "<invalid url>"),
        (None, "<invalid url>"),
    ],
)
def test_log_safe_url_keeps_scheme_and_host_only(url, expected):
    assert log_safe_url(url) == expected


def _messages(caplog):
    return [record.getMessage() for record in caplog.records if record.name == net_safety.__name__]


def _assert_redacted(caplog, event):
    lines = [line for line in _messages(caplog) if line.startswith(event)]
    assert lines, _messages(caplog)
    for line in lines:
        assert "SECRET" not in line, line
        assert "/services" not in line and "token=" not in line, line
    return lines


def test_blocked_webhook_logs_host_only(monkeypatch, caplog):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("10.0.0.5"))

    with caplog.at_level(logging.WARNING, logger=net_safety.__name__):
        assert net_safety.safe_post_json(HOOK, {"x": 1}) is False

    [line] = _assert_redacted(caplog, "webhook_blocked_unsafe_url")
    assert "url=https://hooks.example.com " in line


def test_failed_webhook_logs_host_and_error_class_only(monkeypatch, caplog):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("93.184.216.34"))

    def failing_post(url, **kwargs):
        # httpx error text can repeat the request URL, path and query included.
        raise httpx.ConnectError(f"connect failed for {url}")

    monkeypatch.setattr(httpx, "post", failing_post)

    with caplog.at_level(logging.WARNING, logger=net_safety.__name__):
        assert net_safety.safe_post_json(HOOK, {"x": 1}) is False

    [line] = _assert_redacted(caplog, "webhook_failed")
    assert line == "webhook_failed url=https://hooks.example.com error=ConnectError"


class _Resp:
    def __init__(self, status_code=200, headers=None, content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content
        self.is_redirect = False


def _fake_client(get):
    class _Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    _Client.get = lambda self, url: get(url)
    return _Client


def test_blocked_fetch_logs_host_only(monkeypatch, caplog):
    monkeypatch.setattr(net_safety, "is_safe_public_url", lambda url: (False, "internal"))
    monkeypatch.setattr(httpx, "Client", _fake_client(lambda url: pytest.fail("fetched")))

    with caplog.at_level(logging.WARNING, logger=net_safety.__name__):
        assert net_safety.safe_fetch_document(HOOK) is None

    [line] = _assert_redacted(caplog, "safe_fetch_blocked")
    assert "url=https://hooks.example.com " in line


def test_oversized_fetch_logs_host_only(monkeypatch, caplog):
    monkeypatch.setattr(net_safety, "is_safe_public_url", lambda url: (True, "ok"))
    monkeypatch.setattr(httpx, "Client", _fake_client(lambda url: _Resp(content=b"x" * 11)))

    with caplog.at_level(logging.WARNING, logger=net_safety.__name__):
        assert net_safety.safe_fetch_document(HOOK, max_bytes=10) is None

    _assert_redacted(caplog, "safe_fetch_too_large")


def test_failed_fetch_logs_host_and_error_class_only(monkeypatch, caplog):
    monkeypatch.setattr(net_safety, "is_safe_public_url", lambda url: (True, "ok"))

    def failing_get(url):
        raise httpx.ReadTimeout(f"timed out reading {url}")

    monkeypatch.setattr(httpx, "Client", _fake_client(failing_get))

    with caplog.at_level(logging.INFO, logger=net_safety.__name__):
        assert net_safety.safe_fetch_document(HOOK) is None

    [line] = _assert_redacted(caplog, "safe_fetch_failed")
    assert line == "safe_fetch_failed url=https://hooks.example.com error=ReadTimeout"
