"""AUD-004: content fetch is SSRF-guarded, and the fetcher re-validates every redirect hop."""
import pytest

import src.net_safety as net_safety
from src.net_safety import safe_fetch_html
from src.providers.search import ContentExtractor


# --- extract_content rejects internal targets before any fetch (IP literals: no network) ---
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8000/x",
        "http://localhost/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/internal",
    ],
)
def test_extract_content_blocks_ssrf_targets(url):
    assert ContentExtractor.extract_content(url) is None


# --- safe_fetch_html redirect handling, with httpx + the validator mocked (offline) ---
class _Resp:
    _REDIRECTS = {301, 302, 303, 307, 308}

    def __init__(self, status_code=200, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.content = text.encode()

    @property
    def is_redirect(self):
        return self.status_code in self._REDIRECTS and "location" in self.headers


class _FakeClient:
    script: dict = {}
    calls: list = []

    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url):
        _FakeClient.calls.append(url)
        if url not in _FakeClient.script:
            raise AssertionError(f"unexpected (unsafe?) fetch to {url}")
        return _FakeClient.script[url]


def _offline_validator(url):
    bad = ("169.254.", "127.0.0.1", "localhost", "//10.", "internal")
    return (False, "internal") if any(b in url for b in bad) else (True, "ok")


def _patch(monkeypatch, script):
    _FakeClient.script = script
    _FakeClient.calls = []
    monkeypatch.setattr("httpx.Client", _FakeClient)
    monkeypatch.setattr(net_safety, "is_safe_public_url", _offline_validator)


def test_safe_fetch_returns_body_on_200(monkeypatch):
    _patch(monkeypatch, {"https://example.com/": _Resp(200, {}, "<html>hi</html>")})
    assert safe_fetch_html("https://example.com/", max_redirects=1) == "<html>hi</html>"


def test_safe_fetch_follows_safe_redirect(monkeypatch):
    _patch(
        monkeypatch,
        {
            "https://a.example/": _Resp(302, {"location": "https://b.example/page"}),
            "https://b.example/page": _Resp(200, {}, "<html>final</html>"),
        },
    )
    assert safe_fetch_html("https://a.example/", max_redirects=1) == "<html>final</html>"


def test_safe_fetch_blocks_redirect_to_internal(monkeypatch):
    # The internal target is deliberately absent from the script: it must never be fetched.
    _patch(monkeypatch, {"https://a.example/": _Resp(302, {"location": "http://169.254.169.254/meta"})})
    assert safe_fetch_html("https://a.example/", max_redirects=1) is None
    assert "http://169.254.169.254/meta" not in _FakeClient.calls
