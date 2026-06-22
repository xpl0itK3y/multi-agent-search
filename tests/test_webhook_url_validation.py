"""AUD-026: webhook_url is validated at accept-time."""
import pytest
from pydantic import ValidationError

from src.api.schemas import ResearchRequest


def test_webhook_url_accepts_https():
    r = ResearchRequest(prompt="hello world", webhook_url="https://example.com/hook")
    assert r.webhook_url == "https://example.com/hook"


def test_webhook_url_none_is_ok():
    assert ResearchRequest(prompt="hello world").webhook_url is None


@pytest.mark.parametrize(
    "bad",
    ["javascript:alert(1)", "ftp://host/x", "file:///etc/passwd", "not-a-url", "://missing-scheme"],
)
def test_webhook_url_rejects_non_http(bad):
    with pytest.raises(ValidationError):
        ResearchRequest(prompt="hello world", webhook_url=bad)
