"""AUD-004: the content extractor must apply the SSRF guard before fetching."""
import pytest

from src.providers import search as search_mod
from src.providers.search import ContentExtractor


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8000/x",
        "http://localhost/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/internal",
    ],
)
def test_extract_content_blocks_ssrf_targets(url, monkeypatch):
    def _must_not_fetch(*_a, **_k):
        raise AssertionError("trafilatura.fetch_url must not be called for an unsafe URL")

    monkeypatch.setattr(search_mod.trafilatura, "fetch_url", _must_not_fetch)
    assert ContentExtractor.extract_content(url) is None
