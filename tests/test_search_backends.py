"""P2 — pluggable search backends: Tavily primary with DuckDuckGo fallback."""

from __future__ import annotations

import threading
import time

import pytest

from src.agents.search import SearchAgent
from src.api.schemas import TaskStatus
from src.config import settings
from src.providers.search import (
    DuckDuckGoBackend,
    SearchBackend,
    SearchProvider,
    TavilyBackend,
    build_search_backends,
)
from src.repositories import InMemoryTaskStore


class _StubBackend(SearchBackend):
    def __init__(self, name, results=None, raise_exc=None):
        self.name = name
        self._results = results or []
        self._raise = raise_exc
        self.calls = 0

    def search(self, query, max_results):
        self.calls += 1
        if self._raise:
            raise self._raise
        return self._results


def _fake_response(mocker, payload):
    resp = mocker.Mock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


class _FakeDDGS:
    def __init__(self, search):
        self._search = search

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    def text(self, query, max_results, backend):
        return self._search(query, max_results, backend)


# ── DuckDuckGo fallback scheduling ───────────────────────────────────────────


def test_duckduckgo_uses_only_api_backend_when_it_succeeds(mocker):
    called_backends = []

    def search(query, max_results, backend):
        called_backends.append(backend)
        return [{"title": "Result", "href": "https://example.com", "body": "Summary"}]

    mocker.patch("src.providers.search.DDGS", side_effect=lambda **kwargs: _FakeDDGS(search))

    results = DuckDuckGoBackend().search("query", 5)

    assert results[0]["url"] == "https://example.com"
    assert called_backends == ["api"]


def test_duckduckgo_does_not_wait_for_losing_fallback(mocker):
    slow_started = threading.Event()
    release_slow = threading.Event()

    def search(query, max_results, backend):
        if backend == "api":
            return []
        if backend == "lite":
            slow_started.set()
            release_slow.wait(timeout=5)
            return []
        assert slow_started.wait(timeout=1)
        return [{"title": "Result", "href": "https://example.com", "body": "Summary"}]

    mocker.patch("src.providers.search.DDGS", side_effect=lambda **kwargs: _FakeDDGS(search))
    started_at = time.perf_counter()
    try:
        results = DuckDuckGoBackend().search("query", 5)
        elapsed = time.perf_counter() - started_at
    finally:
        release_slow.set()

    assert results[0]["url"] == "https://example.com"
    assert elapsed < 2


# Tavily backend


def test_tavily_maps_fields_and_passes_raw_content(mocker):
    backend = TavilyBackend("key-123", search_depth="advanced", include_raw_content=True)
    post = mocker.patch(
        "httpx.post",
        return_value=_fake_response(
            mocker,
            {
                "results": [
                    {"title": "T1", "url": "https://a.com", "content": "snippet one", "raw_content": "FULL ONE"},
                    {"title": "T2", "url": "https://b.com", "content": "snippet two", "raw_content": None},
                    {"title": "no url", "url": "", "content": "x", "raw_content": "y"},
                ]
            },
        ),
    )

    out = backend.search("solar energy", 5)

    assert len(out) == 2  # the empty-url result is dropped
    assert out[0] == {"title": "T1", "url": "https://a.com", "snippet": "snippet one", "content": "FULL ONE"}
    assert out[1]["content"] is None  # missing raw_content → no prefetched body
    payload = post.call_args.kwargs["json"]
    assert payload["api_key"] == "key-123"
    assert payload["query"] == "solar energy"
    assert payload["max_results"] == 5
    assert payload["search_depth"] == "advanced"
    assert payload["include_raw_content"] is True


def test_tavily_without_raw_content_returns_no_body(mocker):
    backend = TavilyBackend("key", include_raw_content=False)
    mocker.patch(
        "httpx.post",
        return_value=_fake_response(
            mocker, {"results": [{"title": "T", "url": "https://a.com", "content": "snip", "raw_content": "FULL"}]}
        ),
    )
    out = backend.search("q", 3)
    assert out[0]["content"] is None
    assert out[0]["snippet"] == "snip"


# ── backend selection ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "backend_setting,has_key,expected_primary,expected_fallback",
    [
        ("auto", False, "duckduckgo", None),
        ("auto", True, "tavily", "duckduckgo"),
        ("tavily", True, "tavily", None),
        ("tavily", False, "duckduckgo", None),   # forced tavily without a key degrades to DDG
        ("duckduckgo", True, "duckduckgo", None),  # forced DDG even with a key
    ],
)
def test_backend_selection(mocker, backend_setting, has_key, expected_primary, expected_fallback):
    mocker.patch.object(settings, "search_backend", backend_setting)
    mocker.patch.object(settings, "tavily_api_key", "tvly-key" if has_key else None)
    primary, fallback = build_search_backends()
    assert primary.name == expected_primary
    assert (fallback.name if fallback else None) == expected_fallback


# ── SearchProvider primary/fallback orchestration ────────────────────────────

def test_provider_returns_primary_results_without_calling_fallback():
    provider = SearchProvider(max_results=5)
    provider.primary = _StubBackend("tavily", results=[{"url": "https://a.com", "title": "A"}])
    provider.fallback = _StubBackend("duckduckgo", results=[{"url": "https://b.com"}])
    out = provider.search("q")
    assert out[0]["url"] == "https://a.com"
    assert provider.fallback.calls == 0


def test_provider_falls_back_when_primary_empty():
    provider = SearchProvider(max_results=5)
    provider.primary = _StubBackend("tavily", results=[])
    provider.fallback = _StubBackend("duckduckgo", results=[{"url": "https://b.com"}])
    out = provider.search("q")
    assert out[0]["url"] == "https://b.com"
    assert provider.fallback.calls == 1


def test_provider_falls_back_when_primary_raises():
    provider = SearchProvider(max_results=5)
    provider.primary = _StubBackend("tavily", raise_exc=RuntimeError("429 rate limited"))
    provider.fallback = _StubBackend("duckduckgo", results=[{"url": "https://b.com"}])
    assert provider.search("q")[0]["url"] == "https://b.com"


def test_provider_returns_empty_when_primary_empty_and_no_fallback():
    provider = SearchProvider(max_results=5)
    provider.primary = _StubBackend("duckduckgo", results=[])
    provider.fallback = None
    assert provider.search("q") == []


# ── agent uses backend-provided content (skips fetch) ────────────────────────

def test_search_agent_uses_backend_content_without_fetching(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task({"id": "task-1", "description": "test", "queries": ["query"], "status": "pending"})

    body = "Backend provided body about photovoltaic capacity " * 60
    mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[{"url": "https://a.example/article", "title": "Example", "snippet": "snip", "content": body}],
    )
    extract = mocker.patch("src.providers.search.ContentExtractor.extract_content")

    agent = SearchAgent(task_store=task_store, max_sources=1)
    agent.run_task("task-1")

    final = task_store.get_task("task-1")
    assert final.status == TaskStatus.COMPLETED
    assert final.result[0]["content"] == body.strip()
    assert final.result[0]["extraction_status"] == "success"
    extract.assert_not_called()  # content came from the search backend → no fetch
