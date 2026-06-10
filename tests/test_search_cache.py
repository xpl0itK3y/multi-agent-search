"""P2 / 3.2 — shared TTL cache for search results (skip re-hitting the search API)."""

from __future__ import annotations

from src.agents.search import SearchAgent
from src.api.schemas import TaskStatus
from src.config import settings
from src.repositories import InMemoryTaskStore


# ── store-level cache semantics ──────────────────────────────────────────────

def test_in_memory_cache_hit_miss_and_ttl():
    store = InMemoryTaskStore()
    assert store.get_cached_search("missing", 100) is None

    store.put_cached_search("k", [{"url": "https://a.com", "content": "x"}])
    assert store.get_cached_search("k", 100) == [{"url": "https://a.com", "content": "x"}]
    # ttl=0 → the just-written entry is already older than its allowed age
    assert store.get_cached_search("k", 0) is None


def test_in_memory_cache_returns_copies():
    store = InMemoryTaskStore()
    store.put_cached_search("k", [{"url": "u"}])
    got = store.get_cached_search("k", 100)
    got[0]["url"] = "mutated"
    assert store.get_cached_search("k", 100) == [{"url": "u"}]  # cache not mutated


# ── cache key ────────────────────────────────────────────────────────────────

def test_cache_key_normalizes_query_and_varies_by_max_results():
    a8 = SearchAgent(task_store=InMemoryTaskStore(), search_results_per_query=8)
    a16 = SearchAgent(task_store=InMemoryTaskStore(), search_results_per_query=16)
    assert a8._search_cache_key("Solar Power") == a8._search_cache_key("  solar power ")  # strip + lower
    assert a8._search_cache_key("q") != a16._search_cache_key("q")  # result count is part of the key


# ── agent integration ────────────────────────────────────────────────────────

def _add_query_task(store, task_id, query):
    store.add_task({"id": task_id, "description": "t", "queries": [query], "status": "pending"})


def test_search_agent_reuses_cache_across_tasks(mocker):
    store = InMemoryTaskStore()
    _add_query_task(store, "task-1", "solar power")
    _add_query_task(store, "task-2", "solar power")
    mocker.patch.object(settings, "search_cache_enabled", True)
    search = mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[{"url": "https://a.example/x", "title": "A", "snippet": "s", "content": "Body about solar " * 60}],
    )

    agent = SearchAgent(task_store=store, max_sources=1)
    agent.run_task("task-1")
    agent.run_task("task-2")  # identical query → served from cache

    assert search.call_count == 1
    for tid in ("task-1", "task-2"):
        task = store.get_task(tid)
        assert task.status == TaskStatus.COMPLETED
        assert task.result[0]["content"].startswith("Body about solar")


def test_search_agent_skips_cache_when_disabled(mocker):
    store = InMemoryTaskStore()
    _add_query_task(store, "task-1", "solar power")
    _add_query_task(store, "task-2", "solar power")
    mocker.patch.object(settings, "search_cache_enabled", False)
    search = mocker.patch(
        "src.providers.search.SearchProvider.search",
        return_value=[{"url": "https://a.example/x", "title": "A", "snippet": "s", "content": "Body " * 60}],
    )

    agent = SearchAgent(task_store=store, max_sources=1)
    agent.run_task("task-1")
    agent.run_task("task-2")

    assert search.call_count == 2  # no cache → each task searches
