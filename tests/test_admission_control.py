from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.config import settings
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


class _StubOrchestrator:
    def run_decompose(self, prompt, depth):
        return []


def _fill_running(store, n):
    for i in range(n):
        rec = store.add_research(
            ResearchRequest(prompt=f"running topic {i} here", depth=SearchDepth.EASY),
            task_ids=[], user_id=f"r{i}",
        )
        store.update_research_status(rec.id, ResearchStatus.PROCESSING)


def test_admits_when_under_global_cap(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 5)
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    _fill_running(store, 2)
    resp, rid = svc.start_research(ResearchRequest(prompt="new topic here", depth=SearchDepth.EASY), user_id="u9")
    assert resp.status == "success"
    assert store.get_research(rid).status != ResearchStatus.QUEUED


def test_queues_when_at_global_cap(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 2)
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    _fill_running(store, 2)
    resp, rid = svc.start_research(ResearchRequest(prompt="third topic here", depth=SearchDepth.EASY), user_id="u9")
    assert resp.status == "queued"
    assert store.get_research(rid).status == ResearchStatus.QUEUED
    assert svc._queue_position(rid) == 1


def test_queue_position_is_fifo(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 1)
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    _fill_running(store, 1)
    _, first = svc.start_research(ResearchRequest(prompt="first queued here", depth=SearchDepth.EASY), user_id="a")
    _, second = svc.start_research(ResearchRequest(prompt="second queued here", depth=SearchDepth.EASY), user_id="b")
    assert svc._queue_position(first) == 1
    assert svc._queue_position(second) == 2


def test_cap_zero_disables_admission(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 0)
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    _fill_running(store, 5)
    resp, rid = svc.start_research(ResearchRequest(prompt="uncapped topic here", depth=SearchDepth.EASY), user_id="u9")
    assert resp.status == "success" and store.get_research(rid).status != ResearchStatus.QUEUED


def test_promote_starts_queued_when_slot_frees(monkeypatch):
    monkeypatch.setattr(settings, "max_global_active_researches", 1)
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store, orchestrator=_StubOrchestrator())
    _fill_running(store, 1)
    _, rid = svc.start_research(ResearchRequest(prompt="queued topic here", depth=SearchDepth.EASY), user_id="a")
    assert store.get_research(rid).status == ResearchStatus.QUEUED
    # free the running slot, then promote
    for item in store.list_researches(limit=10, user_id="r0"):
        store.update_research_status(item.id, ResearchStatus.COMPLETED)
    assert svc.promote_queued_researches() == 1
    assert store.get_research(rid).status != ResearchStatus.QUEUED


def test_try_claim_queued_is_atomic_once():
    store = InMemoryTaskStore()
    rec = store.add_research(ResearchRequest(prompt="claim topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.QUEUED)
    assert store.try_claim_queued_research(rec.id) is True
    assert store.get_research(rec.id).status == ResearchStatus.PROCESSING
    assert store.try_claim_queued_research(rec.id) is False  # already claimed — no double promote
