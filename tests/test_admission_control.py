import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.config import settings
from src.domain.errors import ConflictError
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


def test_concurrent_starts_respect_per_user_limit(monkeypatch):
    monkeypatch.setattr(settings, "max_concurrent_researches", 1)
    monkeypatch.setattr(settings, "max_global_active_researches", 0)
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    workers = 10
    barrier = threading.Barrier(workers)

    def start(index):
        barrier.wait()
        try:
            _, research_id = service.start_research(
                ResearchRequest(prompt=f"concurrent topic {index}", depth=SearchDepth.EASY),
                user_id="same-user",
            )
            return research_id
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(start, range(workers)))

    assert len([research_id for research_id in results if research_id]) == 1
    assert len(store.researches) == 1


def test_concurrent_starts_respect_global_limit(monkeypatch):
    monkeypatch.setattr(settings, "max_concurrent_researches", 1)
    monkeypatch.setattr(settings, "max_global_active_researches", 2)
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    workers = 10
    barrier = threading.Barrier(workers)

    def start(index):
        barrier.wait()
        _, research_id = service.start_research(
            ResearchRequest(prompt=f"global topic {index}", depth=SearchDepth.EASY),
            user_id=f"user-{index}",
        )
        return research_id

    with ThreadPoolExecutor(max_workers=workers) as pool:
        research_ids = list(pool.map(start, range(workers)))

    statuses = [store.get_research(research_id).status for research_id in research_ids]
    assert statuses.count(ResearchStatus.PROCESSING) == 2
    assert statuses.count(ResearchStatus.QUEUED) == 8


def test_concurrent_plan_approvals_respect_limits(monkeypatch):
    monkeypatch.setattr(settings, "max_concurrent_researches", 1)
    monkeypatch.setattr(settings, "max_global_active_researches", 0)
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    workers = 10
    barrier = threading.Barrier(workers)
    research_ids = []

    for index in range(workers):
        record = store.add_research(
            ResearchRequest(prompt=f"planned topic {index}", depth=SearchDepth.EASY),
            task_ids=[],
            user_id="same-user",
        )
        store.update_research_graph_state(
            record.id,
            {
                "plan": [
                    {
                        "id": f"planned-task-{index}",
                        "description": "Search",
                        "queries": [f"query-{index}"],
                    }
                ]
            },
        )
        store.update_research_status(record.id, ResearchStatus.PLAN_REVIEW)
        research_ids.append(record.id)

    def approve(research_id):
        barrier.wait()
        try:
            service.approve_research_plan(research_id)
            return True
        except ConflictError:
            return False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        approvals = list(pool.map(approve, research_ids))

    assert approvals.count(True) == 1
    assert approvals.count(False) == workers - 1
    statuses = [store.get_research(research_id).status for research_id in research_ids]
    assert statuses.count(ResearchStatus.PROCESSING) == 1
    assert statuses.count(ResearchStatus.PLAN_REVIEW) == workers - 1
    assert len(store.tasks) == 1


def test_clarification_submission_keeps_state_when_capacity_is_full(monkeypatch):
    monkeypatch.setattr(settings, "max_concurrent_researches", 1)
    monkeypatch.setattr(settings, "max_global_active_researches", 0)
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    _fill_running(store, 1)
    parked = store.add_research(
        ResearchRequest(prompt="clarify this topic", depth=SearchDepth.EASY, plan_first=True),
        task_ids=[],
        user_id="r0",
    )
    initial_state = {
        "clarifications": {"questions": ["Which year?"]},
        "decompose_payload": ResearchRequest(
            prompt="clarify this topic", depth=SearchDepth.EASY, plan_first=True
        ).model_dump(mode="json"),
    }
    store.update_research_graph_state(parked.id, initial_state)
    store.update_research_status(parked.id, ResearchStatus.CLARIFYING)

    with pytest.raises(ConflictError):
        service.submit_clarifications(parked.id, ["2026"])

    unchanged = store.get_research(parked.id)
    assert unchanged.status == ResearchStatus.CLARIFYING
    assert unchanged.graph_state == initial_state


def test_try_claim_queued_is_atomic_once():
    store = InMemoryTaskStore()
    rec = store.add_research(ResearchRequest(prompt="claim topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.QUEUED)
    assert store.try_claim_queued_research(rec.id) is True
    assert store.get_research(rec.id).status == ResearchStatus.PROCESSING
    assert store.try_claim_queued_research(rec.id) is False  # already claimed — no double promote
