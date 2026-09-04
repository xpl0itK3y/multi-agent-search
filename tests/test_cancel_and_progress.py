import pytest

from src.agents.search import SearchAgent
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.graph import FinalizeCancelled
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


def test_cancel_sets_cancelled_then_is_noop_on_terminal():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="some running research", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.PROCESSING)

    assert svc.cancel_research(rec.id).status == ResearchStatus.CANCELLED

    # already-terminal research is left as-is (cancel can't resurrect/override a finished run)
    store.update_research_status(rec.id, ResearchStatus.COMPLETED, "done")
    assert svc.cancel_research(rec.id).status == ResearchStatus.COMPLETED


def test_finalize_skips_a_cancelled_research():
    store = InMemoryTaskStore()
    called = {"analyze": False}

    class _Analyzer:
        def run_analysis(self, *a, **k):
            called["analyze"] = True
            return "report"

    svc = ResearchService(task_store=store, analyzer=_Analyzer())
    rec = store.add_research(ResearchRequest(prompt="cancelled mid-run", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.CANCELLED, "Cancelled by user.")

    svc.complete_research_finalization(rec.id)  # heavy finalize entry
    assert called["analyze"] is False  # never spent the analysis call
    assert store.get_research(rec.id).status == ResearchStatus.CANCELLED


def test_finalize_graph_does_not_start_the_next_step_after_cancellation():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="cancel between steps", depth=SearchDepth.HARD), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.CANCELLED, "Cancelled by user.")
    called = False

    def expensive_action():
        nonlocal called
        called = True

    with pytest.raises(FinalizeCancelled):
        svc.finalize_graph_runner._run_timed_step("analyze", expensive_action, rec.id)

    assert called is False


def test_finalize_service_returns_cancelled_record_when_graph_stops(mocker):
    store = InMemoryTaskStore()

    class _Analyzer:
        def run_analysis(self, *args, **kwargs):
            return "report"

    svc = ResearchService(task_store=store, analyzer=_Analyzer())
    rec = store.add_research(ResearchRequest(prompt="cancel during graph", depth=SearchDepth.HARD), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.ANALYZING)

    def cancel_during_graph(*args, **kwargs):
        store.update_research_status(rec.id, ResearchStatus.CANCELLED, "Cancelled by user.")
        raise FinalizeCancelled(rec.id)

    mocker.patch.object(svc.finalize_graph_runner, "run", side_effect=cancel_during_graph)

    result = svc.complete_research_finalization(rec.id)

    assert result.status == ResearchStatus.CANCELLED


def test_result_previews_dedupe_domains_and_cap():
    out = SearchAgent._result_previews([
        {"url": "https://www.example.com/a", "title": "A"},
        {"url": "https://example.com/b", "title": "B"},   # same registered domain -> deduped
        {"url": "https://other.org/c", "title": "C"},
        {"url": "", "title": "no url"},
    ])
    assert [p["domain"] for p in out] == ["example.com", "other.org"]
    assert out[0]["title"] == "A"
