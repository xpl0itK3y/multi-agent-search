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


def _finalize_ready_service(mocker):
    store = InMemoryTaskStore()
    rec = store.add_research(ResearchRequest(prompt="cancel during trust suite", depth=SearchDepth.EASY), task_ids=["t1"])
    store.add_task(
        {
            "id": "t1",
            "research_id": rec.id,
            "description": "done",
            "queries": ["q"],
            "status": "completed",
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.llm = None
    analyzer.run_analysis.return_value = "draft report [S1]"
    svc = ResearchService(task_store=store, analyzer=analyzer)
    return store, rec, svc


def test_cancel_during_the_trust_suite_stops_the_remaining_steps(mocker):
    # CANCEL-TRUST-SUITE: the graph already returned; a cancel that lands during the
    # red-team pass must stop every later audit/viewpoint step and the commit.
    store, rec, svc = _finalize_ready_service(mocker)

    def red_team_then_cancel(report, research, tasks):
        store.update_research_status(rec.id, ResearchStatus.CANCELLED, "Cancelled by user.")
        return report

    mocker.patch.object(svc, "_maybe_red_team", side_effect=red_team_then_cancel)
    later_steps = [
        mocker.patch.object(svc, name)
        for name in (
            "_audit_citations",
            "_analyze_source_independence",
            "_assess_source_reputation",
            "_check_numbers",
            "_check_retractions",
            "_maybe_build_comparison",
            "_maybe_assess_stance",
            "_analyze_cross_language",
        )
    ]
    _, job = svc.enqueue_research_finalization(rec.id)

    processed = svc.process_finalize_job(job.id)

    for step in later_steps:
        step.assert_not_called()
    current = store.get_research(rec.id)
    assert current.status == ResearchStatus.CANCELLED
    assert current.final_report == "Cancelled by user."
    assert processed.status.value == "completed"  # the cancelled job is closed, not retried
    assert "viewpoints" not in [entry.get("step") for entry in current.graph_trail]


def test_cancel_between_audit_steps_stops_the_next_one(mocker):
    store, rec, svc = _finalize_ready_service(mocker)

    def independence_then_cancel(*args, **kwargs):
        store.update_research_status(rec.id, ResearchStatus.CANCELLED, "Cancelled by user.")

    mocker.patch.object(svc, "_analyze_source_independence", side_effect=independence_then_cancel)
    reputation = mocker.patch.object(svc, "_assess_source_reputation")
    stance = mocker.patch.object(svc, "_maybe_assess_stance")

    result = svc.finalize_research(rec.id)

    assert result.status == ResearchStatus.CANCELLED
    reputation.assert_not_called()
    stance.assert_not_called()
