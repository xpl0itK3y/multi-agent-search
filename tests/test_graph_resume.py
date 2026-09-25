"""RESUME-END: resuming the finalize graph from a checkpoint.

A verify checkpoint that settled on its report must resume straight to END (it used
to raise KeyError('__end__') until dead-letter), and a resumed run must keep the
no-progress flag and the wall-clock budget of the run it continues.
"""
import time
from unittest import mock

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    TaskStatus,
)
from src.config import settings
from src.graph import FinalizeGraphRunner
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

SAVED_REPORT = (
    "## Introduction\nVerified finding [S1].\n\n## Conclusion\nDone [S1].\n\n"
    "## Sources\n### Used Sources\n- [S1] https://example.com"
)


def _service(depth=SearchDepth.MEDIUM, analyzer=None):
    store = InMemoryTaskStore()
    research = store.add_research(ResearchRequest(prompt="topic", depth=depth), task_ids=["task-1"])
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body " * 80}],
        }
    )
    if analyzer is None:
        analyzer = mock.Mock()
        analyzer.llm = None
        analyzer.enable_graph_branching = True
        analyzer.run_analysis.return_value = "a fresh analysis that must not run"
    service = ResearchService(task_store=store, analyzer=analyzer, replan_agent=mock.Mock())
    return store, research, service


def _checkpoint(service, research_id, step, **overrides):
    state = {
        "step": step,
        "effective_prompt": "topic",
        "analyze_attempts": 1,
        "replan_attempts": 0,
        "tie_break_attempts": 0,
        "should_replan": False,
        "should_tie_break": False,
        "should_retry_analysis": False,
        "report": SAVED_REPORT,
        "detected_conflicts": [],
        "replan_recommendations": [],
        "tie_break_recommendations": [],
    }
    state.update(overrides)
    service.checkpoint_graph_state(research_id, state, {"step": step, "detail": "checkpoint"})


def test_resume_from_settled_verify_checkpoint_completes_with_the_saved_report():
    store, research, service = _service()
    _checkpoint(service, research.id, "verify")
    _, job = service.enqueue_research_finalization(research.id)

    processed = service.process_finalize_job(job.id)

    assert processed.status == FinalizeJobStatus.COMPLETED
    current = store.get_research(research.id)
    assert current.status == ResearchStatus.COMPLETED
    assert "Verified finding [S1]" in current.final_report
    assert service.analyzer.run_analysis.call_count == 0
    assert current.graph_state["step"] == "complete"


def test_resume_from_settled_verify_checkpoint_without_a_job():
    store, research, service = _service()
    _checkpoint(service, research.id, "verify")

    finalized = service.finalize_research(research.id)

    assert finalized.status == ResearchStatus.COMPLETED
    assert "Verified finding [S1]" in finalized.final_report
    assert service.analyzer.run_analysis.call_count == 0


def _hard_resume_after_analyze(**overrides):
    # A HARD analyze checkpoint with conflicts and a weakly supported draft: a fresh
    # verify would tie-break unless the run had stalled or spent its budget.
    store, research, service = _service(depth=SearchDepth.HARD)
    service.replan_agent.suggest_tie_breakers.return_value = []
    _checkpoint(
        service,
        research.id,
        "analyze",
        analyze_attempts=settings.langgraph_verification_max_retries + 1,  # no re-analysis
        report=SAVED_REPORT + "\n\nOne claim is weakly supported.",
        detected_conflicts=[{"claim": "x"}],
        **overrides,
    )
    service.finalize_research(research.id)
    return store, research, service


def test_resumed_run_keeps_the_no_progress_stall():
    _, _, service = _hard_resume_after_analyze(
        branch_stalled=True, finalize_deadline=time.time() + 3600
    )
    service.replan_agent.suggest_tie_breakers.assert_not_called()


def test_resumed_run_does_not_get_a_fresh_budget():
    _, _, service = _hard_resume_after_analyze(
        branch_stalled=False, finalize_deadline=time.time() - 5
    )
    service.replan_agent.suggest_tie_breakers.assert_not_called()


def test_legacy_checkpoint_without_stall_or_deadline_resumes_like_a_fresh_run():
    _, _, service = _hard_resume_after_analyze()
    service.replan_agent.suggest_tie_breakers.assert_called_once()


def test_checkpoint_persists_stall_and_deadline_and_resume_caps_the_deadline():
    store, research, service = _service(depth=SearchDepth.HARD)
    runner = FinalizeGraphRunner(service)
    far_future = time.time() + 10 * settings.finalize_budget_max_seconds
    runner._checkpoint(
        {
            "research_id": research.id,
            "prompt": "topic",
            "tasks": [],
            "branch_stalled": True,
            "finalize_deadline": far_future,
        },
        "replan",
        "stalled",
    )
    graph_state = store.get_research(research.id).graph_state
    assert graph_state["branch_stalled"] is True
    assert graph_state["finalize_deadline"] == far_future

    resumed = runner._build_initial_state(research.id, "topic", [], SearchDepth.HARD)
    assert resumed["branch_stalled"] is True
    # min(stored, now + budget): a stored deadline can shorten a resume, never extend it.
    assert resumed["finalize_deadline"] <= time.time() + settings.finalize_budget_max_seconds
