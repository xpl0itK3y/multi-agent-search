"""AUD-022: the LLM-backed follow-up recommendations in /summary only run once the research
has finished — an on-demand fetch mid-run must not trigger replan LLM calls."""
from src.api.schemas import ResearchRequest, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def _svc_with_spy():
    svc = ResearchService(task_store=InMemoryTaskStore())
    calls = []
    svc.replan_agent.suggest_follow_up = lambda *a, **k: calls.append(1) or []
    return svc, calls


def _research_with_task(svc, status):
    rec = svc.task_store.add_research(
        ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[]
    )
    svc.task_store.add_task(
        {"id": "t1", "research_id": rec.id, "description": "d", "queries": ["q"], "status": status}
    )
    return rec


def test_no_llm_followup_while_in_progress():
    svc, calls = _svc_with_spy()
    rec = _research_with_task(svc, "running")
    summary = svc.get_research_summary(rec.id)
    assert calls == []  # in-progress => no replan LLM call
    assert summary.replan_recommendations == []


def test_llm_followup_runs_when_finalize_ready():
    svc, calls = _svc_with_spy()
    rec = _research_with_task(svc, "completed")
    svc.get_research_summary(rec.id)
    assert calls == [1]  # all branches done => follow-up computed
