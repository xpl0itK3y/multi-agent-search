import pytest
from fastapi import HTTPException

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


def _setup():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="Is X better than Y?", depth=SearchDepth.MEDIUM), task_ids=[], user_id="u1")
    store.add_task({
        "id": "t1", "research_id": rec.id, "description": "What is X?",
        "queries": ["x overview", "x benchmarks"], "status": TaskStatus.COMPLETED,
        "result": [{"url": "https://a.com/x", "title": "X", "domain": "a.com", "source_quality": "high", "content": "about x"}],
    })
    store.add_task({
        "id": "t2", "research_id": rec.id, "description": "What is Y?",
        "queries": ["y overview"], "status": TaskStatus.COMPLETED,
        "result": [{"url": "https://b.org/y", "title": "Y", "domain": "b.org", "source_quality": "medium", "content": "about y"}],
    })
    store.set_research_task_ids(rec.id, ["t1", "t2"])
    store.update_research_graph_state(rec.id, {
        "model": "deepseek-v4-pro",
        "llm_token_usage": {"prompt_tokens": 1000, "completion_tokens": 500, "estimated_cost_usd": 0.012},
        "analyze_attempts": 2, "replan_attempts": 1,
        "red_team": {"challenged": 2, "held": 3},
        "source_independence": {"total_sources": 2, "independent_origins": 2},
    })
    store.append_research_graph_event(rec.id, {"step": "collect_context", "detail": "gathered 2 sources", "timestamp": "2026-06-14T10:00:00"})
    store.append_research_graph_event(rec.id, {"step": "analyze", "detail": "draft generated"})
    store.update_research_status(rec.id, ResearchStatus.COMPLETED)
    return svc, rec.id


def test_audit_trail_assembles_full_provenance():
    svc, rid = _setup()
    t = svc.get_research_audit_trail(rid)
    assert t.prompt == "Is X better than Y?"
    assert t.model == "deepseek-v4-pro" and t.depth == "medium" and t.status == "completed"
    assert t.plan == ["What is X?", "What is Y?"]
    assert t.query_count == 3
    assert {q.task for q in t.queries} == {"What is X?", "What is Y?"}
    assert t.source_count == 2
    assert any("analyze passes: 2" in d for d in t.decisions)
    assert any("replan" in d for d in t.decisions)
    assert any("red-team" in d for d in t.decisions)
    assert [s.step for s in t.steps] == ["collect_context", "analyze"]
    assert t.token_usage.get("prompt_tokens") == 1000
    assert t.completed_at  # terminal → set


def test_audit_trail_markdown_render():
    svc, rid = _setup()
    md = svc._render_audit_trail_md(svc.get_research_audit_trail(rid))
    assert md.startswith("# Audit trail —")
    assert "## Plan (sub-questions)" in md
    assert "## Search queries" in md
    assert "`x benchmarks`" in md
    assert "## Sources fetched" in md
    assert "deepseek-v4-pro" in md


def test_audit_trail_export_format():
    svc, rid = _setup()
    # mark a final report so the export endpoint's readiness guard passes
    svc.task_store.update_research_status(rid, ResearchStatus.COMPLETED, "# Report\nBody [S1].")
    data, media, filename = svc.export_research_report(rid, "trail")
    assert media.startswith("text/markdown")
    assert filename.endswith("-audit-trail.md")
    assert b"Audit trail" in data


def test_audit_trail_missing_research_404():
    svc = ResearchService(task_store=InMemoryTaskStore())
    with pytest.raises(HTTPException):
        svc.get_research_audit_trail("does-not-exist")
