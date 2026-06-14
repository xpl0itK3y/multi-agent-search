"""Regression: the end-of-finalize token-usage write must not wipe the trust artifacts
(citation audit, source independence, numeric check) that the finalize steps stored.

Before the fix, finalize copied a stale graph_state snapshot captured at the start of
finalize and wrote it back together with token usage — erasing everything the trust steps
had persisted in between. This drove the stored trust layer empty for every real research.
"""
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import FinalizeWorker


def test_finalize_keeps_trust_artifacts_and_token_usage(mocker):
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="Compare A and B for analytics", depth=SearchDepth.EASY),
        task_ids=["t1"],
    )
    store.add_task({
        "id": "t1", "research_id": research.id, "description": "what is A vs B",
        "queries": ["q"], "status": TaskStatus.COMPLETED,
        "result": [{"url": "https://a.com", "title": "A", "content": "alpha grew 40% this year"}],
    })

    sources = [
        {"source_id": "S1", "url": "https://a.com", "domain": "a.com", "title": "A", "content": "alpha grew 40 percent this year"},
        {"source_id": "S2", "url": "https://b.org", "domain": "b.org", "title": "B", "content": "beta covers different unrelated ground"},
    ]
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "## Summary\nAlpha grew 40% [S1]. Beta differs entirely [S2]."
    analyzer._prepare_aggregated_data.return_value = (sources, None)
    analyzer.llm = mocker.Mock()
    analyzer.llm.token_usage = {"prompt_tokens": 100, "completion_tokens": 50, "estimated_cost_usd": 0.01}

    service = ResearchService(task_store=store, analyzer=analyzer)
    service.enqueue_research_finalization(research.id)
    assert FinalizeWorker(service).run_once() == 1

    gs = store.get_research(research.id).graph_state
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED
    # token usage persisted …
    assert gs.get("llm_token_usage", {}).get("prompt_tokens") == 100
    # … AND the trust artifacts the finalize steps stored survived the token write
    assert "citation_audit" in gs
    assert "source_independence" in gs
    assert "source_reputation" in gs
    assert "numeric_check" in gs
