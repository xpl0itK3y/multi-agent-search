"""AUD-033: complete_research_finalization always returns a ResearchRecord (never a bare None),
so finalize_research and its callers can rely on the -> ResearchRecord contract."""
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def test_returns_record_when_cancelled_before_analysis():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store)
    rec = store.add_research(ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.CANCELLED)

    result = svc.complete_research_finalization(rec.id)

    assert result is not None  # was a bare `return` (None) before AUD-033
    assert result.id == rec.id
    assert result.status == ResearchStatus.CANCELLED
