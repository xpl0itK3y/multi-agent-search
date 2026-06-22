"""AUD-005: finalize enqueue must be an atomic single-winner transition."""
from src.api.schemas import ResearchRequest, ResearchStatus
from src.repositories.in_memory_task_store import InMemoryTaskStore


def _processing_research():
    store = InMemoryTaskStore()
    rec = store.add_research(ResearchRequest(prompt="hello world test"), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.PROCESSING)
    return store, rec.id


def test_try_begin_finalization_single_winner():
    store, rid = _processing_research()
    assert store.try_begin_finalization(rid) is True
    assert store.get_research(rid).status == ResearchStatus.ANALYZING
    # A concurrent/second caller must lose the CAS.
    assert store.try_begin_finalization(rid) is False


def test_try_begin_finalization_rejects_terminal_states():
    for terminal in (ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED):
        store, rid = _processing_research()
        store.update_research_status(rid, terminal)
        assert store.try_begin_finalization(rid) is False
