"""AUD-014: merge_research_graph_state must not clobber other writers' keys."""
from src.api.schemas import ResearchRequest, SearchDepth
from src.repositories import InMemoryTaskStore


def _research(store):
    return store.add_research(ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[])


def test_merge_preserves_existing_keys():
    store = InMemoryTaskStore()
    rec = _research(store)
    store.merge_research_graph_state(rec.id, {"citation_audit": {"x": 1}})
    store.merge_research_graph_state(rec.id, {"numeric_check": {"y": 2}})
    gs = store.get_research(rec.id).graph_state
    assert gs["citation_audit"] == {"x": 1}
    assert gs["numeric_check"] == {"y": 2}


def test_merge_overwrites_same_key_and_noops_on_missing():
    store = InMemoryTaskStore()
    rec = _research(store)
    store.merge_research_graph_state(rec.id, {"k": 1})
    store.merge_research_graph_state(rec.id, {"k": 2})
    assert store.get_research(rec.id).graph_state["k"] == 2
    assert store.merge_research_graph_state("does-not-exist", {"k": 1}) is None
