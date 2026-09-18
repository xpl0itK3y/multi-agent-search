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


def test_merge_removes_keys_and_patches_atomically():
    store = InMemoryTaskStore()
    rec = _research(store)
    store.merge_research_graph_state(
        rec.id, {"decompose_pending": True, "decompose_payload": {"prompt": "x"}, "keep": 1}
    )
    store.merge_research_graph_state(rec.id, {"plan": []}, remove_keys=["decompose_pending", "decompose_payload"])
    gs = store.get_research(rec.id).graph_state
    assert "decompose_pending" not in gs and "decompose_payload" not in gs
    assert gs["keep"] == 1 and "plan" in gs
    # Removal only, no patch.
    store.merge_research_graph_state(rec.id, remove_keys=["keep"])
    assert "keep" not in store.get_research(rec.id).graph_state


def test_concurrent_merges_lose_no_keys():
    import threading

    store = InMemoryTaskStore()
    rec = _research(store)
    writers, iterations = 2, 100

    def write(writer_index):
        for i in range(iterations):
            store.merge_research_graph_state(rec.id, {f"key-{writer_index}-{i}": i})

    threads = [threading.Thread(target=write, args=(w,)) for w in range(writers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    graph_state = store.get_research(rec.id).graph_state
    assert all(f"key-{w}-{i}" in graph_state for w in range(writers) for i in range(iterations))
