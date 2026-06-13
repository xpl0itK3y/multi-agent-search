from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.brokers.redis_broker import RedisBroker
from src.repositories.in_memory_task_store import InMemoryTaskStore


def test_store_emits_change_on_state_mutations():
    store = InMemoryTaskStore()
    seen: list[str] = []
    store.set_event_notifier(lambda rid: seen.append(rid))
    rec = store.add_research(ResearchRequest(prompt="topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.PROCESSING)
    store.save_partial_report(rec.id, "partial")
    store.save_partial_reasoning(rec.id, "reasoning")
    store.append_research_graph_event(rec.id, {"step": "x", "detail": "y"})
    assert seen.count(rec.id) >= 4  # one ping per mutating call


def test_store_without_notifier_does_not_crash():
    store = InMemoryTaskStore()  # no notifier set
    rec = store.add_research(ResearchRequest(prompt="topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.PROCESSING)  # must not raise


def test_research_channel_name_and_publish_is_best_effort():
    assert RedisBroker._research_channel("abc") == "mas:rt:abc"
    # No Redis server here — publish must swallow the connection error, not raise.
    RedisBroker("redis://127.0.0.1:6399").publish_research_event("abc")
