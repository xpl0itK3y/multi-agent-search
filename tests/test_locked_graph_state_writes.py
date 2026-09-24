"""LOCKED-MERGE twins: graph_state writes that must not be computed from an earlier read.

A merge is atomic, but a patch built from a snapshot read before the lock still loses
updates: two chat turns each appended to their own copy of `messages`, and stale
finalize recovery merged a pre-read copy of the whole graph_state back.
"""
import threading
from datetime import datetime, timedelta, timezone

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def _research(store):
    return store.add_research(ResearchRequest(prompt="locked writes", depth=SearchDepth.EASY), task_ids=[])


def test_concurrent_chat_turns_keep_both_messages():
    store = InMemoryTaskStore()
    record = _research(store)
    service = ResearchService(task_store=store)
    # Hold every reader until both turns have read: a turn that appended to a list it
    # read earlier would now overwrite the other turn's message.
    both_read = threading.Barrier(2, timeout=0.5)
    read = store.get_research

    def racing_read(research_id):
        snapshot = read(research_id).model_copy(deep=True)  # detached, like a SQL read
        try:
            both_read.wait()
        except threading.BrokenBarrierError:
            pass
        return snapshot

    store.get_research = racing_read
    turns = [
        threading.Thread(target=service.append_research_message, args=(record.id, "user", text))
        for text in ("first question", "second question")
    ]
    for turn in turns:
        turn.start()
    for turn in turns:
        turn.join()
    store.get_research = read

    contents = sorted(message.content for message in service.list_research_messages(record.id))
    assert contents == ["first question", "second question"]


def test_chat_history_is_capped_to_the_most_recent_messages():
    store = InMemoryTaskStore()
    record = _research(store)
    service = ResearchService(task_store=store)

    for index in range(ResearchService._CHAT_HISTORY_LIMIT + 3):
        service.append_research_message(record.id, "user", f"q{index}")

    messages = service.list_research_messages(record.id)
    assert len(messages) == ResearchService._CHAT_HISTORY_LIMIT
    assert messages[-1].content == f"q{ResearchService._CHAT_HISTORY_LIMIT + 2}"


def test_stale_finalize_recovery_patches_only_its_flag(monkeypatch):
    store = InMemoryTaskStore()
    record = _research(store)
    store.update_research_status(record.id, ResearchStatus.ANALYZING)
    store.merge_research_graph_state(record.id, {"step": "verify", "citation_audit": {"v": "old"}})
    job = store.add_research_finalize_job(record.id)
    job.status = job.status.RUNNING
    job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    service = ResearchService(task_store=store)
    read = store.get_research

    def read_then_runner_writes(research_id):
        snapshot = read(research_id).model_copy(deep=True)  # detached, like a SQL read
        # The fenced runner is still alive and lands a trust-suite write after the read.
        store.merge_research_graph_state(research_id, {"citation_audit": {"v": "new"}})
        return snapshot

    monkeypatch.setattr(store, "get_research", read_then_runner_writes)
    monkeypatch.setattr("src.services.job_queue_mixin.settings.finalize_job_timeout_seconds", 60)

    assert service.recover_stale_research_finalize_jobs().recovered_count == 1

    graph_state = read(record.id).graph_state
    assert graph_state["citation_audit"] == {"v": "new"}
    assert graph_state["resume_after_stale_recovery"] is True
    assert graph_state["step"] == "verify"
