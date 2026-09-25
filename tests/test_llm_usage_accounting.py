"""USAGE-ACCOUNTING: llm_usage_logs gets one row per LLM call, written by the provider's
usage sink and attributed to the research/user bound where the call was made. It used to
get one row per successful finalization, read from the process-wide provider counter."""
import threading
import time
from types import SimpleNamespace

import pytest

from src.agents.optimizer import PromptOptimizerAgent
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.bootstrap import _llm_usage_sink
from src.observability import bind_observability_context, get_observability_context
from src.providers.deepseek import DeepSeekProvider, calculate_deepseek_cost
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import FinalizeWorker


class _StubCompletions:
    def __init__(self, barrier: threading.Barrier | None = None):
        self.barrier = barrier

    def create(self, model, messages, stream, **kwargs):
        if self.barrier is not None:
            self.barrier.wait(timeout=5)  # both calls are in flight at once
        usage = SimpleNamespace(prompt_tokens=1234, completion_tokens=567, prompt_cache_hit_tokens=1000)
        return SimpleNamespace(usage=usage, choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])


def _provider(sink, barrier=None) -> DeepSeekProvider:
    provider = DeepSeekProvider(api_key="sk-test", model="deepseek-v4-pro")
    provider.client = SimpleNamespace(chat=SimpleNamespace(completions=_StubCompletions(barrier)))
    provider.set_usage_sink(sink)
    return provider


def test_each_call_is_recorded_with_its_context_model_and_unrounded_cost():
    records = []
    provider = _provider(lambda **usage: records.append(usage))

    with bind_observability_context(research_id="research-1", user_id="user-1"):
        provider.generate("sys", "user", model="deepseek-chat")
    provider.generate("sys", "user")  # nothing bound: unattributed, still recorded

    assert len(records) == 2
    first = records[0]
    assert first["research_id"] == "research-1"
    assert first["user_id"] == "user-1"
    assert first["model"] == "deepseek-chat"  # the model sent, not the provider default
    assert (first["prompt_tokens"], first["completion_tokens"], first["cache_hit_tokens"]) == (1234, 567, 1000)
    expected = calculate_deepseek_cost("deepseek-chat", 1234, 567, cache_hit_tokens=1000)
    assert first["estimated_cost_usd"] == pytest.approx(expected, rel=1e-12)
    assert first["estimated_cost_usd"] != round(expected, 4)  # not pre-rounded
    assert records[1]["research_id"] is None and records[1]["model"] == "deepseek-v4-pro"


def test_concurrent_calls_are_attributed_to_their_own_research():
    records = []
    lock = threading.Lock()

    def sink(**usage):
        with lock:
            records.append(usage)

    provider = _provider(sink, barrier=threading.Barrier(2))

    def call(research_id: str):
        with bind_observability_context(research_id=research_id, user_id=f"owner-{research_id}"):
            provider.generate("sys", "user")

    threads = [threading.Thread(target=call, args=(rid,)) for rid in ("r-a", "r-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert sorted((r["research_id"], r["user_id"]) for r in records) == [
        ("r-a", "owner-r-a"),
        ("r-b", "owner-r-b"),
    ]


def test_a_failing_sink_does_not_fail_the_call(caplog):
    def sink(**usage):
        raise RuntimeError("db down")

    provider = _provider(sink)
    with caplog.at_level("WARNING", logger="src.providers.deepseek"):
        assert provider.generate("sys", "user") == "ok"
    assert "llm_usage_record_failed" in caplog.text


def test_bootstrap_sink_writes_a_row_and_drops_the_local_user():
    store = InMemoryTaskStore()
    store.create_user("u1", "u1@example.com", None)
    research = store.add_research(ResearchRequest(prompt="sink topic", depth=SearchDepth.EASY), task_ids=[])
    sink = _llm_usage_sink(store)

    sink(research_id=research.id, user_id="local", model="deepseek-chat", prompt_tokens=10,
         completion_tokens=5, cache_hit_tokens=3, estimated_cost_usd=0.000012345)
    sink(research_id=None, user_id="u1", model="deepseek-v4-pro", prompt_tokens=1,
         completion_tokens=1, cache_hit_tokens=0, estimated_cost_usd=0.0)

    first, second = store.llm_usage_logs
    assert first["user_id"] is None  # LOCAL_USER has no users row (FK on Postgres)
    assert (first["research_id"], first["model"], first["total_tokens"]) == (research.id, "deepseek-chat", 15)
    assert first["cache_hit_tokens"] == 3
    assert first["estimated_cost_usd"] == 0.000012345
    assert (second["user_id"], second["research_id"]) == ("u1", None)


def test_api_process_optimize_call_is_recorded_for_its_user():
    store = InMemoryTaskStore()
    store.create_user("user-42", "user-42@example.com", None)
    provider = _provider(_llm_usage_sink(store))
    service = ResearchService(task_store=store, optimizer=PromptOptimizerAgent(provider))

    service.optimize_prompt("make this better", user_id="user-42")

    assert [(row["user_id"], row["research_id"]) for row in store.llm_usage_logs] == [("user-42", None)]


def test_summary_follow_up_runs_bound_to_the_research_owner():
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    seen = []
    service.replan_agent.suggest_follow_up = lambda *a, **k: seen.append(get_observability_context()) or []
    research = store.add_research(
        ResearchRequest(prompt="hello world", depth=SearchDepth.EASY), task_ids=[], user_id="owner-1"
    )
    store.add_task({"id": "t1", "research_id": research.id, "description": "d", "queries": ["q"],
                    "status": TaskStatus.COMPLETED})

    service.get_research_summary(research.id)

    assert seen and seen[0]["research_id"] == research.id and seen[0]["user_id"] == "owner-1"


def test_finalization_no_longer_writes_its_own_usage_row(mocker):
    """The provider records every call as it happens; the old end-of-finalize write of the
    shared counter would count them twice. graph_state keeps the per-research figure."""
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="Compare A and B", depth=SearchDepth.EASY), task_ids=["t1"]
    )
    store.add_task({"id": "t1", "research_id": research.id, "description": "A vs B", "queries": ["q"],
                    "status": TaskStatus.COMPLETED,
                    "result": [{"url": "https://a.com", "title": "A", "content": "alpha"}]})
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "## Summary\nAlpha [S1]."
    analyzer._prepare_aggregated_data.return_value = (
        [{"source_id": "S1", "url": "https://a.com", "domain": "a.com", "title": "A", "content": "alpha"}],
        None,
    )
    analyzer.llm = mocker.Mock()
    analyzer.llm.token_usage = {"prompt_tokens": 100, "completion_tokens": 50, "estimated_cost_usd": 0.01}

    service = ResearchService(task_store=store, analyzer=analyzer)
    service.enqueue_research_finalization(research.id)
    assert FinalizeWorker(service).run_once() == 1

    assert store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert store.llm_usage_logs == []
    assert store.get_research(research.id).graph_state["llm_token_usage"]["prompt_tokens"] == 100


@pytest.mark.postgres
def test_sqlalchemy_sink_rows_satisfy_the_owner_foreign_keys(postgres_session_factory):
    from sqlalchemy import select

    from src.db.models import LLMUsageLogORM
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

    store = SQLAlchemyTaskStore(postgres_session_factory)
    store.delete_user("usage-owner")
    store.create_user("usage-owner", "usage-owner@example.com", None)
    research = store.add_research(
        ResearchRequest(prompt="usage topic", depth=SearchDepth.EASY), task_ids=[], user_id="usage-owner"
    )
    sink = _llm_usage_sink(store)

    sink(research_id=research.id, user_id="usage-owner", model="deepseek-chat", prompt_tokens=10,
         completion_tokens=5, cache_hit_tokens=4, estimated_cost_usd=0.0000123456)
    sink(research_id=None, user_id="local", model="deepseek-v4-pro", prompt_tokens=1,
         completion_tokens=1, cache_hit_tokens=0, estimated_cost_usd=0.0)

    with postgres_session_factory() as session:
        rows = session.execute(select(LLMUsageLogORM).order_by(LLMUsageLogORM.model)).scalars().all()
    assert [(r.model, r.user_id, r.research_id, r.cache_hit_tokens) for r in rows] == [
        ("deepseek-chat", "usage-owner", research.id, 4),
        ("deepseek-v4-pro", None, None, 0),
    ]
    assert rows[0].estimated_cost_usd == pytest.approx(0.0000123456)
    assert store.get_user_token_analytics("usage-owner")["total_tokens"] == 15
    store.delete_user("usage-owner")


def _wait_for_a_lock_wait(session_factory, timeout: float = 5.0) -> None:
    from sqlalchemy import text

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with session_factory() as session:
            if session.execute(text("SELECT count(*) FROM pg_locks WHERE NOT granted")).scalar_one():
                return
        time.sleep(0.05)
    raise AssertionError("the usage insert never waited on the deleted research")


@pytest.mark.postgres
def test_sqlalchemy_usage_row_survives_a_delete_committed_mid_insert(postgres_session_factory):
    """The race the owner lookup cannot close: the research is deleted after the INSERT
    read it, and its FK check fails once the delete commits. The row is written again
    with that owner NULL instead of being dropped (the call was billed)."""
    from sqlalchemy import delete, select

    from src.db.models import LLMUsageLogORM, ResearchORM
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

    store = SQLAlchemyTaskStore(postgres_session_factory)
    store.delete_user("race-owner")
    store.create_user("race-owner", "race-owner@example.com", None)
    research = store.add_research(
        ResearchRequest(prompt="race topic", depth=SearchDepth.EASY), task_ids=[], user_id="race-owner"
    )
    errors: list[Exception] = []

    def record() -> None:
        try:
            store.record_llm_usage(research.id, "race-owner", "deepseek-chat", 3, 2, 5, 0.01)
        except Exception as exc:  # surfaced by the assertion below
            errors.append(exc)

    with postgres_session_factory() as deleter:
        deleter.execute(delete(ResearchORM).where(ResearchORM.id == research.id))  # row locked, not committed
        writer = threading.Thread(target=record)
        writer.start()
        _wait_for_a_lock_wait(postgres_session_factory)  # the FK check waits on the deleted row
        deleter.commit()
    writer.join(timeout=10)

    assert errors == []
    with postgres_session_factory() as session:
        rows = session.execute(select(LLMUsageLogORM.research_id, LLMUsageLogORM.user_id)).all()
    assert [tuple(row) for row in rows] == [(None, "race-owner")]
    store.delete_user("race-owner")
