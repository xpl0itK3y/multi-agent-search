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


# SUMMARY-LLM: /summary computes the follow-ups once per task set and stores them.


def test_follow_up_is_computed_once_and_served_from_graph_state():
    svc, calls = _svc_with_spy()
    rec = _research_with_task(svc, "completed")

    svc.get_research_summary(rec.id)
    svc.get_research_summary(rec.id)

    assert calls == [1]
    graph_state = svc.task_store.get_research(rec.id).graph_state
    assert graph_state["summary_follow_up"]["recommendations"] == []
    assert "replan_recommendations" not in graph_state  # the graph's own key is left alone


def test_follow_up_is_recomputed_when_the_tasks_change():
    svc, calls = _svc_with_spy()
    rec = _research_with_task(svc, "completed")
    svc.get_research_summary(rec.id)

    svc.task_store.add_task(
        {"id": "t2", "research_id": rec.id, "description": "d", "queries": ["q"], "status": "failed"}
    )
    svc.get_research_summary(rec.id)

    assert calls == [1, 1]


class _CountingLLM:
    def __init__(self):
        self.calls = 0

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.calls += 1
        return '["specific query one", "specific query two", "specific query three"]'


def _research_with_every_gap(svc):
    # MEDIUM, thin coverage, a failed branch, no primary sources and a single dominant
    # domain: four gaps, of which only three can become recommendations.
    rec = svc.task_store.add_research(
        ResearchRequest(prompt="hello world topic", depth=SearchDepth.MEDIUM), task_ids=[]
    )
    svc.task_store.add_task(
        {
            "id": "t1",
            "research_id": rec.id,
            "description": "d",
            "queries": ["q"],
            "status": "completed",
            "result": [{"url": "https://blog.example.com/a", "domain": "blog.example.com", "content": "Body " * 40}],
        }
    )
    svc.task_store.add_task(
        {"id": "t2", "research_id": rec.id, "description": "d", "queries": ["q"], "status": "failed"}
    )
    return rec


def test_summary_makes_at_most_three_llm_calls_and_only_on_the_first_fetch():
    from src.agents.replan import ReplanAgent

    llm = _CountingLLM()
    svc = ResearchService(task_store=InMemoryTaskStore(), replan_agent=ReplanAgent(llm=llm))
    rec = _research_with_every_gap(svc)

    first = svc.get_research_summary(rec.id)
    second = svc.get_research_summary(rec.id)

    assert llm.calls == 3  # the fourth gap is no longer generated just to be discarded
    assert len(first.replan_recommendations) == 3
    assert second.replan_recommendations == first.replan_recommendations


def test_summary_route_is_llm_rate_limited():
    from src.api.app import create_app
    from src.auth.llm_rate_limit import enforce_llm_rate_limit

    route = next(
        route for route in create_app().routes
        if getattr(route, "path", None) == "/v1/research/{research_id}/summary"
    )
    assert enforce_llm_rate_limit in [dependency.call for dependency in route.dependant.dependencies]


# SUMMARY-DEGRADED: templates computed while the LLM was failing are not cached for good.


class _FlakyLLM:
    def __init__(self):
        self.calls = 0
        self.failing = True

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.calls += 1
        if self.failing:
            raise RuntimeError("429 rate limited")
        return '["specific query one", "specific query two", "specific query three"]'


def _all_queries(summary):
    return [query for item in summary.replan_recommendations for query in item.suggested_queries]


def test_follow_up_computed_while_the_llm_fails_is_retried_after_a_short_ttl(monkeypatch):
    from datetime import datetime, timedelta, timezone

    from src.agents.replan import ReplanAgent

    llm = _FlakyLLM()
    svc = ResearchService(task_store=InMemoryTaskStore(), replan_agent=ReplanAgent(llm=llm))
    rec = _research_with_every_gap(svc)

    degraded = svc.get_research_summary(rec.id)
    assert llm.calls == 3
    assert "specific query one" not in _all_queries(degraded)  # template fallback
    stored = svc.task_store.get_research(rec.id).graph_state["summary_follow_up"]
    retry_after = datetime.fromisoformat(stored["retry_after"])
    assert timedelta(0) < retry_after - datetime.now(timezone.utc) <= timedelta(seconds=300)

    llm.failing = False
    # Within the TTL an outage does not cost LLM calls on every GET.
    assert svc.get_research_summary(rec.id).replan_recommendations == degraded.replan_recommendations
    assert llm.calls == 3

    svc.task_store.merge_research_graph_state(
        rec.id,
        {"summary_follow_up": {**stored, "retry_after": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}},
    )
    recovered = svc.get_research_summary(rec.id)
    assert llm.calls == 6
    assert "specific query one" in _all_queries(recovered)
    healthy = svc.task_store.get_research(rec.id).graph_state["summary_follow_up"]
    assert "retry_after" not in healthy  # a clean result is cached until the tasks change

    svc.get_research_summary(rec.id)
    assert llm.calls == 6


def test_template_only_follow_up_is_cached_without_a_retry():
    from src.agents.replan import ReplanAgent

    svc = ResearchService(task_store=InMemoryTaskStore(), replan_agent=ReplanAgent(llm=None))
    rec = _research_with_every_gap(svc)

    svc.get_research_summary(rec.id)

    assert "retry_after" not in svc.task_store.get_research(rec.id).graph_state["summary_follow_up"]


def test_replan_agent_reports_the_gaps_that_fell_back_after_an_llm_error():
    from src.agents.replan import ReplanAgent

    svc = ResearchService(task_store=InMemoryTaskStore())
    rec = _research_with_every_gap(svc)
    tasks = svc.task_store.get_tasks_by_research(rec.id)

    class _GarbageLLM:
        def generate(self, system_prompt, user_prompt, **kwargs):
            return "not json at all"

    for llm, expected in ((_FlakyLLM(), 3), (_GarbageLLM(), 3), (_CountingLLM(), 0), (None, 0)):
        failures: list[str] = []
        ReplanAgent(llm=llm).suggest_follow_up("hello world topic", SearchDepth.MEDIUM, tasks, llm_failures=failures)
        assert len(failures) == expected, llm
