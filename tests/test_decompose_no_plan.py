"""NO-PLAN-RETRY: a decomposition that produced nothing to search fails without persisting
the plan, and a retry decomposes again instead of searching the degenerate plan.

decompose_and_enqueue used to save every returned task and drop the stored request before
it noticed that none had a searchable query, so retry_research (which re-decomposes only a
research with no tasks) searched the orchestrator's parse fallback or 'completed' the empty
tasks with 0 sources, and the orchestrator was never called again.
"""
from src.agents.orchestrator import OrchestratorAgent
from src.api.schemas import (
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

_PLAN = (
    '[{"description": "Primary sources", "queries": ["topic official report"]},'
    ' {"description": "Independent coverage", "queries": ["topic independent review"]}]'
)


class _ScriptedLLM:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.calls += 1
        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


class _Analyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report"


def _service(orchestrator):
    store = InMemoryTaskStore()
    return store, ResearchService(task_store=store, orchestrator=orchestrator, analyzer=_Analyzer())


def _start(service, depth=SearchDepth.MEDIUM):
    response, research_id = service.start_research(
        ResearchRequest(prompt="topic to research", depth=depth, thread_id="thread-7")
    )
    return research_id


def test_unparseable_plan_fails_without_persisting_it_and_keeps_the_request():
    llm = _ScriptedLLM("Sorry, I cannot help with that.")
    store, service = _service(OrchestratorAgent(llm))
    research_id = _start(service)

    service.decompose_and_enqueue(research_id, ResearchRequest(prompt="topic to research", depth=SearchDepth.MEDIUM))

    research = store.get_research(research_id)
    assert research.status == ResearchStatus.FAILED
    assert research.final_report == service.NO_SEARCH_PLAN_REPORT
    assert store.get_tasks_by_research(research_id) == [] and research.task_ids == []
    assert research.graph_state["decompose_payload"]["depth"] == "medium"
    assert "decompose_pending" not in research.graph_state  # recovery must not replay it


def test_retry_after_an_unsearchable_plan_decomposes_again():
    llm = _ScriptedLLM("not json", _PLAN)
    store, service = _service(OrchestratorAgent(llm))
    research_id = _start(service)
    service.decompose_and_enqueue(research_id, ResearchRequest(prompt="topic to research", depth=SearchDepth.MEDIUM))
    assert store.get_research(research_id).status == ResearchStatus.FAILED

    retried = service.retry_research(research_id)

    assert llm.calls == 2
    assert retried.status == ResearchStatus.PROCESSING
    tasks = store.get_tasks_by_research(research_id)
    assert sorted(task.queries[0] for task in tasks) == ["topic independent review", "topic official report"]
    for task in tasks:
        job = store.get_latest_search_task_job(task.id)
        assert job.status == SearchJobStatus.PENDING and job.depth == SearchDepth.MEDIUM  # the stored request
    assert "decompose_payload" not in store.get_research(research_id).graph_state


def test_a_plan_whose_tasks_have_no_queries_is_not_persisted():
    class _EmptyQueries:
        def run_decompose(self, prompt, depth, **kwargs):
            return [{"id": "empty-1", "description": "d", "queries": [], "status": TaskStatus.PENDING}]

    store, service = _service(_EmptyQueries())
    research_id = _start(service)

    service.decompose_and_enqueue(research_id, ResearchRequest(prompt="topic to research", depth=SearchDepth.EASY))

    assert store.get_research(research_id).status == ResearchStatus.FAILED
    assert store.get_task("empty-1") is None


def test_retry_of_a_degenerate_plan_persisted_by_an_older_build_decomposes_again():
    llm = _ScriptedLLM(_PLAN)
    store, service = _service(OrchestratorAgent(llm))
    research = store.add_research(ResearchRequest(prompt="topic to research", depth=SearchDepth.EASY), task_ids=[])
    # What the old decompose_and_enqueue left: the parse fallback saved, the request dropped.
    store.add_task(
        {
            "id": "fallback-1",
            "research_id": research.id,
            "description": "Error parsing LLM response",
            "queries": ["topic to research"],
            "status": TaskStatus.FAILED,
        }
    )
    store.set_research_task_ids(research.id, ["fallback-1"])
    store.update_research_status(research.id, ResearchStatus.FAILED, service.NO_SEARCH_PLAN_REPORT)

    service.retry_research(research.id)

    assert llm.calls == 1
    assert store.get_task("fallback-1") is None
    tasks = store.get_tasks_by_research(research.id)
    assert len(tasks) == 2 and all(store.get_latest_search_task_job(task.id) for task in tasks)
    assert "fallback-1" not in store.get_research(research.id).task_ids


def test_a_failed_search_is_still_retried_as_a_search():
    llm = _ScriptedLLM(_PLAN)
    store, service = _service(OrchestratorAgent(llm))
    research = store.add_research(ResearchRequest(prompt="topic to research", depth=SearchDepth.EASY), task_ids=[])
    store.add_task(
        {
            "id": "searched-1",
            "research_id": research.id,
            "description": "d",
            "queries": ["q"],
            "status": TaskStatus.FAILED,
            "logs": ["Agent started search process", "Error: provider down"],
        }
    )
    store.set_research_task_ids(research.id, ["searched-1"])
    store.update_research_status(research.id, ResearchStatus.FAILED, "All tasks failed.")

    service.retry_research(research.id)

    assert llm.calls == 0
    assert store.get_task("searched-1").status == TaskStatus.PENDING
    assert store.get_latest_search_task_job("searched-1").status == SearchJobStatus.PENDING
