import json

from src.agents.stance import StanceAgent
from src.core.llm import LLMProvider


class _StubLLM(LLMProvider):
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.calls += 1
        return self.payload


_SOURCES = {
    "S1": {"content": "Nuclear power is the fastest way to decarbonise the grid."},
    "S2": {"content": "Nuclear is too slow and expensive to help with climate goals."},
    "S3": {"content": "Renewables plus storage already outcompete new nuclear on cost."},
    "S4": {"content": "A neutral overview of various low-carbon energy options."},
}


def test_balance_aggregates_stances():
    payload = json.dumps({
        "proposition": "Nuclear power is the best path to decarbonise",
        "stances": [
            {"source_id": "S1", "stance": "supports"},
            {"source_id": "S2", "stance": "opposes"},
            {"source_id": "S3", "stance": "opposes"},
            {"source_id": "S4", "stance": "neutral"},
        ],
    })
    r = StanceAgent(_StubLLM(payload)).assess("Is nuclear the best path to decarbonise?", _SOURCES)
    assert r.applicable is True
    assert r.supports == 1 and r.opposes == 2 and r.neutral == 1
    assert r.dominant_side == "opposes"
    assert r.skew == 0.6667  # 2 of 3 decisive on one side, rounded to 4dp


def test_ignores_unknown_ids_and_bad_labels():
    payload = json.dumps({
        "proposition": "X is good",
        "stances": [
            {"source_id": "S1", "stance": "supports"},
            {"source_id": "S9", "stance": "supports"},   # not in sources → dropped
            {"source_id": "S2", "stance": "maybe"},       # invalid label → dropped
            {"source_id": "S3", "stance": "opposes"},
        ],
    })
    r = StanceAgent(_StubLLM(payload)).assess("Is X good?", _SOURCES)
    assert r.supports == 1 and r.opposes == 1
    assert {s.source_id for s in r.sources} == {"S1", "S3"}


def test_not_applicable_when_all_neutral():
    payload = json.dumps({"proposition": "p", "stances": [
        {"source_id": "S1", "stance": "neutral"}, {"source_id": "S2", "stance": "neutral"},
    ]})
    assert StanceAgent(_StubLLM(payload)).assess("q", _SOURCES).applicable is False


def test_handles_garbage_and_no_llm():
    assert StanceAgent(_StubLLM("not json at all")).assess("q", _SOURCES).applicable is False
    assert StanceAgent(None).assess("q", _SOURCES).applicable is False
    assert StanceAgent(_StubLLM("{}")).assess("q", {}).applicable is False


def test_service_gates_and_stores_stance():
    from src.api.schemas import ResearchRequest, SearchDepth, StanceBalance, StanceSource
    from src.repositories.in_memory_task_store import InMemoryTaskStore
    from src.services.research_service import ResearchService

    class _Analyzer:
        def _prepare_aggregated_data(self, prompt, tasks, depth):
            return ([{"source_id": "S1", "content": "pro"}, {"source_id": "S2", "content": "con"}], None)

    class _Stance:
        def assess(self, prompt, sources_by_id, language="ru", model=None):
            return StanceBalance(
                applicable=True, supports=1, opposes=1, dominant_side="balanced", skew=0.5,
                sources=[StanceSource(source_id="S1", stance="supports"), StanceSource(source_id="S2", stance="opposes")],
            )

    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store, analyzer=_Analyzer(), stance_agent=_Stance())

    contestable = store.add_research(ResearchRequest(prompt="Is nuclear power worth it?", depth=SearchDepth.EASY), task_ids=[])
    svc._maybe_assess_stance(store.get_research(contestable.id), [])
    assert svc.get_research_stance(contestable.id).applicable is True

    factual = store.add_research(ResearchRequest(prompt="What is the capital of France?", depth=SearchDepth.EASY), task_ids=[])
    svc._maybe_assess_stance(store.get_research(factual.id), [])
    assert svc.get_research_stance(factual.id).applicable is False  # not contestable → skipped
