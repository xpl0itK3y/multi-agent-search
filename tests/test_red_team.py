import json
from types import SimpleNamespace

from src.agents.red_team import RedTeamAgent
from src.api.schemas import RedTeamFinding, RedTeamReport, SearchDepth
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


class _StubLLM:
    """Returns claims on the extract call and verdicts on the judge call."""

    def __init__(self):
        self.models = []

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.models.append(kwargs.get("model"))
        if "red-team analyst" in system_prompt:  # extract step
            return json.dumps(
                [{"claim": "X is the fastest", "counter_queries": ["X slow benchmark", "X criticism"]}]
            )
        return json.dumps(  # judge step
            [{"claim": "X is the fastest", "verdict": "contested", "challenge": "Some benchmarks show Y ahead."}]
        )


def _search_fn(query):
    return [{"url": "https://a.com", "title": "Bench", "content": "Y beats X in some tests."}]


def test_red_team_challenge_produces_findings():
    rt = RedTeamAgent(_StubLLM()).challenge(
        "compare X and Y", "X is the fastest [S1].", _search_fn, language="en", model="deepseek-chat"
    )
    assert rt.challenged == 1 and rt.held == 0
    assert rt.findings[0].verdict == "contested"
    assert rt.findings[0].source_urls == ["https://a.com"]


def test_red_team_routes_to_configured_model():
    stub = _StubLLM()
    RedTeamAgent(stub).challenge("topic", "report text", _search_fn, model="deepseek-chat")
    assert stub.models and all(m == "deepseek-chat" for m in stub.models)


def test_red_team_without_llm_returns_empty():
    rt = RedTeamAgent(None).challenge("topic", "report", _search_fn)
    assert rt.findings == [] and isinstance(rt, RedTeamReport)


def test_red_team_empty_report_returns_empty():
    assert RedTeamAgent(_StubLLM()).challenge("topic", "   ", _search_fn).findings == []


def test_red_team_tolerates_prose_wrapped_json():
    class ProseLLM(_StubLLM):
        def generate(self, system_prompt, user_prompt, **kwargs):
            return "Here you go:\n" + super().generate(system_prompt, user_prompt, **kwargs) + "\nDone."

    rt = RedTeamAgent(ProseLLM()).challenge("t", "report", _search_fn)
    assert rt.findings and rt.findings[0].verdict == "contested"


def test_maybe_red_team_skips_when_not_hard():
    svc = ResearchService(task_store=InMemoryTaskStore(), red_team_agent=RedTeamAgent(_StubLLM()))
    research = SimpleNamespace(id="r1", prompt="p", depth=SearchDepth.EASY)
    assert svc._maybe_red_team("report body", research, []) == "report body"  # unchanged


def test_maybe_red_team_noop_without_agent():
    svc = ResearchService(task_store=InMemoryTaskStore())  # red_team_agent=None
    research = SimpleNamespace(id="r1", prompt="p", depth=SearchDepth.HARD)
    assert svc._maybe_red_team("report body", research, []) == "report body"


def test_render_red_team_section_localizes_verdicts():
    svc = ResearchService(task_store=InMemoryTaskStore())
    rt = RedTeamReport(findings=[RedTeamFinding(claim="C", verdict="contested", challenge="ch")])
    out = svc._render_red_team_section(rt, "ru")
    assert "Оспаривается" in out and "C" in out and "ch" in out
    assert "Weaknesses" in svc._render_red_team_section(rt, "en")
