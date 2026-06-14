from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import SearchDepth
from src.config import settings
from src.core.llm import LLMProvider


class _StubLLM(LLMProvider):
    def __init__(self, response: str):
        self.response = response
        self.calls = 0

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls += 1
        return self.response


_DRAFT = "## Findings\nRevenue grew sharply [S1]. Margins held steady [S2]. Costs fell [S3]."


def test_editor_applies_when_it_preserves_citations():
    edited = "## Executive summary\nStrong year [S1].\n\n## Findings\nRevenue grew [S1]; margins held [S2]; costs fell [S3]."
    agent = AnalyzerAgent(_StubLLM(edited))
    out = agent._maybe_edit_report(_DRAFT, "How did the company do?", "en", SearchDepth.MEDIUM, None, ["revenue", "margins"])
    assert out == edited and agent.llm.calls == 1


def test_editor_discarded_when_it_drops_citations():
    rogue = "## Summary\nThe company did well overall, with broad improvements across the board."  # no [Sn]
    agent = AnalyzerAgent(_StubLLM(rogue))
    out = agent._maybe_edit_report(_DRAFT, "q", "en", SearchDepth.HARD, None, None)
    assert out == _DRAFT  # citation guard → keep the original


def test_editor_discarded_when_it_truncates():
    agent = AnalyzerAgent(_StubLLM("[S1]"))  # preserves a citation but far too short
    out = agent._maybe_edit_report(_DRAFT, "q", "en", SearchDepth.MEDIUM, None, None)
    assert out == _DRAFT


def test_editor_skipped_on_easy_depth():
    agent = AnalyzerAgent(_StubLLM("anything [S1][S2][S3]"))
    out = agent._maybe_edit_report(_DRAFT, "q", "en", SearchDepth.EASY, None, None)
    assert out == _DRAFT and agent.llm.calls == 0  # not worth the call on EASY


def test_editor_respects_disable_flag():
    original = settings.report_editor_enabled
    settings.report_editor_enabled = False
    try:
        agent = AnalyzerAgent(_StubLLM("edited [S1][S2][S3]"))
        out = agent._maybe_edit_report(_DRAFT, "q", "en", SearchDepth.HARD, None, None)
        assert out == _DRAFT and agent.llm.calls == 0
    finally:
        settings.report_editor_enabled = original


def test_editor_falls_back_on_exception():
    class _Boom(LLMProvider):
        def generate(self, system_prompt, user_prompt, **kwargs):
            raise RuntimeError("provider down")

    agent = AnalyzerAgent(_Boom())
    out = agent._maybe_edit_report(_DRAFT, "q", "en", SearchDepth.MEDIUM, None, None)
    assert out == _DRAFT  # never breaks finalization
