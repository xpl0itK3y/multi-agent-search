"""SEC-010: every prompt that ingests untrusted scraped content must instruct the model to
treat that content as data, not instructions — a regression guard against prompt injection.

"Scraped content" includes text derived from it: a report quotes its sources, so the prompts
that take a report (editor, translator, comparison, red-team extraction) are covered too.
Prompts that only see the user's own question (plan, clarifier, orchestrator) are not."""
import pytest

from src.agents import comparison, cross_language, red_team, stance
from src.agents.analyzer import AnalyzerAgent
from src.agents.chat import ChatAgent


def _hardened(prompt: str) -> bool:
    low = prompt.lower()
    return "untrusted" in low and "instruction" in low


def test_analyzer_prompts_defend_against_injection():
    assert _hardened(AnalyzerAgent.SYSTEM_PROMPT)
    assert _hardened(AnalyzerAgent.SECTION_SYSTEM_PROMPT)
    assert _hardened(AnalyzerAgent.SYNTHESIS_SYSTEM_PROMPT)


def test_chat_prompt_defends_against_injection():
    assert _hardened(ChatAgent.SYSTEM_PROMPT)


@pytest.mark.parametrize(
    "name, prompt",
    [
        ("analyzer conflict adjudication", AnalyzerAgent.CONFLICT_ADJUDICATION_SYSTEM_PROMPT),
        ("analyzer editor", AnalyzerAgent.EDITOR_SYSTEM_PROMPT),
        ("analyzer translator", AnalyzerAgent.TRANSLATOR_SYSTEM_PROMPT.format(language="Russian")),
        ("red-team claim extraction", red_team._EXTRACT_SYSTEM),
        ("red-team judge", red_team._JUDGE_SYSTEM),
        ("stance", stance._SYSTEM),
        ("comparison", comparison._SYSTEM),
        ("cross-language surface", cross_language._SURFACE_SYSTEM.replace("{base}", "ru")),
    ],
)
def test_every_prompt_that_reads_sources_or_reports_defends_against_injection(name, prompt):
    assert _hardened(prompt), f"the {name} prompt takes untrusted text but does not say so"
