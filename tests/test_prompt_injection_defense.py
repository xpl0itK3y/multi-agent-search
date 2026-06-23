"""SEC-010: every prompt that ingests untrusted scraped content must instruct the model to
treat that content as data, not instructions — a regression guard against prompt injection."""
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
