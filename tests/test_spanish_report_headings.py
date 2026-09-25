"""C7-8: a Spanish report carries Spanish structural headings throughout, not the English
fallback next to the localised conflicts and execution-trail sections."""
import pytest

from src.agents.analyzer import AnalyzerAgent
from src.core.llm import LLMProvider
from src.domain import RedTeamFinding, RedTeamReport
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _NoopLLM(LLMProvider):
    def generate(self, system_prompt, user_prompt, **kwargs):
        return ""


def test_red_team_section_is_written_in_spanish():
    service = ResearchService(task_store=InMemoryTaskStore())
    report = RedTeamReport(
        findings=[
            RedTeamFinding(claim="A", verdict="refuted", challenge="x"),
            RedTeamFinding(claim="B", verdict="contested"),
            RedTeamFinding(claim="C", verdict="qualified"),
            RedTeamFinding(claim="D", verdict="holds"),
        ]
    )

    section = service._render_red_team_section(report, "es")

    lines = section.splitlines()
    assert lines[0] == "## Debilidades y contraargumentos"
    assert lines[2] == "Las afirmaciones clave del informe se contrastaron con evidencia en contra."
    assert "- **Refutada** — A" in lines
    assert "- **Cuestionada** — B" in lines
    assert "- **Con matices** — C" in lines
    assert "- **Se sostiene** — D" in lines
    assert "Weaknesses" not in section


@pytest.mark.parametrize("language", ["ru", "en", "es"])
def test_every_structural_heading_has_a_table_entry_for_the_report_languages(language):
    agent = AnalyzerAgent(_NoopLLM())
    headings = [
        agent._sources_heading(language),
        agent._used_sources_heading(language),
        agent._additional_sources_heading(language),
        agent._report_notes_heading(language),
        agent._conflicts_heading(language),
    ]
    if language != "en":
        english = [
            agent._sources_heading("en"),
            agent._used_sources_heading("en"),
            agent._additional_sources_heading("en"),
            agent._report_notes_heading("en"),
            agent._conflicts_heading("en"),
        ]
        assert not set(headings) & set(english)
    # Each heading the analyzer writes is one its own patterns recognise again.
    assert agent.SOURCE_HEADING_LINE_PATTERN.fullmatch(agent._sources_heading(language))
    assert agent.SOURCE_HEADING_PATTERN.search("\n" + agent._sources_heading(language) + "\n- x")
    assert agent.REPORT_NOTES_HEADING_PATTERN.fullmatch(agent._report_notes_heading(language))
    assert agent.CONFLICT_HEADING_PATTERN.fullmatch(agent._conflicts_heading(language))


def test_other_languages_fall_back_to_english_headings():
    agent = AnalyzerAgent(_NoopLLM())
    assert agent._sources_heading("de") == "## Sources"
    assert agent._used_sources_heading("unknown") == "### Used Sources"
    assert agent._report_notes_heading("zh") == "## Report Notes"
    assert ResearchService(task_store=InMemoryTaskStore())._render_red_team_section(
        RedTeamReport(findings=[RedTeamFinding(claim="A", verdict="holds")]), "de"
    ).startswith("## Weaknesses & counter-arguments")
