from src.agents.analyzer import AnalyzerAgent
from src.core.llm import LLMProvider


class _NoopLLM(LLMProvider):
    def generate(self, system_prompt, user_prompt, **kwargs):
        return ""


def _agent():
    return AnalyzerAgent(_NoopLLM())


def test_clean_table_rows_strips_leaked_prefix():
    agent = _agent()
    text = "Intro line.\nBased on the available sources, | Area | PG | CH |\n| A | 1 | 2 |"
    out = agent._clean_table_rows(text)
    assert "Based on the available sources, |" not in out
    assert "| Area | PG | CH |" in out  # the row survives, cleaned
    assert "| A | 1 | 2 |" in out       # an already-clean row is untouched


def test_clean_table_rows_leaves_legit_content():
    agent = _agent()
    # one pipe + no trailing comma clause → not a polluted table row
    assert agent._clean_table_rows("Use a | b shell pipe here") == "Use a | b shell pipe here"
    # ordinary prose with a comma but no table
    assert agent._clean_table_rows("This holds, in general.") == "This holds, in general."
    # a real header separator is untouched
    assert agent._clean_table_rows("|---|---|") == "|---|---|"


def test_intro_conclusion_headings_recognize_new_structure():
    agent = _agent()
    assert agent.INTRODUCTION_HEADING_PATTERN.search("## Executive Summary\nbody")
    assert agent.INTRODUCTION_HEADING_PATTERN.search("## Краткое резюме\nтело")
    assert agent.INTRODUCTION_HEADING_PATTERN.search("## Introduction\nbody")
    assert agent.CONCLUSION_HEADING_PATTERN.search("## Conclusion / Bottom Line\nbody")
    assert agent.CONCLUSION_HEADING_PATTERN.search("## Итог\nтело")
    assert agent.CONCLUSION_HEADING_PATTERN.search("## Conclusion\nbody")


def test_no_false_missing_heading_notes_for_new_structure():
    agent = _agent()
    report = (
        "## Executive Summary\nClickHouse wins for OLAP [S1].\n\n"
        "## Analysis\nColumnar storage helps [S1][S2].\n\n"
        "## Conclusion / Bottom Line\nUse ClickHouse for scale [S2].\n\n"
        "## Sources\n- [S1] a\n- [S2] b"
    )
    sources = [{"source_id": "S1", "content": "clickhouse olap"}, {"source_id": "S2", "content": "columnar storage scale"}]
    notes = agent._report_quality_notes(report, sources, "en")
    messages = agent._quality_note_messages("en")
    assert messages["missing_intro"] not in notes
    assert messages["missing_conclusion"] not in notes


def test_conflicts_are_inserted_before_extended_conclusion_heading():
    agent = _agent()
    report = "## Analysis\nEvidence differs.\n\n## Conclusion / Bottom Line\nChoose carefully."
    conflicts = [{
        "topic": "cost",
        "reason": "different figures",
        "source_ids": ["S1", "S2"],
        "sentences": ["The total cost was 10 in 2024.", "The total cost was 20 in 2024."],
    }]

    result = agent._inject_conflicts_section(report, conflicts, "en")

    assert result.index("## Conflicts And Uncertainties") < result.index("## Conclusion / Bottom Line")
