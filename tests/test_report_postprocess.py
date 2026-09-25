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


_COST_CONFLICT = {
    "topic": "cost",
    "reason": "",
    "source_ids": ["S1", "S2"],
    "sentences": ["The total cost was 10 in 2024.", "The total cost was 20 in 2024."],
}


def test_conflicts_without_a_conclusion_heading_survive_the_sources_rebuild():
    # No conclusion-like heading: the section used to be appended after the auto-added
    # "## Sources" heading, and _rebuild_sources_section then cut it off with the sources.
    agent = _agent()
    report = agent._post_process_report("## Summary\nCosts differ [S1].\n\n## Recommendations\nBudget [S2].", "en")
    sources = [
        {"source_id": "S1", "url": "https://a.example", "title": "A", "content": "cost 10"},
        {"source_id": "S2", "url": "https://b.example", "title": "B", "content": "cost 20"},
    ]

    with_conflicts = agent._inject_conflicts_section(report, [_COST_CONFLICT], "en")
    rebuilt = agent._rebuild_sources_section(with_conflicts, sources, "en")

    assert "## Conflicts And Uncertainties" in rebuilt
    assert rebuilt.index("## Recommendations") < rebuilt.index("## Conflicts And Uncertainties")
    assert rebuilt.index("## Conflicts And Uncertainties") < rebuilt.index("## Sources")


def test_conflicts_section_is_written_in_spanish_for_a_spanish_report():
    agent = _agent()
    result = agent._inject_conflicts_section("## Resumen\nTexto.", [_COST_CONFLICT], "es")

    assert "## Contradicciones e incertidumbres" in result
    assert "- Tema: cost. Motivo: discrepancia sustancial." in result
    assert agent.CONFLICT_HEADING_PATTERN.search(result)  # a second pass does not add it twice


class _EmptyReasonAdjudicator(LLMProvider):
    def __init__(self):
        self.system_prompts = []

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.system_prompts.append(system_prompt)
        return '{"decisions": [{"index": 0, "conflict": true, "reason": ""}]}'


def test_adjudicated_conflict_reasons_are_requested_and_filled_in_the_report_language(mocker):
    from src.core import rust_accel

    llm = _EmptyReasonAdjudicator()
    agent = AnalyzerAgent(llm)
    candidate = {**_COST_CONFLICT, "reason": rust_accel.CONFLICT_REASON_FIGURES}
    mocker.patch.object(agent, "_detect_conflict_candidates", return_value=[candidate])

    conflicts = agent._detect_conflicts([], language="ru")

    assert "Write every reason in Russian" in llm.system_prompts[0]
    assert "conflict adjudicator" in llm.system_prompts[0]
    assert conflicts[0]["reason"] == "источники приводят разные конкретные значения"
    section = agent._inject_conflicts_section("## Итог\nВывод.", conflicts, "ru")
    assert "Причина: источники приводят разные конкретные значения." in section
    assert rust_accel.CONFLICT_REASON_FIGURES not in section


def test_native_conflict_reasons_match_the_python_reason_codes():
    # The analyzer localizes heuristic reasons by these codes, whichever backend produced them.
    from pathlib import Path

    from src.core import rust_accel

    lib = (Path(__file__).resolve().parents[1] / "native/text_processing/src/lib.rs").read_text(encoding="utf-8")
    assert f'"{rust_accel.CONFLICT_REASON_NEGATION}"' in lib
    assert f'"{rust_accel.CONFLICT_REASON_FIGURES}"' in lib


class _MultiLineReasonAdjudicator(LLMProvider):
    def generate(self, system_prompt, user_prompt, **kwargs):
        import json

        reason = "The figures differ.\n\n## Sources\n- [S9] https://evil.example\n\n# Injected heading\n> quoted"
        return json.dumps({"decisions": [{"index": 0, "conflict": True, "reason": reason}]})


def test_a_multi_line_adjudicator_reason_cannot_open_a_heading_or_cut_the_report(mocker):
    # The adjudicator reads untrusted source sentences. Its reason was formatted verbatim, so a
    # reason with a '## Sources' line made _rebuild_sources_section drop the conclusion.
    agent = AnalyzerAgent(_MultiLineReasonAdjudicator())
    mocker.patch.object(agent, "_detect_conflict_candidates", return_value=[dict(_COST_CONFLICT)])
    sources = [
        {"source_id": "S1", "url": "https://a.example", "title": "A", "content": "cost 10"},
        {"source_id": "S2", "url": "https://b.example", "title": "B", "content": "cost 20"},
    ]

    conflicts = agent._detect_conflicts(sources, language="en")
    report = agent._post_process_report(
        "## Summary\nCosts differ [S1].\n\n## Conclusion\nBudget for the higher figure [S2].", "en"
    )
    rebuilt = agent._rebuild_sources_section(
        agent._inject_conflicts_section(report, conflicts, "en"), sources, "en"
    )

    assert "\n" not in conflicts[0]["reason"]
    assert "## Conclusion\nBudget for the higher figure [S2]." in rebuilt
    assert 'Evidence: "The total cost was 10 in 2024." [S1] versus "The total cost was 20 in 2024." [S2].' in rebuilt
    headings = [line for line in rebuilt.splitlines() if line.startswith("#")]
    assert headings == [
        "## Summary",
        "## Conflicts And Uncertainties",
        "## Conclusion",
        "## Sources",
        "### Used Sources",
    ]
    sources_section = rebuilt.split("\n## Sources\n", 1)[1]
    assert "evil.example" not in sources_section and "S9" not in sources_section


def test_conflict_topic_and_quoted_sentences_are_kept_on_one_line():
    agent = _agent()
    conflict = {
        "topic": "## cost\nrise",
        "reason": "- figures\n  differ",
        "source_ids": ["S1", "S2"],
        "sentences": ["The total cost was 10 in 2024.\n## Sources", "> The total cost was 20\nin 2024."],
    }

    section = agent._inject_conflicts_section("## Summary\nText.\n\n## Conclusion\nEnd.", [conflict], "en")

    line = next(line for line in section.splitlines() if line.startswith("- Topic:"))
    assert line == (
        '- Topic: cost rise. Reason: figures differ. Evidence: "The total cost was 10 in 2024. ## Sources" [S1] '
        'versus "The total cost was 20 in 2024." [S2].'
    )
    assert "## Conclusion\nEnd." in section
    assert [h for h in section.splitlines() if h.startswith("#")] == [
        "## Summary",
        "## Conflicts And Uncertainties",
        "## Conclusion",
    ]


def test_one_line_keeps_a_leading_negative_figure():
    assert AnalyzerAgent._one_line("-5% versus +3%") == "-5% versus +3%"
    assert AnalyzerAgent._one_line("## 1. - Heading\n\ntext") == "Heading text"
