"""language-fix: the HARD/parallel writer path enforces the report language.

- the section/synthesis prompts demand the target language forcefully at the START and re-assert
  it AFTER the (often-English) source material, where recency otherwise causes drift;
- _enforce_report_language rewrites a drifted report exactly once, guarded by citation
  preservation + a re-detection, and falls back to the original on any failure."""
from src.agents.analyzer import AnalyzerAgent

AZ = AnalyzerAgent.__new__(AnalyzerAgent)


def test_structural_headings_localized_to_report_language():
    md = "## Executive Summary\n\nТекст.\n\n## Время когерентности\n\n## Conclusion"
    out = AZ._localize_structural_headings(md, "ru")
    assert "## Исполнительное резюме" in out  # English structural heading translated
    assert "## Заключение" in out
    assert "## Время когерентности" in out  # an unknown (already-RU) heading is left as-is
    assert AZ._localize_structural_headings("# Executive Summary", "es") == "# Resumen ejecutivo"
    assert AZ._localize_structural_headings("## Executive Summary", "en") == "## Executive Summary"


def test_language_instruction_is_forceful():
    instr = AZ._language_instruction("ru")
    assert "Russian" in instr and "CRITICAL" in instr


def test_synthesis_prompt_brackets_drafts_with_language():
    p = AZ._build_synthesis_user_prompt("вопрос", ["English draft text [S1]"], "ru", None)
    assert p.lstrip().startswith("CRITICAL")            # forceful at the start
    assert "FINAL REMINDER" in p and "Russian" in p     # re-asserted at the end
    assert p.index("FINAL REMINDER") > p.index("English draft text")  # reminder AFTER the drafts


def test_section_prompt_reasserts_language_after_sources():
    p = AZ._build_section_user_prompt(
        [{"source_id": "S1", "content": "english source"}], "вопрос", "ru", None
    )
    assert "FINAL REMINDER" in p and p.rstrip().endswith("Russian.")


class _StubLLM:
    def __init__(self, out):
        self.out, self.calls = out, 0

    def generate(self, **kw):
        self.calls += 1
        return self.out


def _analyzer_with(out):
    az = AnalyzerAgent.__new__(AnalyzerAgent)
    az.llm = _StubLLM(out)
    return az


def test_no_rewrite_when_already_target_language():
    az = _analyzer_with("SHOULD NOT BE USED")
    report = "Это полностью русский отчёт по теме исследования. Подробности здесь [S1]."
    assert az._enforce_report_language(report, "ru", model=None) == report
    assert az.llm.calls == 0


def test_rewrites_when_drifted_to_english():
    ru = "Это русский отчёт об OpenAI [S1] с важными деталями и выводами по данной теме."
    az = _analyzer_with(ru)
    en = "This is an English report about OpenAI [S1] with key details and conclusions on the topic."
    out = az._enforce_report_language(en, "ru", model=None)
    assert az.llm.calls == 1 and out == ru


def test_falls_back_if_rewrite_drops_citations():
    az = _analyzer_with("Это русский текст без единой ссылки на источники, поэтому он отбракован.")
    en = "English report [S1] [S2] [S3] with three citations that must be preserved."
    assert az._enforce_report_language(en, "ru", model=None) == en  # dropped [Sn] -> fallback
