"""Unit tests for the P3 verifier (ReportCriticAgent) — deterministic, no LLM."""

from __future__ import annotations

from src.agents.report_critic import ReportCriticAgent
from src.api.schemas import ClaimVerificationSummary


class _Task:
    def __init__(self, description: str):
        self.description = description


REPORT = """# Phones

## Camera
The camera comparison shows the iPhone leads in dynamic range [S1][S2].

## Battery
Battery autonomy endurance is strongest on the Galaxy flagship [S3].

## Conclusion
Solid options exist across the lineup [S1].

## Sources
[S1] http://a.com
[S2] http://b.com
[S3] http://c.com
"""

GROUPS = [
    {
        "topic": "camera, iphone, dynamic",
        "source_ids": ["S1", "S2", "S3"],
        "support_level": "strong",
        "evidence": [{"source_id": "S1", "sentence": "The iPhone camera leads in dynamic range across independent lab tests."}],
    },
    {
        "topic": "battery, galaxy, endurance",
        "source_ids": ["S3"],
        "support_level": "weak",
        "evidence": [{"source_id": "S3", "sentence": "Galaxy battery endurance exceeds fourteen hours in mixed-use testing."}],
    },
]


def test_plan_coverage_marks_covered_and_uncovered():
    rc = ReportCriticAgent()
    tasks = [
        _Task("Camera comparison across flagship phones"),
        _Task("Battery autonomy endurance"),
        _Task("Quantum teleportation pricing models"),
    ]
    coverage = rc.assess_plan_coverage(tasks, REPORT)
    by_q = {c.question: c for c in coverage}
    assert by_q["Camera comparison across flagship phones"].covered is True
    assert by_q["Battery autonomy endurance"].covered is True
    assert by_q["Quantum teleportation pricing models"].covered is False


def test_plan_coverage_excludes_sources_section():
    # A task whose keywords only appear inside the Sources URLs must not count as covered.
    rc = ReportCriticAgent()
    coverage = rc.assess_plan_coverage([_Task("information about example domains")], REPORT)
    assert coverage[0].covered is False


def test_plan_coverage_dedupes_and_handles_empty():
    rc = ReportCriticAgent()
    coverage = rc.assess_plan_coverage([_Task("Camera"), _Task("Camera"), _Task("the of in")], REPORT)
    # "Camera" appears once; the stopword-only description is treated as covered.
    questions = [c.question for c in coverage]
    assert questions.count("Camera") == 1
    assert any(c.question == "the of in" and c.covered for c in coverage)


def test_confidence_findings_sorted_and_capped():
    rc = ReportCriticAgent()
    findings = rc.confidence_findings(GROUPS)
    assert [f.support_level for f in findings] == ["strong", "weak"]
    assert findings[0].source_count == 3
    assert findings[0].source_ids == ["S1", "S2", "S3"]


def test_confidence_findings_skip_groups_without_statement_or_sources():
    rc = ReportCriticAgent()
    groups = [
        {"source_ids": [], "support_level": "strong", "evidence": [{"sentence": "x"}]},  # no sources
        {"source_ids": ["S1"], "support_level": "weak", "evidence": []},                   # no statement
    ]
    assert rc.confidence_findings(groups) == []


def test_confidence_findings_infers_support_level_when_missing():
    rc = ReportCriticAgent()
    groups = [{"source_ids": ["S1", "S2"], "evidence": [{"sentence": "A corroborated claim with two sources here."}]}]
    findings = rc.confidence_findings(groups)
    assert findings[0].support_level == "medium"


def test_build_reports_coverage_ratio_and_uncovered():
    rc = ReportCriticAgent()
    tasks = [_Task("Camera comparison flagship"), _Task("Quantum teleportation pricing")]
    report = rc.build("rid", tasks, GROUPS, REPORT, claim_summary=ClaimVerificationSummary())
    assert report.research_id == "rid"
    assert report.coverage_ratio == 0.5
    assert report.uncovered_questions == ["Quantum teleportation pricing"]
    assert len(report.findings) == 2


def test_render_sections_includes_findings_and_open_questions():
    rc = ReportCriticAgent()
    report = rc.build(
        "rid",
        [_Task("Quantum teleportation pricing")],
        GROUPS,
        REPORT,
        claim_summary=ClaimVerificationSummary(unsupported_lines=2),
    )
    block = rc.render_sections(report, "en")
    assert "## Confidence in Key Findings" in block
    assert "_(high)_" in block and "[S1][S2][S3]" in block
    assert "## Open Questions & Unconfirmed" in block
    assert "Quantum teleportation pricing" in block
    assert "softened" in block.lower()


def test_render_sections_localized_headings():
    rc = ReportCriticAgent()
    report = rc.build("rid", [], GROUPS, REPORT, claim_summary=ClaimVerificationSummary())
    assert "## Уверенность в ключевых выводах" in rc.render_sections(report, "ru")
    assert "## Confianza en los hallazgos clave" in rc.render_sections(report, "es")


def test_render_sections_empty_when_clean():
    rc = ReportCriticAgent()
    # No findings, full coverage, no softened claims -> nothing to render.
    report = rc.build("rid", [_Task("Camera comparison flagship")], [], REPORT, claim_summary=ClaimVerificationSummary())
    assert rc.render_sections(report, "en") == ""


def test_inject_places_block_before_sources_and_is_idempotent():
    rc = ReportCriticAgent()
    report = rc.build("rid", [_Task("Quantum teleportation pricing")], GROUPS, REPORT, claim_summary=ClaimVerificationSummary())
    injected = rc.inject(REPORT, report, "en")
    assert injected.index("## Confidence in Key Findings") < injected.index("## Sources")
    assert injected.index("## Conclusion") < injected.index("## Confidence in Key Findings")
    # Re-injecting must not duplicate the section.
    twice = rc.inject(injected, report, "en")
    assert twice.count("## Confidence in Key Findings") == 1
