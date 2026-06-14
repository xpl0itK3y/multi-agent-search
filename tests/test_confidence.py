from src.agents.confidence import ConfidenceAgent
from src.api.schemas import (
    CitationAudit,
    CitationGround,
    ConfidenceFinding,
    OriginCluster,
    RedTeamFinding,
    RedTeamReport,
    SourceIndependence,
    VerificationReport,
)


def _verification(*findings: ConfidenceFinding) -> VerificationReport:
    return VerificationReport(research_id="r1", findings=list(findings), coverage_ratio=1.0)


def test_strong_signals_grade_high():
    verification = _verification(
        ConfidenceFinding(statement="Solar capacity grew sharply in 2024", support_level="strong", source_ids=["S1", "S2"]),
        ConfidenceFinding(statement="Battery costs fell year over year", support_level="strong", source_ids=["S3"]),
    )
    citations = CitationAudit(total=10, supported=10, integrity=1.0)
    red_team = RedTeamReport(held=4, challenged=0, findings=[RedTeamFinding(claim="x", verdict="holds")])
    independence = SourceIndependence(total_sources=6, independent_origins=6, independence_score=1.0)

    rep = ConfidenceAgent().compose(verification, citations, red_team, independence)
    assert rep.grade == "high"
    assert rep.overall >= 0.9
    assert rep.solid == 2 and rep.contested == 0 and rep.speculative == 0
    # all four components present and transparent
    assert {c.key for c in rep.components} == {"citations", "corroboration", "resilience", "independence"}
    assert abs(sum(c.weight for c in rep.components) - 1.0) < 1e-6


def test_red_team_refutation_pulls_claim_to_contested():
    verification = _verification(
        ConfidenceFinding(statement="The new drug cures the disease completely", support_level="strong", source_ids=["S1"]),
    )
    citations = CitationAudit(total=2, supported=2, integrity=1.0)
    red_team = RedTeamReport(
        held=0,
        challenged=1,
        findings=[RedTeamFinding(claim="claims the new drug cures disease completely", verdict="refuted")],
    )
    rep = ConfidenceAgent().compose(verification, citations, red_team, SourceIndependence())
    claim = rep.claims[0]
    assert claim.band == "contested" and claim.note == "red-team"
    assert rep.contested == 1 and rep.solid == 0


def test_single_origin_and_weak_grounding_downgrade():
    verification = _verification(
        ConfidenceFinding(statement="Claim backed only by an echo cluster source", support_level="strong", source_ids=["S1", "S2"]),
        ConfidenceFinding(statement="Claim whose citation does not match", support_level="medium", source_ids=["S3"]),
    )
    citations = CitationAudit(
        total=3, supported=1, integrity=0.33,
        grounding=[CitationGround(source_id="S3", supported=False)],
    )
    independence = SourceIndependence(
        total_sources=3, independent_origins=2, independence_score=0.66,
        clusters=[OriginCluster(label="wire.com", kind="syndicated", size=2, source_ids=["S1", "S2"])],
    )
    rep = ConfidenceAgent().compose(verification, citations, RedTeamReport(), independence)
    by_stmt = {c.statement: c for c in rep.claims}
    assert by_stmt["Claim backed only by an echo cluster source"].band == "contested"  # single origin
    assert by_stmt["Claim whose citation does not match"].band == "speculative"          # weak grounding


def test_empty_inputs_are_safe():
    rep = ConfidenceAgent().compose(VerificationReport(research_id="r"), CitationAudit(), RedTeamReport(), SourceIndependence())
    assert rep.total_claims == 0
    assert rep.overall == 0.0
    assert rep.components == []
