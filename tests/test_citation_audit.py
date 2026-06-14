from src.agents.citation_audit import CitationAuditAgent


def test_citation_audit_flags_matching_and_mismatched():
    report = (
        "Quantum computers use qubits and superposition to process information [S1]. "
        "The economy of Brazil grew rapidly last year [S2]."
    )
    sources = {
        "S1": {
            "content": "Qubits exploit superposition to represent states; quantum computers process information this way.",
            "url": "https://a.com",
            "title": "Qubits",
        },
        "S2": {"content": "A recipe for chocolate cake with flour and sugar.", "url": "https://b.com", "title": "Cake"},
    }
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 2
    assert audit.supported == 1
    assert 0.4 < audit.integrity < 0.6
    assert any("Brazil" in c or "economy" in c for c in audit.unsupported_claims)
    g1 = next(g for g in audit.grounding if g.source_id == "S1")
    assert g1.supported and "superposition" in g1.quote.lower()


def test_citation_audit_empty_inputs():
    assert CitationAuditAgent().audit("", {}).total == 0
    assert CitationAuditAgent().audit("No citations here at all.", {"S1": {"content": "x"}}).total == 0


def test_citation_audit_skips_too_short_claims():
    audit = CitationAuditAgent().audit("Yes [S1].", {"S1": {"content": "no match here whatsoever"}})
    assert audit.total == 0


def test_citation_audit_unknown_source_id_ignored():
    audit = CitationAuditAgent().audit(
        "This claim cites a source that does not exist in the pool [S9].",
        {"S1": {"content": "unrelated content about something"}},
    )
    assert audit.total == 0  # S9 not in pool -> nothing to check


def test_citation_audit_credits_pooled_synthesis():
    # No single source covers the whole synthesis bullet, but together they do.
    report = "Fasting improves insulin sensitivity, reduces arterial pressure, and preserves muscle mass [S1][S2][S3]."
    sources = {
        "S1": {"content": "Studies reported better insulin outcomes overall."},
        "S2": {"content": "Pressure readings fell during the period."},
        "S3": {"content": "Muscle was largely retained throughout."},
    }
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 1 and audit.supported == 1  # one claim, grounded by the pooled sources


def test_citation_audit_still_flags_fabrication_across_all_sources():
    # None of the cited sources mention the claim's terms -> still unsupported.
    report = "The Roman Empire collapsed because of dietary fasting trends [S1][S2]."
    sources = {"S1": {"content": "A guide to baking sourdough bread."}, "S2": {"content": "Tips for indoor gardening."}}
    audit = CitationAuditAgent().audit(report, sources)
    assert audit.total == 1 and audit.supported == 0
