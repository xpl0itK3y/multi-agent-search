"""Confidence / honesty meter (deterministic, no LLM).

Perplexity and Gemini present every sentence with the same flat confidence. This agent
fuses the four trust signals the pipeline already produces into ONE calibrated number,
and — crucially — lists the components that feed it, so the score is transparent:

  * citation grounding      (CitationAudit.integrity)      — do sources back the claims?
  * claim corroboration     (VerificationReport.findings)  — how strong is the support?
  * adversarial resilience  (RedTeamReport held/challenged) — does it survive attack?
  * source independence     (SourceIndependence.score)     — independent origins, not echoes?

Per load-bearing finding it also assigns a band (solid / contested / speculative) by
letting a red-team refutation, weak grounding, or single-origin sourcing pull a claim
down — producing the "X% solid / Y% contested / Z% speculative" headline.
"""
from __future__ import annotations

from src.domain import (
    CitationAudit,
    ConfidenceClaim,
    ConfidenceComponent,
    ConfidenceReport,
    RedTeamReport,
    SourceIndependence,
    VerificationReport,
)

# Map a verification support level to a 0..1 corroboration weight.
_LEVEL_SCORE = {"strong": 1.0, "medium": 0.6, "weak": 0.25}
# Component weights (normalized over the components actually present).
_WEIGHTS = {"citations": 0.30, "corroboration": 0.30, "resilience": 0.25, "independence": 0.15}
_RT_MATCH = 0.34  # share of a finding's tokens that must appear in a red-team claim to link them

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "into", "than", "then",
    "have", "has", "был", "была", "было", "были", "это", "как", "что", "для", "при",
    "также", "более", "может", "быть", "если", "или", "так", "его", "their", "these",
    "those", "which", "while", "about", "would", "could", "should", "там",
}


class ConfidenceAgent:
    def compose(
        self,
        verification: VerificationReport,
        citations: CitationAudit,
        red_team: RedTeamReport,
        independence: SourceIndependence,
    ) -> ConfidenceReport:
        components = self._components(verification, citations, red_team, independence)
        overall = self._blend(components)
        claims = self._classify_claims(verification, citations, red_team, independence)

        solid = sum(1 for c in claims if c.band == "solid")
        contested = sum(1 for c in claims if c.band == "contested")
        speculative = sum(1 for c in claims if c.band == "speculative")

        return ConfidenceReport(
            overall=round(overall, 4),
            grade="high" if overall >= 0.75 else "medium" if overall >= 0.5 else "low",
            total_claims=len(claims),
            solid=solid,
            contested=contested,
            speculative=speculative,
            components=components,
            claims=claims,
        )

    # ── component scores (transparent inputs) ───────────────────────────────────

    def _components(
        self,
        verification: VerificationReport,
        citations: CitationAudit,
        red_team: RedTeamReport,
        independence: SourceIndependence,
    ) -> list[ConfidenceComponent]:
        out: list[ConfidenceComponent] = []

        if citations and citations.total:
            out.append(
                ConfidenceComponent(
                    key="citations",
                    score=round(citations.integrity, 4),
                    weight=_WEIGHTS["citations"],
                    detail=f"{citations.supported}/{citations.total} matched source",
                )
            )

        findings = verification.findings if verification else []
        if findings:
            corro = sum(_LEVEL_SCORE.get(f.support_level, 0.25) for f in findings) / len(findings)
            strong = sum(1 for f in findings if f.support_level == "strong")
            out.append(
                ConfidenceComponent(
                    key="corroboration",
                    score=round(corro, 4),
                    weight=_WEIGHTS["corroboration"],
                    detail=f"{strong}/{len(findings)} strongly corroborated",
                )
            )

        if red_team and (red_team.held or red_team.challenged):
            denom = red_team.held + red_team.challenged
            resilience = red_team.held / denom if denom else 1.0
            out.append(
                ConfidenceComponent(
                    key="resilience",
                    score=round(resilience, 4),
                    weight=_WEIGHTS["resilience"],
                    detail=f"{red_team.held}/{denom} held under red-team",
                )
            )

        if independence and independence.total_sources > 1:
            out.append(
                ConfidenceComponent(
                    key="independence",
                    score=round(independence.independence_score, 4),
                    weight=_WEIGHTS["independence"],
                    detail=f"{independence.independent_origins}/{independence.total_sources} independent origins",
                )
            )

        return out

    @staticmethod
    def _blend(components: list[ConfidenceComponent]) -> float:
        if not components:
            return 0.0
        total_w = sum(c.weight for c in components) or 1.0
        return sum(c.score * c.weight for c in components) / total_w

    # ── per-claim band fusion ───────────────────────────────────────────────────

    def _classify_claims(
        self,
        verification: VerificationReport,
        citations: CitationAudit,
        red_team: RedTeamReport,
        independence: SourceIndependence,
    ) -> list[ConfidenceClaim]:
        findings = verification.findings if verification else []
        if not findings:
            return []

        unsupported_ids = {g.source_id for g in (citations.grounding if citations else []) if not g.supported}
        # source_ids that sit inside a multi-source non-independent origin (echo cluster)
        echo_ids: set[str] = set()
        for cluster in (independence.clusters if independence else []):
            if cluster.size > 1:
                echo_ids.update(cluster.source_ids)
        rt_findings = list(red_team.findings) if red_team else []

        claims: list[ConfidenceClaim] = []
        for f in findings:
            band = "solid" if f.support_level in ("strong", "medium") else "speculative"
            note = ""
            sids = list(f.source_ids)

            # 1) red-team is the strongest signal — a refuted/contested claim is contested.
            verdict = self._red_team_verdict(f.statement, rt_findings)
            if verdict in ("refuted", "contested"):
                band, note = "contested", "red-team"
            elif verdict == "qualified" and band == "solid":
                band, note = "contested", "qualified"

            # 2) weak grounding: the finding's own citations don't back it.
            if band == "solid" and sids and all(sid in unsupported_ids for sid in sids):
                band, note = "speculative", "weak grounding"

            # 3) single-origin: every cited source traces to one non-independent origin.
            if band == "solid" and sids and all(sid in echo_ids for sid in sids):
                band, note = "contested", "single origin"

            claims.append(
                ConfidenceClaim(
                    statement=f.statement,
                    band=band,
                    support_level=f.support_level,
                    source_ids=sids,
                    note=note,
                )
            )
        return claims

    def _red_team_verdict(self, statement: str, rt_findings: list) -> str:
        """Strongest adverse verdict among red-team claims that lexically match the finding."""
        tokens = self._tokens(statement)
        if not tokens:
            return ""
        rank = {"refuted": 3, "contested": 2, "qualified": 1, "holds": 0, "": 0}
        best, best_rank = "", 0
        for rf in rt_findings:
            overlap = len(tokens & self._tokens(rf.claim)) / len(tokens)
            if overlap >= _RT_MATCH and rank.get(rf.verdict, 0) > best_rank:
                best, best_rank = rf.verdict, rank.get(rf.verdict, 0)
        return best

    @staticmethod
    def _tokens(text: str) -> set[str]:
        out: set[str] = set()
        word = ""
        for ch in (text or "").lower():
            if ch.isalnum():
                word += ch
            else:
                if len(word) >= 3 and word not in _STOPWORDS:
                    out.add(word)
                word = ""
        if len(word) >= 3 and word not in _STOPWORDS:
            out.add(word)
        return out
