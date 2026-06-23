"""P3 verifier — deterministic (no-LLM) critique of a finalized report.

Three responsibilities, all derived from data already produced upstream:
  * per-claim confidence  — from the evidence groups' support levels;
  * plan-vs-report coverage — token overlap between each plan sub-question and
    the report body (same heuristic family as the eval harness);
  * an "open questions / unconfirmed" surface — uncovered sub-questions plus a
    pointer when claims were softened for weak support.

It also renders these as two markdown sections injected into the report, so the
transparency is visible without any frontend change.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from src.domain import (
    ClaimVerificationSummary,
    ConfidenceFinding,
    PlanCoverageItem,
    VerificationReport,
)

_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
_CITATION = re.compile(r"\[S\d+\]")
_SOURCES_HEADING = re.compile(r"(?im)^##\s+(Sources|Источники|Fuentes)\s*$")
_CONFIDENCE_HEADING = re.compile(
    r"(?im)^##\s+(Confidence in Key Findings|Уверенность в ключевых выводах|Confianza en los hallazgos clave)\s*$"
)

# Compact bilingual stopword set — keeps the coverage heuristic honest without a
# dependency. Mirrors the eval-harness / chat-escalation token filters.
_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "by",
    "is", "are", "was", "were", "be", "as", "at", "from", "that", "this", "it",
    "what", "which", "how", "why", "vs", "between", "about", "into", "their",
    "и", "в", "во", "не", "на", "с", "со", "что", "как", "для", "по", "из",
    "это", "к", "у", "о", "от", "за", "же", "или", "над", "под", "чем", "при",
}


def _tokens(text: str) -> set[str]:
    return {
        w
        for w in _WORD.findall((text or "").lower())
        if len(w) >= 4 and w not in _STOPWORDS
    }


class ReportCriticAgent:
    COVERAGE_THRESHOLD = 0.5
    MAX_FINDINGS = 6
    _LEVEL_ORDER = {"strong": 0, "medium": 1, "weak": 2}

    HEADINGS = {
        "confidence": {
            "ru": "## Уверенность в ключевых выводах",
            "en": "## Confidence in Key Findings",
            "es": "## Confianza en los hallazgos clave",
        },
        "open": {
            "ru": "## Открытые вопросы и неподтверждённое",
            "en": "## Open Questions & Unconfirmed",
            "es": "## Preguntas abiertas y sin confirmar",
        },
    }
    LEVEL_LABELS = {
        "ru": {"strong": "высокая", "medium": "средняя", "weak": "низкая"},
        "en": {"strong": "high", "medium": "medium", "weak": "low"},
        "es": {"strong": "alta", "medium": "media", "weak": "baja"},
    }
    OPEN_INTRO = {
        "ru": "Эти под-вопросы плана остались без явного покрытия в отчёте:",
        "en": "These plan sub-questions were not clearly addressed in the report:",
        "es": "Estas sub-preguntas del plan no quedaron claramente cubiertas en el informe:",
    }
    SOFTENED_NOTE = {
        "ru": "Часть утверждений смягчена из-за ограниченного подтверждения источниками.",
        "en": "Some statements were softened due to limited source support.",
        "es": "Algunas afirmaciones se suavizaron por respaldo limitado de las fuentes.",
    }

    # ── structured assessment ────────────────────────────────────────────────

    def _report_body(self, report: str) -> str:
        """Report text with the Sources section dropped (so URLs/domains don't
        create spurious coverage matches)."""
        match = _SOURCES_HEADING.search(report or "")
        return report[: match.start()] if match else (report or "")

    def assess_plan_coverage(
        self, tasks: Iterable[Any], report: str
    ) -> list[PlanCoverageItem]:
        body_tokens = _tokens(self._report_body(report))
        items: list[PlanCoverageItem] = []
        seen: set[str] = set()
        for task in tasks:
            question = (getattr(task, "description", None) or "").strip()
            if not question or question in seen:
                continue
            seen.add(question)
            needles = _tokens(question)
            if not needles:
                # Nothing meaningful to match — treat as covered to avoid noise.
                items.append(PlanCoverageItem(question=question, covered=True, match_ratio=1.0))
                continue
            ratio = len(needles & body_tokens) / len(needles)
            items.append(
                PlanCoverageItem(
                    question=question,
                    covered=ratio >= self.COVERAGE_THRESHOLD,
                    match_ratio=round(ratio, 3),
                )
            )
        return items

    def _statement_for(self, group: dict[str, Any]) -> str:
        for item in group.get("evidence") or []:
            sentence = (item.get("sentence") or "").strip()
            if len(sentence) >= 40:
                return sentence
        # Fall back to any sentence, then to the topic tokens.
        for item in group.get("evidence") or []:
            sentence = (item.get("sentence") or "").strip()
            if sentence:
                return sentence
        return (group.get("topic") or "").strip()

    def confidence_findings(
        self, evidence_groups: Iterable[dict[str, Any]], max_items: Optional[int] = None
    ) -> list[ConfidenceFinding]:
        findings: list[ConfidenceFinding] = []
        for group in evidence_groups or []:
            source_ids = [s for s in (group.get("source_ids") or []) if s]
            statement = self._statement_for(group)
            if not statement or not source_ids:
                continue
            support = group.get("support_level") or (
                "strong" if len(source_ids) >= 3 else "medium" if len(source_ids) == 2 else "weak"
            )
            findings.append(
                ConfidenceFinding(
                    statement=statement,
                    support_level=support,
                    source_count=len(source_ids),
                    source_ids=source_ids,
                )
            )
        findings.sort(key=lambda f: (self._LEVEL_ORDER.get(f.support_level, 3), -f.source_count))
        return findings[: (max_items or self.MAX_FINDINGS)]

    def build(
        self,
        research_id: str,
        tasks: Iterable[Any],
        evidence_groups: Iterable[dict[str, Any]],
        report: str,
        *,
        claim_summary: Optional[ClaimVerificationSummary] = None,
    ) -> VerificationReport:
        coverage = self.assess_plan_coverage(tasks, report)
        findings = self.confidence_findings(evidence_groups)
        uncovered = [item.question for item in coverage if not item.covered]
        ratio = (
            round(sum(1 for c in coverage if c.covered) / len(coverage), 3) if coverage else 0.0
        )
        return VerificationReport(
            research_id=research_id,
            findings=findings,
            plan_coverage=coverage,
            uncovered_questions=uncovered,
            coverage_ratio=ratio,
            claim_verification=claim_summary or ClaimVerificationSummary(),
        )

    # ── rendering / injection ────────────────────────────────────────────────

    def render_sections(self, report_obj: VerificationReport, language: str) -> str:
        lang = language if language in self.HEADINGS["confidence"] else "en"
        labels = self.LEVEL_LABELS[lang]
        blocks: list[str] = []

        if report_obj.findings:
            lines = [self.HEADINGS["confidence"][lang]]
            for finding in report_obj.findings:
                cites = "".join(f"[{sid}]" for sid in finding.source_ids)
                label = labels.get(finding.support_level, finding.support_level)
                lines.append(f"- _({label})_ {finding.statement} {cites}".rstrip())
            blocks.append("\n".join(lines))

        softened = bool(
            report_obj.claim_verification.unsupported_lines
            or report_obj.claim_verification.insufficient_evidence_lines
        )
        if report_obj.uncovered_questions or softened:
            lines = [self.HEADINGS["open"][lang]]
            if report_obj.uncovered_questions:
                lines.append(self.OPEN_INTRO[lang])
                lines.extend(f"- {q}" for q in report_obj.uncovered_questions)
            if softened:
                lines.append(self.SOFTENED_NOTE[lang])
            blocks.append("\n".join(lines))

        return "\n\n".join(blocks)

    def inject(self, report: str, report_obj: VerificationReport, language: str) -> str:
        """Insert the confidence + open-questions sections before the Sources list."""
        if not report or _CONFIDENCE_HEADING.search(report):
            return report
        block = self.render_sections(report_obj, language)
        if not block:
            return report
        match = _SOURCES_HEADING.search(report)
        if match:
            head = report[: match.start()].rstrip()
            tail = report[match.start():].lstrip()
            return f"{head}\n\n{block}\n\n{tail}"
        return f"{report.rstrip()}\n\n{block}"
