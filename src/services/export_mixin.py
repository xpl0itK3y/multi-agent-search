"""Report export concern of ResearchService (PDF / DOCX / HTML / JSON / scorecard), extracted
as a mixin (AUD-030). Composed into ResearchService; the export builders call the
self.get_research_* trust getters and self._detect_report_language, which remain on
ResearchService, via normal composition.
"""
import json

from src.domain.errors import ConflictError, NotFoundError, UnprocessableError

from src.domain import *  # noqa: F401,F403


class ExportMixin:
    def _export_filename(self, title: str, ext: str) -> str:
        base = "".join(c if (c.isalnum() or c in "-_") else "-" for c in (title or "").strip())
        base = "-".join(filter(None, base.split("-")))[:60] or "research"
        return f"{base}.{ext}"

    def export_research_report(
        self, research_id: str, fmt: str,
        theme: str | None = None, accent: str | None = None, base: str | None = None,
    ) -> tuple[bytes, str, str]:
        """Render the final report to PDF/DOCX/HTML bytes. Returns (data, media_type, filename)."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if not research.final_report:
            raise ConflictError("Report is not ready yet")

        from src.ui.report_export import generate_docx, generate_pdf
        from src.ui.report_utils import clean_report

        # Strip the internal "Report Notes" section once for every human-facing format
        # (generate_pdf/docx also clean internally — calling it here is idempotent).
        report = clean_report(research.final_report)
        depth = getattr(research.depth, "value", str(research.depth))
        created_at = research.created_at.isoformat() if research.created_at else None
        title = (research.graph_state or {}).get("title") or research.prompt
        normalized = (fmt or "").lower()
        if normalized == "pdf":
            data = generate_pdf(report, research.prompt, depth, created_at)
            return data, "application/pdf", self._export_filename(title, "pdf")
        if normalized == "docx":
            data = generate_docx(report, research.prompt, depth, created_at)
            return (
                data,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                self._export_filename(title, "docx"),
            )
        if normalized == "html":
            from src.ui.report_export import generate_html

            language = self._detect_report_language(research.prompt, report)
            labels = self._HTML_EXPORT_LABELS.get(language, self._HTML_EXPORT_LABELS["en"])
            data = generate_html(
                report, research.prompt, depth, created_at,
                scorecard=self._export_scorecard(research_id), labels=labels,
                theme=theme, accent=accent, base=base,
            )
            return data, "text/html; charset=utf-8", self._export_filename(title, "html")
        if normalized in ("md", "markdown"):
            return report.encode("utf-8"), "text/markdown; charset=utf-8", self._export_filename(title, "md")
        if normalized == "json":
            payload = self._export_json_payload(research, depth, created_at)
            data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            return data, "application/json; charset=utf-8", self._export_filename(title, "json")
        if normalized in ("trail", "audit"):
            md = self._render_audit_trail_md(self.get_research_audit_trail(research_id))
            return md.encode("utf-8"), "text/markdown; charset=utf-8", self._export_filename(f"{title}-audit-trail", "md")
        raise UnprocessableError("Unsupported export format (use pdf, docx, html, md, json or trail)")

    def _export_json_payload(self, research, depth: str, created_at: str | None) -> dict:
        """Full structured export — the report plus all trust artifacts as machine-readable data."""

        def safe(fn):
            try:
                value = fn()
                return value.model_dump() if hasattr(value, "model_dump") else value
            except Exception:  # pragma: no cover - defensive
                return None

        rid = research.id
        return {
            "research_id": rid,
            "prompt": research.prompt,
            "depth": depth,
            "created_at": created_at,
            "report": research.final_report,
            "verification": safe(lambda: self.get_research_verification(rid)),
            "citations": safe(lambda: self.get_research_citation_audit(rid)),
            "source_independence": safe(lambda: self.get_research_source_independence(rid)),
            "source_reputation": safe(lambda: self.get_research_source_reputation(rid)),
            "source_integrity": safe(lambda: self.get_research_source_integrity(rid)),
            "cross_language": safe(lambda: self.get_research_cross_language(rid)),
            "stance_balance": safe(lambda: self.get_research_stance(rid)),
            "numeric_check": safe(lambda: self.get_research_numeric_check(rid)),
            "confidence": safe(lambda: self.get_research_confidence(rid)),
            "red_team": safe(lambda: self.get_research_red_team(rid)),
            "comparison": safe(lambda: self.get_research_comparison(rid)),
            "sources": safe(lambda: [s.model_dump() for s in self.get_research_sources(rid)]),
        }

    _HTML_EXPORT_LABELS = {
        "ru": {
            "coverage": "Покрытие плана", "citations": "Цитаты", "sources": "Источников",
            "highQuality": "высокого качества", "redteam": "Red-team", "challengedHeld": "оспорено / устояло",
            "eyebrow": "Исследование",
            "footer": "Сделано в verifiable research — каждое утверждение прослеживается до источника.",
        },
        "en": {
            "coverage": "Plan coverage", "citations": "Citations", "sources": "Sources",
            "highQuality": "high quality", "redteam": "Red-team", "challengedHeld": "challenged / held",
            "eyebrow": "Research report",
            "footer": "Generated with verifiable research — every claim traceable to its source.",
        },
    }

    def _export_scorecard(self, research_id: str) -> dict:
        """Trust metrics for the HTML export header (best-effort — each metric is optional)."""
        sc: dict = {}
        try:
            sc["coverage_pct"] = round(self.get_research_verification(research_id).coverage_ratio * 100)
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            citations = self.get_research_citation_audit(research_id)
            if citations.total:
                sc.update(integrity_pct=round(citations.integrity * 100), supported=citations.supported, total=citations.total)
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            sources = self.get_research_sources(research_id)
            sc["sources"] = len(sources)
            sc["high_sources"] = sum(1 for s in sources if (s.source_quality or "") == "high")
        except Exception:  # pragma: no cover - defensive
            pass
        try:
            red_team = self.get_research_red_team(research_id)
            if red_team.findings:
                sc.update(has_redteam=True, challenged=red_team.challenged, held=red_team.held)
        except Exception:  # pragma: no cover - defensive
            pass
        return sc

