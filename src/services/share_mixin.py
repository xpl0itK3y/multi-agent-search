"""Public share-link concern of ResearchService (token-scoped read-only sharing), extracted
as a mixin (AUD-030). Composed into ResearchService; get_public_report assembles the report
from the self.get_research_* getters that remain on ResearchService / TrustReportMixin.
"""
import logging
import secrets

from src.domain.errors import ConflictError, NotFoundError

from src.domain import *  # noqa: F401,F403

logger = logging.getLogger(__name__)


class ShareMixin:
    def create_share_link(self, research_id: str) -> ShareInfo:
        """Mint (or return the existing) unguessable public token for a completed research."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if not research.final_report:
            raise ConflictError("Report is not ready yet")
        state = dict(research.graph_state or {})
        token = state.get("share_token")
        if not token:
            token = secrets.token_urlsafe(32)  # 256-bit, non-sequential
            state["share_token"] = token
            self.task_store.update_research_graph_state(research_id, state)
            logger.info("share_link_created research_id=%s", research_id)
        return ShareInfo(shared=True, token=token)

    def revoke_share_link(self, research_id: str) -> ShareInfo:
        """Invalidate the public link immediately (the token stops resolving)."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        state = dict(research.graph_state or {})
        if state.pop("share_token", None) is not None:
            self.task_store.update_research_graph_state(research_id, state)
            logger.info("share_link_revoked research_id=%s", research_id)
        return ShareInfo(shared=False, token="")

    def get_share_info(self, research_id: str) -> ShareInfo:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        token = (research.graph_state or {}).get("share_token") or ""
        return ShareInfo(shared=bool(token), token=token)

    def get_public_report(self, token: str) -> PublicReport:
        """Resolve a shared research by its token and return a STRICTLY-SCOPED, read-only view.

        No auth — security rests entirely on the unguessable token and the explicit field
        whitelist below. Never expose the owner, raw graph_state, or the token itself.
        """
        if not token or len(token) < 20:
            raise NotFoundError("Not found")
        research = self.task_store.get_research_by_share_token(token)
        if not research or not research.final_report:
            raise NotFoundError("Not found")
        rid = research.id
        from src.ui.report_utils import clean_report

        def safe(fn, default):
            try:
                return fn()
            except Exception:  # pragma: no cover - a missing artifact must not break the page
                return default

        return PublicReport(
            prompt=research.prompt,
            final_report=clean_report(research.final_report),
            depth=getattr(research.depth, "value", str(research.depth)),
            model=(research.graph_state or {}).get("model", "") or "",
            created_at=research.created_at.isoformat() if research.created_at else "",
            sources=safe(lambda: self.get_research_sources(rid), []),
            citations=safe(lambda: self.get_research_citation_audit(rid), CitationAudit()),
            confidence=safe(lambda: self.get_research_confidence(rid), ConfidenceReport()),
            source_independence=safe(lambda: self.get_research_source_independence(rid), SourceIndependence()),
            source_reputation=safe(lambda: self.get_research_source_reputation(rid), SourceReputation()),
            numeric_check=safe(lambda: self.get_research_numeric_check(rid), NumericCheck()),
            stance=safe(lambda: self.get_research_stance(rid), StanceBalance()),
            red_team=safe(lambda: self.get_research_red_team(rid), RedTeamReport()),
            source_integrity=safe(lambda: self.get_research_source_integrity(rid), SourceIntegrity()),
            cross_language=safe(lambda: self.get_research_cross_language(rid), CrossLanguageReport()),
        )

    # ── confidence / honesty meter (fuses all trust signals into one number) ─────

