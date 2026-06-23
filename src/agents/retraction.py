"""Retraction / integrity check against Crossref + Retraction Watch (no LLM).

Extracts DOIs from cited sources, looks each up against Crossref (whose ``updated-by`` field
carries Retraction Watch records), and flags any source backed by a retracted paper or one
under an expression of concern — "your conclusion rests on a retracted study", the single most
damning source problem, which neither Perplexity nor Gemini surfaces inline.

The network fetch is INJECTED (a ``fetch(doi) -> message|None`` callable), so the DOI parsing
and classification are pure and unit-testable, and a network failure degrades to "no flags"
rather than breaking finalization.
"""
from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

from src.domain import IntegrityFlag, SourceIntegrity

# DOI syntax: 10.<registrant>/<suffix>. DOIs may contain parentheses (e.g. Lancet
# 10.1016/S0140-6736(97)11096-0), so allow them and trim a trailing markdown-wrapping ")"
# separately by paren-balance. Stop only at whitespace / quotes / angle/square/curly brackets.
_DOI = re.compile(r"10\.\d{4,9}/[^\s\"'<>\[\]{}]+", re.IGNORECASE)
_TRIM = ".,;:>\"'"
_MAX_DOIS = 12  # cap network lookups per report

_RETRACTION_TYPES = {"retraction", "removal", "withdrawal"}
_CONCERN_TYPES = {"expression_of_concern", "expression-of-concern", "concern"}


class RetractionAgent:
    def check(self, sources_by_id: dict[str, dict], fetch: Callable[[str], Optional[dict]]) -> SourceIntegrity:
        """``sources_by_id``: {"S1": {"url","content"}, ...}; ``fetch(doi)`` -> Crossref message or None."""
        doi_sources: dict[str, list[str]] = {}
        for source_id, src in sources_by_id.items():
            if not source_id:
                continue
            for doi in self._extract_dois(f"{src.get('url') or ''} {src.get('content') or ''}"):
                doi_sources.setdefault(doi, []).append(source_id)
        if not doi_sources:
            return SourceIntegrity()

        dois = list(doi_sources)[:_MAX_DOIS]
        results: dict[str, Optional[dict]] = {}
        with ThreadPoolExecutor(max_workers=min(len(dois), 6)) as executor:
            futures = {executor.submit(fetch, doi): doi for doi in dois}
            for future in as_completed(futures):
                try:
                    results[futures[future]] = future.result()
                except Exception:  # pragma: no cover - defensive
                    results[futures[future]] = None

        flagged: list[IntegrityFlag] = []
        for doi in dois:
            kind, detail = self._classify(results.get(doi))
            if not kind:
                continue
            for source_id in dict.fromkeys(doi_sources[doi]):
                flagged.append(IntegrityFlag(source_id=source_id, doi=doi, kind=kind, detail=detail))

        return SourceIntegrity(
            checked_dois=len(dois),
            retracted_count=sum(1 for f in flagged if f.kind == "retraction"),
            flagged=flagged,
        )

    # ── helpers ─────────────────────────────────────────────────────────────────

    def _extract_dois(self, text: str) -> list[str]:
        out: list[str] = []
        for raw in _DOI.findall(text or ""):
            doi = self._clean(raw)
            if doi and doi not in out:
                out.append(doi)
        return out

    @staticmethod
    def _clean(raw: str) -> str:
        doi = raw.rstrip(_TRIM)
        # Drop a trailing ")" only when it's an unbalanced wrapper (markdown "(doi)"),
        # preserving balanced parens that belong to the DOI itself.
        while doi.endswith(")") and doi.count(")") > doi.count("("):
            doi = doi[:-1].rstrip(_TRIM)
        return doi.lower()

    @staticmethod
    def _classify(message: Optional[dict]) -> tuple[str, str]:
        """Read Crossref's ``updated-by`` (Retraction Watch) and the title for a retraction signal."""
        if not message:
            return "", ""
        for update in (message.get("updated-by") or []):
            kind = (update.get("type") or "").lower()
            if kind in _RETRACTION_TYPES:
                return "retraction", update.get("label") or "Retraction"
            if kind in _CONCERN_TYPES:
                return "concern", update.get("label") or "Expression of concern"
        title = ((message.get("title") or [""])[0] or "").lower()
        if title.startswith("retracted"):
            return "retraction", "Retraction"
        if title.startswith("expression of concern") or title.startswith("withdrawn"):
            return "concern", "Expression of concern"
        return "", ""
