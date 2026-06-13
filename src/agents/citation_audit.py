"""Deterministic citation audit (no LLM).

For every inline ``[Sn]`` citation in the report, check whether the cited source's
text actually matches the sentence — by lexical overlap between the claim and the
best-matching passage in that source. Surfaces:
  * an integrity score (how many citations the source text actually backs),
  * the specific claims whose citation does not match (Gemini/Perplexity's failure),
  * a grounding quote per source for inline hover.

This is a lexical match, not a truth judgement — it catches citations whose source
never mentions the claim's terms, which is the common "fabricated citation" failure.
"""
from __future__ import annotations

import re

from src.api.schemas import CitationAudit, CitationGround

_CITATION = re.compile(r"\[S(\d+)\]")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_WORD = re.compile(r"[^\W\d_]{3,}", re.UNICODE)
_SUPPORT_THRESHOLD = 0.20  # share of claim terms that must appear in the source passage

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "into", "than", "then",
    "have", "has", "был", "была", "было", "были", "это", "как", "что", "для", "при",
    "также", "более", "может", "быть", "если", "или", "так", "его", "their", "these",
    "those", "which", "while", "about", "would", "could", "should", "там",
}


class CitationAuditAgent:
    def audit(self, report: str, sources_by_id: dict[str, dict]) -> CitationAudit:
        """``sources_by_id``: {"S1": {"content","url","title"}, ...} as the report numbered them."""
        if not report or not sources_by_id:
            return CitationAudit()

        checks: list[tuple[str, str, bool]] = []  # (claim, source_id, supported)
        best: dict[str, tuple[float, str, bool]] = {}  # source_id -> (score, quote, supported)

        for sentence in _SENTENCE_SPLIT.split(report):
            ids = _CITATION.findall(sentence)
            if not ids:
                continue
            claim = _CITATION.sub("", sentence).strip(" -*#•\t")
            claim_tokens = self._tokens(claim)
            if len(claim_tokens) < 4:
                continue  # too short to judge meaningfully
            for n in dict.fromkeys(ids):  # dedupe, preserve order
                source_id = f"S{n}"
                source = sources_by_id.get(source_id)
                if not source:
                    continue
                quote, score = self._best_passage(claim_tokens, source.get("content") or "")
                supported = score >= _SUPPORT_THRESHOLD
                checks.append((claim, source_id, supported))
                if source_id not in best or score > best[source_id][0]:
                    best[source_id] = (score, quote, supported)

        total = len(checks)
        supported = sum(1 for _, _, ok in checks if ok)
        unsupported_claims = [claim for claim, _, ok in checks if not ok][:20]
        grounding = [
            CitationGround(
                source_id=source_id,
                url=(sources_by_id.get(source_id) or {}).get("url", "") or "",
                title=(sources_by_id.get(source_id) or {}).get("title", "") or "",
                quote=quote,
                supported=supported,
            )
            for source_id, (_score, quote, supported) in best.items()
        ]
        return CitationAudit(
            total=total,
            supported=supported,
            integrity=(supported / total) if total else 0.0,
            unsupported_claims=unsupported_claims,
            grounding=grounding,
        )

    # ── matching ────────────────────────────────────────────────────────────────

    def _best_passage(self, claim_tokens: set[str], content: str) -> tuple[str, float]:
        """Return the source passage best overlapping the claim, and the overlap share."""
        best_text, best_score = "", 0.0
        for passage in _SENTENCE_SPLIT.split(content):
            passage = passage.strip()
            if not passage:
                continue
            overlap = len(claim_tokens & self._tokens(passage))
            score = overlap / len(claim_tokens) if claim_tokens else 0.0
            if score > best_score:
                best_text, best_score = passage, score
        return best_text[:240].strip(), best_score

    @staticmethod
    def _tokens(text: str) -> set[str]:
        return {t for t in _WORD.findall((text or "").lower()) if t not in _STOPWORDS}
