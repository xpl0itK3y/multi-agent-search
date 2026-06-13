"""Living-research diff (no LLM).

Compares the per-claim findings of a fresh run against the previous run of the same
research and reports what changed: new claims, dropped claims, claims whose confidence
shifted, and newly-appearing source domains. Claims are matched by lexical similarity.
"""
from __future__ import annotations

import re

from src.api.schemas import DiffClaim, ResearchDiff

_WORD = re.compile(r"[^\W\d_]{3,}", re.UNICODE)
_MATCH_THRESHOLD = 0.45  # Jaccard over claim tokens to treat two claims as "the same"

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "than", "then", "have",
    "это", "как", "что", "для", "при", "также", "более", "может", "быть", "если", "или",
}


class ResearchDiffAgent:
    def diff(self, new_findings: list, old_findings: list, new_domains: list[str], old_domains: list[str]) -> ResearchDiff:
        new_tokens = [(f, self._tokens(getattr(f, "statement", ""))) for f in new_findings]
        old_tokens = [(f, self._tokens(getattr(f, "statement", ""))) for f in old_findings]

        matched_old: set[int] = set()
        new_claims: list[str] = []
        shifted: list[DiffClaim] = []

        for finding, tokens in new_tokens:
            best_i, best_score = -1, 0.0
            for i, (_old, old_tok) in enumerate(old_tokens):
                if i in matched_old:
                    continue
                score = self._similarity(tokens, old_tok)
                if score > best_score:
                    best_i, best_score = i, score
            if best_score >= _MATCH_THRESHOLD and best_i >= 0:
                matched_old.add(best_i)
                old_finding = old_tokens[best_i][0]
                old_level = getattr(old_finding, "support_level", "")
                new_level = getattr(finding, "support_level", "")
                if old_level and new_level and old_level != new_level:
                    shifted.append(
                        DiffClaim(statement=getattr(finding, "statement", ""), old_level=old_level, new_level=new_level)
                    )
            else:
                new_claims.append(getattr(finding, "statement", ""))

        dropped = [
            getattr(old, "statement", "")
            for i, (old, _t) in enumerate(old_tokens)
            if i not in matched_old
        ]
        old_domain_set = {d for d in old_domains if d}
        new_domains_only = [d for d in dict.fromkeys(new_domains) if d and d not in old_domain_set]

        return ResearchDiff(
            new_claims=[c for c in new_claims if c][:20],
            dropped_claims=[c for c in dropped if c][:20],
            shifted_claims=shifted[:20],
            new_sources=len(new_domains_only),
            new_domains=new_domains_only[:15],
        )

    @staticmethod
    def _similarity(a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)

    @staticmethod
    def _tokens(text: str) -> set[str]:
        return {t for t in _WORD.findall((text or "").lower()) if t not in _STOPWORDS}
