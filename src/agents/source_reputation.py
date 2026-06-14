"""Deterministic domain-reputation check (no LLM).

Matches each cited source's domain against a transparent, bundled reputation list
(satire / fabricated / conspiracy / state-controlled) and flags it with the category and a
human reason. Catches "this claim rests on a known hoax site" or "this is satire" — a signal
neither Perplexity nor Gemini surfaces inline. Purely a domain lookup; no truth judgement on
any individual article.
"""
from __future__ import annotations

from urllib.parse import urlparse

from src.api.schemas import ReputationFlag, SourceReputation
from src.source_reputation_policy import REPUTATION_FLAGS


class SourceReputationAgent:
    def assess(self, sources_by_id: dict[str, dict]) -> SourceReputation:
        """``sources_by_id``: {"S1": {"url","domain"?}, ...} as the report numbered them."""
        flagged: list[ReputationFlag] = []
        seen: set[str] = set()
        total = 0
        for source_id, src in sources_by_id.items():
            if not source_id:
                continue
            total += 1
            domain = (src.get("domain") or self._domain(src.get("url"))).lower().removeprefix("www.")
            hit = self._lookup(domain)
            if not hit:
                continue
            category, reason = hit
            key = f"{source_id}:{domain}"
            if key in seen:
                continue
            seen.add(key)
            flagged.append(ReputationFlag(source_id=source_id, domain=domain, category=category, reason=reason))

        categories = list(dict.fromkeys(f.category for f in flagged))
        return SourceReputation(
            total_sources=total,
            flagged_count=len(flagged),
            categories=categories,
            flagged=flagged,
        )

    def _lookup(self, domain: str) -> tuple[str, str] | None:
        """Exact domain or its registered domain on the list."""
        if not domain:
            return None
        if domain in REPUTATION_FLAGS:
            return REPUTATION_FLAGS[domain]
        reg = self._registered(domain)
        return REPUTATION_FLAGS.get(reg)

    @staticmethod
    def _domain(url: str | None) -> str:
        return urlparse(url or "").netloc.lower().removeprefix("www.")

    @staticmethod
    def _registered(domain: str) -> str:
        """Collapse subdomains so edition.rt.com matches rt.com."""
        parts = [p for p in (domain or "").split(".") if p]
        if len(parts) <= 2:
            return domain
        two_level = {"co", "com", "org", "net", "gov", "ac", "edu"}
        if parts[-2] in two_level and len(parts[-1]) == 2:
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])
