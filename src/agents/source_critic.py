from collections import Counter
from urllib.parse import urlparse

from src.api.schemas import SourceCriticSummary


class SourceCriticAgent:
    PRIMARY_DOMAIN_SUFFIXES = (".gov", ".edu")
    PRIMARY_DOMAIN_EXACT_MATCHES = {
        "apple.com",
        "support.apple.com",
        "developer.apple.com",
        "google.com",
        "blog.google",
        "openai.com",
        "platform.openai.com",
        "docs.python.org",
        "developer.mozilla.org",
        "wikipedia.org",
    }
    COMMUNITY_DOMAIN_EXACT_MATCHES = {
        "reddit.com",
        "news.ycombinator.com",
        "stackexchange.com",
        "stackoverflow.com",
        "quora.com",
    }
    SPECULATIVE_TOKENS = (
        "rumor",
        "rumour",
        "prediction",
        "predictions",
        "what to expect",
        "coming soon",
        "upcoming",
        "forecast",
    )
    EDITORIAL_TOKENS = (
        "best",
        "top",
        "roundup",
        "review",
        "reviews",
        "comparison",
        "guide",
        "ranking",
        "editor",
    )

    def _domain(self, url: str | None) -> str:
        return urlparse(url or "").netloc.lower().removeprefix("www.")

    def _source_type(self, item: dict) -> str:
        domain = self._domain(item.get("url"))
        title = (item.get("title") or "").lower()
        content = (item.get("content") or "").lower()[:500]

        if domain in self.PRIMARY_DOMAIN_EXACT_MATCHES or any(domain.endswith(suffix) for suffix in self.PRIMARY_DOMAIN_SUFFIXES):
            return "primary"
        if domain in self.COMMUNITY_DOMAIN_EXACT_MATCHES:
            return "community"
        if any(token in title or token in content for token in self.SPECULATIVE_TOKENS):
            return "speculative"
        if any(token in title or token in content for token in self.EDITORIAL_TOKENS):
            return "editorial"
        return "general"

    def _confidence(self, item: dict, source_type: str) -> str:
        quality = item.get("source_quality") or "low"
        content_length = len(item.get("content") or "")

        if quality == "high" or source_type == "primary":
            return "high"
        if quality == "medium" or (source_type == "editorial" and content_length >= 600):
            return "medium"
        return "low"

    def assess_sources(self, aggregated_data: list[dict]) -> tuple[list[dict], SourceCriticSummary]:
        annotated: list[dict] = []
        domain_counter: Counter[str] = Counter()
        high_confidence = 0
        medium_confidence = 0
        low_confidence = 0
        primary_sources = 0
        editorial_sources = 0
        community_sources = 0
        speculative_sources = 0
        flagged_sources = 0

        for item in aggregated_data:
            domain = item.get("domain") or self._domain(item.get("url"))
            source_type = self._source_type(item)
            confidence = self._confidence(item, source_type)
            caution_flags: list[str] = []

            if source_type == "speculative":
                speculative_sources += 1
                caution_flags.append("speculative")
            if source_type == "community":
                community_sources += 1
                caution_flags.append("community")
            if source_type == "primary":
                primary_sources += 1
            if source_type == "editorial":
                editorial_sources += 1

            if confidence == "high":
                high_confidence += 1
            elif confidence == "medium":
                medium_confidence += 1
            else:
                low_confidence += 1
                caution_flags.append("low_confidence")

            if caution_flags:
                flagged_sources += 1

            if domain:
                domain_counter[domain] += 1

            annotated.append(
                {
                    **item,
                    "source_type": source_type,
                    "confidence": confidence,
                    "caution_flags": caution_flags,
                }
            )

        summary = SourceCriticSummary(
            total_sources=len(annotated),
            high_confidence_sources=high_confidence,
            medium_confidence_sources=medium_confidence,
            low_confidence_sources=low_confidence,
            primary_sources=primary_sources,
            editorial_sources=editorial_sources,
            community_sources=community_sources,
            speculative_sources=speculative_sources,
            flagged_sources=flagged_sources,
            dominant_domains=[domain for domain, _ in domain_counter.most_common(3)],
        )
        return annotated, summary
