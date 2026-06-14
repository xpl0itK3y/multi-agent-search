"""Deterministic source-independence / echo-chamber detector (no LLM).

Perplexity and Gemini present every citation as if it were independent corroboration.
But ten "sources" are often the same wire story reprinted across ten domains, or ten
articles from one outlet — so the claim really rests on **one** origin, not ten.

This agent clusters the cited sources into *independent origins* by two relations:
  * same registered domain  → same outlet (not independent of each other), and
  * near-duplicate text     → the same story syndicated across outlets (a reprint).

It then reports how many independent origins the source set truly represents
(``independence_score``), the share concentrated in the largest origin, and a
human-readable warning for each echo cluster. Purely lexical — no truth judgement.
"""
from __future__ import annotations

from urllib.parse import urlparse

from src.api.schemas import OriginCluster, SourceIndependence

_WORD_MIN = 3
_SYNDICATION_THRESHOLD = 0.50  # Jaccard over content tokens above which two sources are "the same text"
_DOMINANT_WARN_SHARE = 0.40    # warn when one origin backs >= this share of all sources

_STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "into", "than", "then",
    "have", "has", "был", "была", "было", "были", "это", "как", "что", "для", "при",
    "также", "более", "может", "быть", "если", "или", "так", "его", "their", "these",
    "those", "which", "while", "about", "would", "could", "should", "там", "https", "http",
    "www", "com", "html",
}


class SourceIndependenceAgent:
    def analyze(self, sources_by_id: dict[str, dict]) -> SourceIndependence:
        """``sources_by_id``: {"S1": {"content","url","title","domain"?}, ...} as the report numbered them."""
        items = self._materialize(sources_by_id)
        total = len(items)
        if total == 0:
            return SourceIndependence()
        if total == 1:
            return SourceIndependence(total_sources=1, independent_origins=1, independence_score=1.0, dominant_origin_share=1.0)

        parent = list(range(total))

        def find(a: int) -> int:
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

        # Relation 1: same registered domain → same outlet.
        by_domain: dict[str, int] = {}
        for i, it in enumerate(items):
            anchor = by_domain.setdefault(it["reg_domain"], i)
            if anchor != i and it["reg_domain"]:
                union(anchor, i)

        # Relation 2: near-duplicate body text → the same story reprinted across outlets.
        for i in range(total):
            for j in range(i + 1, total):
                if find(i) == find(j):
                    continue
                if self._jaccard(items[i]["tokens"], items[j]["tokens"]) >= _SYNDICATION_THRESHOLD:
                    union(i, j)

        groups: dict[int, list[int]] = {}
        for i in range(total):
            groups.setdefault(find(i), []).append(i)

        clusters: list[OriginCluster] = []
        for members in groups.values():
            if len(members) < 2:
                continue  # singletons are independent by definition — not worth listing
            domains = list(dict.fromkeys(items[m]["reg_domain"] for m in members if items[m]["reg_domain"]))
            multi_domain = len(domains) > 1
            label = self._cluster_label(items, members, domains)
            clusters.append(
                OriginCluster(
                    label=label,
                    kind="syndicated" if multi_domain else "single-domain",
                    size=len(members),
                    domains=domains,
                    source_ids=[items[m]["source_id"] for m in members],
                )
            )

        clusters.sort(key=lambda c: c.size, reverse=True)
        origins = len(groups)
        largest = max(len(m) for m in groups.values())

        return SourceIndependence(
            total_sources=total,
            independent_origins=origins,
            independence_score=round(origins / total, 4),
            dominant_origin_share=round(largest / total, 4),
            clusters=clusters,
            echo_warnings=self._warnings(clusters, largest, total),
        )

    # ── helpers ───────────────────────────────────────────────────────────────

    def _materialize(self, sources_by_id: dict[str, dict]) -> list[dict]:
        out: list[dict] = []
        for source_id, src in sources_by_id.items():
            if not source_id:
                continue
            url = src.get("url") or ""
            domain = (src.get("domain") or self._domain(url)).lower().removeprefix("www.")
            text = f"{src.get('title') or ''} {src.get('content') or ''}"
            out.append(
                {
                    "source_id": source_id,
                    "url": url,
                    "domain": domain,
                    "reg_domain": self._registered(domain),
                    "title": (src.get("title") or "").strip(),
                    "tokens": self._tokens(text),
                }
            )
        return out

    def _warnings(self, clusters: list[OriginCluster], largest: int, total: int) -> list[str]:
        warnings: list[str] = []
        for c in clusters:
            if c.kind == "syndicated":
                warnings.append(
                    f"{c.size} sources are the same story reprinted across {', '.join(c.domains[:4])}"
                )
            else:
                warnings.append(f"{c.size} sources all come from {c.label}")
        if total and largest / total >= _DOMINANT_WARN_SHARE and largest > 1:
            warnings.insert(0, f"{round(100 * largest / total)}% of sources trace back to a single origin")
        return warnings[:8]

    @staticmethod
    def _cluster_label(items: list[dict], members: list[int], domains: list[str]) -> str:
        if domains:
            return domains[0]
        for m in members:
            if items[m]["title"]:
                return items[m]["title"][:80]
        return items[members[0]]["source_id"]

    @staticmethod
    def _jaccard(a: set[str], b: set[str]) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        if not inter:
            return 0.0
        return inter / len(a | b)

    @staticmethod
    def _domain(url: str) -> str:
        return urlparse(url or "").netloc.lower().removeprefix("www.")

    @staticmethod
    def _registered(domain: str) -> str:
        """Collapse subdomains so edition.cnn.com and www.cnn.com count as one outlet."""
        parts = [p for p in (domain or "").split(".") if p]
        if len(parts) <= 2:
            return domain
        # keep last 3 labels for common two-level TLDs (co.uk, com.au, …), else last 2
        two_level = {"co", "com", "org", "net", "gov", "ac", "edu"}
        if parts[-2] in two_level and len(parts[-1]) == 2:
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])

    @staticmethod
    def _tokens(text: str) -> set[str]:
        out: set[str] = set()
        word = ""
        for ch in (text or "").lower():
            if ch.isalnum():
                word += ch
            else:
                if len(word) >= _WORD_MIN and word not in _STOPWORDS:
                    out.add(word)
                word = ""
        if len(word) >= _WORD_MIN and word not in _STOPWORDS:
            out.add(word)
        return out
