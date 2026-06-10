"""Pure metrics over an :class:`~eval.sample.EvalSample`.

Each metric is a plain function ``(sample, query) -> float`` so it is trivially
unit-testable and free of network/LLM dependencies. A metric may return ``nan``
to mean "not applicable for this sample" (e.g. coverage when there is nothing to
cover, or latency on a fixture run) — aggregation skips ``nan`` values.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlsplit

from .sample import EvalSample

_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
_CITATION = re.compile(r"\[S(\d+)\]")

# Compact bilingual stopword set — enough to keep coverage heuristics honest
# without dragging in a dependency. Mirrors the spirit of the chat-escalation
# heuristic in the service layer.
_STOPWORDS = {
    # en
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "by",
    "is", "are", "was", "were", "be", "as", "at", "from", "that", "this", "it",
    "what", "which", "how", "why", "vs", "between", "about", "into", "their",
    # ru
    "и", "в", "во", "не", "на", "с", "со", "что", "как", "для", "по", "из",
    "это", "к", "у", "о", "от", "за", "же", "ли", "бы", "или", "а", "но",
    "их", "его", "её", "при", "про", "над", "под", "чем", "vs",
}


def _tokens(text: str) -> set[str]:
    return {
        t
        for w in _WORD.findall((text or "").lower())
        if len(t := w) >= 4 and t not in _STOPWORDS
    }


def _domain(source: dict[str, Any]) -> str:
    dom = (source.get("domain") or "").strip().lower()
    if dom:
        return dom
    host = urlsplit(source.get("url") or "").netloc.lower()
    return host[4:] if host.startswith("www.") else host


def _overlap_ratio(needles: set[str], haystack: set[str]) -> float:
    if not needles:
        return math.nan
    return len(needles & haystack) / len(needles)


def _word_count(text: str) -> int:
    # Count words on the prose only — strip [Sn] markers so citations don't
    # inflate the word total (and thus deflate citation density).
    return len(_WORD.findall(_CITATION.sub(" ", text or "")))


# --- individual metrics --------------------------------------------------------

def report_words(s: EvalSample, q: dict[str, Any]) -> float:
    return float(_word_count(s.report))


def unique_sources(s: EvalSample, q: dict[str, Any]) -> float:
    return float(len({(src.get("url") or "").strip() for src in s.sources if src.get("url")}))


def unique_domains(s: EvalSample, q: dict[str, Any]) -> float:
    return float(len({d for src in s.sources if (d := _domain(src))}))


def high_quality_rate(s: EvalSample, q: dict[str, Any]) -> float:
    """Share of sources graded high or medium quality."""
    graded = [(src.get("source_quality") or "").lower() for src in s.sources]
    graded = [g for g in graded if g]
    if not graded:
        return math.nan
    good = sum(1 for g in graded if g in ("high", "medium"))
    return good / len(graded)


def citation_count(s: EvalSample, q: dict[str, Any]) -> float:
    return float(len(_CITATION.findall(s.report or "")))


def cited_sources(s: EvalSample, q: dict[str, Any]) -> float:
    return float(len(set(_CITATION.findall(s.report or ""))))


def citation_density(s: EvalSample, q: dict[str, Any]) -> float:
    """Citations per 1000 words of report — grounding intensity."""
    words = _word_count(s.report)
    if words == 0:
        return math.nan
    return len(_CITATION.findall(s.report or "")) / words * 1000.0


def plan_coverage(s: EvalSample, q: dict[str, Any]) -> float:
    """Share of plan sub-questions reflected in the final report.

    A plan item counts as covered when at least half of its meaningful keywords
    appear in the report. ``nan`` when there is no plan (e.g. easy depth).
    """
    ratios: list[float] = []
    for item in s.plan_items:
        needles = _tokens(item.get("description", ""))
        r = _overlap_ratio(needles, _tokens(s.report))
        if not math.isnan(r):
            ratios.append(1.0 if r >= 0.5 else 0.0)
    if not ratios:
        return math.nan
    return sum(ratios) / len(ratios)


def must_mention_rate(s: EvalSample, q: dict[str, Any]) -> float:
    """Share of the gold ``must_mention`` terms present in the report.

    Case-insensitive substring match on whitespace-normalized text, so specific
    terms like ``cas9``, ``off-target`` or ``olive oil`` are matched exactly
    rather than approximated by token overlap. ``nan`` when the gold query
    specifies no required terms.
    """
    phrases = [p for p in (q.get("must_mention") or []) if p.strip()]
    if not phrases:
        return math.nan
    haystack = re.sub(r"\s+", " ", (s.report or "").lower())
    hits = sum(1 for p in phrases if re.sub(r"\s+", " ", p.strip().lower()) in haystack)
    return hits / len(phrases)


def cost_usd(s: EvalSample, q: dict[str, Any]) -> float:
    return float(s.token_usage.get("estimated_cost_usd") or 0.0)


def total_tokens(s: EvalSample, q: dict[str, Any]) -> float:
    u = s.token_usage
    return float((u.get("prompt_tokens") or 0) + (u.get("completion_tokens") or 0))


def latency_seconds(s: EvalSample, q: dict[str, Any]) -> float:
    return float(s.latency_seconds) if s.latency_seconds is not None else math.nan


@dataclass(frozen=True)
class Metric:
    name: str
    fn: Callable[[EvalSample, dict[str, Any]], float]
    higher_is_better: bool
    unit: str = ""


# Ordered registry — drives computation, aggregation and the baseline gate.
METRICS: list[Metric] = [
    Metric("unique_sources", unique_sources, True),
    Metric("unique_domains", unique_domains, True),
    Metric("high_quality_rate", high_quality_rate, True),
    Metric("citation_count", citation_count, True),
    Metric("cited_sources", cited_sources, True),
    Metric("citation_density", citation_density, True, "per 1k words"),
    Metric("plan_coverage", plan_coverage, True),
    Metric("must_mention_rate", must_mention_rate, True),
    Metric("report_words", report_words, True),
    Metric("cost_usd", cost_usd, False, "$"),
    Metric("total_tokens", total_tokens, False),
    Metric("latency_seconds", latency_seconds, False, "s"),
]

METRICS_BY_NAME: dict[str, Metric] = {m.name: m for m in METRICS}


def compute_metrics(sample: EvalSample, query: dict[str, Any] | None = None) -> dict[str, float]:
    """Compute every registered metric for one sample."""
    query = query or {}
    out: dict[str, float] = {}
    for m in METRICS:
        value = float(m.fn(sample, query))
        out[m.name] = value if math.isnan(value) else round(value, 4)
    return out
