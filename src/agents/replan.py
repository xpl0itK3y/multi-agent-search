import json
import logging
from collections import Counter
from typing import Optional

from src.api.schemas import ReplanRecommendation, SearchDepth, SearchTask, SourceCriticSummary
from src.config import settings
from src.core.llm import LLMProvider

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are a Search Gap Analyst.
Given a research topic and a specific coverage gap, generate exactly 3 targeted search queries to fill that gap.
The queries must be distinct, concrete, and more specific than a generic topic search.
Return ONLY a valid JSON array of 3 strings. No explanation, no markdown, no extra keys.
Example: ["query one here", "query two here", "query three here"]"""


class ReplanAgent:
    MIN_SELECTED_SOURCES_BY_DEPTH = {
        SearchDepth.EASY: 4,
        SearchDepth.MEDIUM: 8,
        SearchDepth.HARD: 14,
    }
    TOPIC_HINTS = {
        "movie": ["studio press release", "box office report", "festival review"],
        "film": ["studio press release", "box office report", "festival review"],
        "linux": ["official documentation", "vendor documentation", "benchmark report"],
        "macos": ["official documentation", "vendor documentation", "benchmark report"],
        "ai": ["research paper", "official model card", "benchmark report"],
        "llm": ["research paper", "official model card", "benchmark report"],
        "phone": ["manufacturer specification", "benchmark report", "independent review"],
        "smartphone": ["manufacturer specification", "benchmark report", "independent review"],
    }

    def __init__(self, llm: Optional[LLMProvider] = None) -> None:
        self.llm = llm

    # ── LLM-based query generation ────────────────────────────────────────────

    def _llm_queries(
        self,
        prompt: str,
        reason: str,
        task_descriptions: list[str],
        fallback: list[str],
    ) -> list[str]:
        """Ask the LLM for gap-specific queries; fall back to templates on any failure."""
        if self.llm is None:
            return fallback

        already_searched = "; ".join(task_descriptions[:6]) if task_descriptions else "none"
        user_prompt = (
            f"Research topic: {prompt}\n"
            f"Coverage gap: {reason}\n"
            f"Already searched: {already_searched}\n"
            "Generate 3 highly specific queries to fill this gap."
        )
        # Gap analysis ("what to search next") benefits from a reasoning model — route to
        # it when configured (opt-in; falls back to the base model when unset).
        generate_kwargs = {}
        if settings.deepseek_reasoner_model:
            generate_kwargs["model"] = settings.deepseek_reasoner_model
        try:
            raw = self.llm.generate(
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                **generate_kwargs,
            )
            clean = raw.strip()
            # Strip optional markdown fences
            if clean.startswith("```"):
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
                clean = clean.strip()
            queries = json.loads(clean)
            if isinstance(queries, list) and queries:
                valid = [str(q).strip() for q in queries if str(q).strip()]
                if valid:
                    logger.info(
                        "replan_llm_queries generated count=%d reason=%r", len(valid), reason
                    )
                    return valid[:3]
        except Exception as exc:
            logger.warning("replan_llm_queries_failed reason=%r error=%s", reason, exc)
        return fallback

    # ── helpers ───────────────────────────────────────────────────────────────

    def _topic_policy_hints(self, prompt: str) -> list[str]:
        lowered = prompt.lower()
        hints: list[str] = []
        for keyword, values in self.TOPIC_HINTS.items():
            if keyword in lowered:
                hints.extend(values)
        deduped: list[str] = []
        for hint in hints:
            if hint not in deduped:
                deduped.append(hint)
        return deduped[:3]

    # ── public API ────────────────────────────────────────────────────────────

    def suggest_follow_up(
        self,
        prompt: str,
        depth: SearchDepth,
        tasks: list[SearchTask],
        source_summary: SourceCriticSummary | None = None,
    ) -> list[ReplanRecommendation]:
        recommendations: list[ReplanRecommendation] = []
        selected_sources = sum(task.search_metrics.selected_source_count for task in tasks)
        failed_tasks = [task for task in tasks if task.status.value == "failed" or not (task.result or [])]
        domain_counter: Counter[str] = Counter()
        for task in tasks:
            for result in task.result or []:
                domain = result.get("domain")
                if domain:
                    domain_counter[domain] += 1

        minimum_sources = self.MIN_SELECTED_SOURCES_BY_DEPTH[depth]
        topic_hints = self._topic_policy_hints(prompt)
        task_descriptions = [t.description for t in tasks if t.description]

        if selected_sources < minimum_sources:
            reason = "coverage is thinner than expected for the selected depth"
            fallback = [
                f"{prompt} official sources",
                f"{prompt} primary data",
                f"{prompt} expert analysis",
                *[f"{prompt} {hint}" for hint in topic_hints],
            ]
            recommendations.append(
                ReplanRecommendation(
                    reason=reason,
                    suggested_queries=self._llm_queries(prompt, reason, task_descriptions, fallback),
                )
            )

        if failed_tasks:
            reason = "some task branches returned weak or empty evidence"
            fallback = [
                f"{prompt} site:gov",
                f"{prompt} site:edu",
                f"{prompt} documentation OR report",
                *[f"{prompt} {hint}" for hint in topic_hints],
            ]
            recommendations.append(
                ReplanRecommendation(
                    reason=reason,
                    suggested_queries=self._llm_queries(prompt, reason, task_descriptions, fallback),
                )
            )

        if source_summary and source_summary.primary_sources == 0 and depth != SearchDepth.EASY:
            reason = "the current source pool lacks strong primary-source coverage"
            fallback = [
                f"{prompt} official announcement",
                f"{prompt} original report",
                f"{prompt} primary source",
                *[f"{prompt} {hint}" for hint in topic_hints],
            ]
            recommendations.append(
                ReplanRecommendation(
                    reason=reason,
                    suggested_queries=self._llm_queries(prompt, reason, task_descriptions, fallback),
                )
            )

        if domain_counter:
            dominant_domain, dominant_count = domain_counter.most_common(1)[0]
            total_source_count = sum(domain_counter.values())
            if total_source_count and dominant_count / total_source_count >= 0.4:
                reason = f"evidence is concentrated in a narrow domain set (dominant: {dominant_domain})"
                fallback = [
                    f"{prompt} alternative sources",
                    f"{prompt} independent review",
                    f"{prompt} comparison analysis",
                    *[f"{prompt} {hint}" for hint in topic_hints],
                ]
                recommendations.append(
                    ReplanRecommendation(
                        reason=reason,
                        suggested_queries=self._llm_queries(prompt, reason, task_descriptions, fallback),
                    )
                )

        return recommendations[:3]

    def suggest_tie_breakers(
        self,
        prompt: str,
        conflicts: list[dict] | None = None,
        weak_support: bool = False,
    ) -> list[ReplanRecommendation]:
        recommendations: list[ReplanRecommendation] = []
        conflicts = conflicts or []
        topic_hints = self._topic_policy_hints(prompt)

        for conflict in conflicts[:2]:
            topic = conflict.get("topic") or "disputed point"
            reason = f"resolve conflicting evidence about {topic}"
            fallback = [
                f"{prompt} {topic} official source",
                f"{prompt} {topic} primary source",
                f"{prompt} {topic} expert analysis",
                *[f"{prompt} {topic} {hint}" for hint in topic_hints],
            ]
            recommendations.append(
                ReplanRecommendation(
                    reason=reason,
                    suggested_queries=self._llm_queries(prompt, reason, [], fallback),
                )
            )

        if weak_support:
            reason = "strengthen weakly supported claims with narrower evidence"
            fallback = [
                f"{prompt} source-backed analysis",
                f"{prompt} official report",
                f"{prompt} documentation evidence",
                *[f"{prompt} {hint}" for hint in topic_hints],
            ]
            recommendations.append(
                ReplanRecommendation(
                    reason=reason,
                    suggested_queries=self._llm_queries(prompt, reason, [], fallback),
                )
            )

        return recommendations[:3]
