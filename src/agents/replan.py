from collections import Counter

from src.api.schemas import ReplanRecommendation, SearchDepth, SearchTask, SourceCriticSummary


class ReplanAgent:
    MIN_SELECTED_SOURCES_BY_DEPTH = {
        SearchDepth.EASY: 4,
        SearchDepth.MEDIUM: 8,
        SearchDepth.HARD: 14,
    }

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
        if selected_sources < minimum_sources:
            recommendations.append(
                ReplanRecommendation(
                    reason="coverage is thinner than expected for the selected depth",
                    suggested_queries=[
                        f"{prompt} official sources",
                        f"{prompt} primary data",
                        f"{prompt} expert analysis",
                    ],
                )
            )

        if failed_tasks:
            recommendations.append(
                ReplanRecommendation(
                    reason="some task branches returned weak or empty evidence",
                    suggested_queries=[
                        f"{prompt} site:gov",
                        f"{prompt} site:edu",
                        f"{prompt} documentation OR report",
                    ],
                )
            )

        if source_summary and source_summary.primary_sources == 0 and depth != SearchDepth.EASY:
            recommendations.append(
                ReplanRecommendation(
                    reason="the current source pool lacks strong primary-source coverage",
                    suggested_queries=[
                        f"{prompt} official announcement",
                        f"{prompt} original report",
                        f"{prompt} primary source",
                    ],
                )
            )

        if domain_counter:
            dominant_domain, dominant_count = domain_counter.most_common(1)[0]
            total_domains = sum(domain_counter.values())
            if total_domains and dominant_count / total_domains >= 0.4:
                recommendations.append(
                    ReplanRecommendation(
                        reason="evidence is concentrated in a narrow domain set",
                        suggested_queries=[
                            f"{prompt} alternative sources",
                            f"{prompt} independent review",
                            f"{prompt} comparison analysis",
                        ],
                    )
                )

        return recommendations[:3]
