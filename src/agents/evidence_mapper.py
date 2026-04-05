from src.api.schemas import EvidenceCoverageSummary
from src.core import rust_accel


class EvidenceMapperAgent:
    def build_evidence_groups(
        self,
        aggregated_data: list[dict],
        stopwords: set[str],
        generic_tokens: set[str],
        negation_tokens: set[str],
        max_groups: int = 5,
    ) -> tuple[list[dict], EvidenceCoverageSummary]:
        groups = rust_accel.extract_evidence_groups(
            aggregated_data=aggregated_data,
            stopwords=stopwords,
            generic_tokens=generic_tokens,
            negation_tokens=negation_tokens,
            max_groups=max_groups,
        )

        enriched_groups: list[dict] = []
        total_sources = 0
        multi_source_group_count = 0
        weak_group_count = 0
        for group in groups:
            source_ids = group.get("source_ids") or []
            source_count = len(source_ids)
            total_sources += source_count
            if source_count >= 2:
                multi_source_group_count += 1
            else:
                weak_group_count += 1

            support_level = "strong" if source_count >= 3 else "medium" if source_count == 2 else "weak"
            enriched_groups.append(
                {
                    **group,
                    "source_count": source_count,
                    "support_level": support_level,
                }
            )

        summary = EvidenceCoverageSummary(
            evidence_group_count=len(enriched_groups),
            multi_source_group_count=multi_source_group_count,
            weak_group_count=weak_group_count,
            avg_sources_per_group=round(total_sources / len(enriched_groups), 1) if enriched_groups else 0.0,
        )
        return enriched_groups, summary
