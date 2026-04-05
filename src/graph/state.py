from typing import Any, TypedDict

from src.api.schemas import SearchDepth, SearchTask


class FinalizeGraphState(TypedDict, total=False):
    prompt: str
    effective_prompt: str
    depth: SearchDepth
    tasks: list[SearchTask]
    report: str
    replan_recommendations: list[dict[str, Any]]
    source_summary: dict[str, Any]
    evidence_summary: dict[str, Any]
    analyze_attempts: int
    replan_attempts: int
    should_replan: bool
    should_retry_analysis: bool
