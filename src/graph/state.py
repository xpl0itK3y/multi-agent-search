from typing import Any, TypedDict

from src.domain import SearchDepth, SearchTask


class FinalizeGraphState(TypedDict, total=False):
    research_id: str
    finalize_job_id: str
    finalize_lease_epoch: int
    prompt: str
    effective_prompt: str
    depth: SearchDepth
    tasks: list[SearchTask]
    model: str | None
    report: str
    replan_recommendations: list[dict[str, Any]]
    tie_break_recommendations: list[dict[str, Any]]
    detected_conflicts: list[dict[str, Any]]
    source_summary: dict[str, Any]
    evidence_summary: dict[str, Any]
    analyze_attempts: int
    replan_attempts: int
    tie_break_attempts: int
    finalize_deadline: float
    branch_stalled: bool
    resume_from_step: str
    should_replan: bool
    should_tie_break: bool
    should_retry_analysis: bool
