"""Normalized snapshot of one completed research — the unit metrics operate on."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class EvalSample:
    """A completed research reduced to the fields the metrics need.

    Produced two ways, both yielding the same shape:
      * ``from_service`` — read the live ``ResearchService`` after a run.
      * ``from_dict`` — load a saved JSON fixture (deterministic, offline/CI).
    """

    query_id: str
    prompt: str
    depth: str = "medium"
    status: str = "completed"
    report: str = ""
    # plan items: {"description": str, "queries": list[str]}
    plan_items: list[dict[str, Any]] = field(default_factory=list)
    # sources: {"url", "domain", "source_quality", "extraction_status"}
    sources: list[dict[str, Any]] = field(default_factory=list)
    # {"prompt_tokens", "completion_tokens", "estimated_cost_usd"}
    token_usage: dict[str, Any] = field(default_factory=dict)
    latency_seconds: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvalSample":
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def from_service(
        cls,
        service: Any,
        *,
        query_id: str,
        prompt: str,
        research_id: str,
        latency_seconds: Optional[float] = None,
    ) -> "EvalSample":
        """Build a sample from a finished research via the cheap (no-LLM) getters."""
        status = service.get_research_status_summary(research_id)
        report = service.get_research_report(research_id)
        plan = service.get_research_plan(research_id)
        sources = service.get_research_sources(research_id)
        return cls(
            query_id=query_id,
            prompt=prompt,
            depth=getattr(status.depth, "value", str(status.depth)),
            status=getattr(status.status, "value", str(status.status)),
            report=report.final_report or "",
            plan_items=[
                {"description": it.description, "queries": list(it.queries)}
                for it in plan.items
            ],
            sources=[
                {
                    "url": s.url,
                    "domain": s.domain,
                    "source_quality": s.source_quality,
                    "extraction_status": s.extraction_status,
                }
                for s in sources
            ],
            token_usage=dict(status.llm_token_usage or {}),
            latency_seconds=latency_seconds,
        )
