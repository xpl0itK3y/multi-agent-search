from __future__ import annotations

import logging
from typing import Any

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import SearchTask
from src.config import settings
from src.graph.state import FinalizeGraphState

logger = logging.getLogger(__name__)

try:
    from langgraph.graph import END, StateGraph
except Exception:  # pragma: no cover - optional dependency
    END = "__end__"
    StateGraph = None


class FinalizeGraphRunner:
    def __init__(self, service):
        self.service = service

    def run(self, prompt: str, tasks: list[SearchTask], depth) -> str:
        state: FinalizeGraphState = {
            "prompt": prompt,
            "effective_prompt": prompt,
            "depth": depth,
            "tasks": tasks,
            "analyze_attempts": 0,
            "replan_attempts": 0,
            "should_replan": False,
            "should_retry_analysis": False,
        }
        if StateGraph is not None:
            return self._run_langgraph(state)
        return self._run_fallback(state)

    def _collect_context(self, state: FinalizeGraphState) -> FinalizeGraphState:
        aggregated_sources = self.service._build_research_source_pool(state["tasks"])
        _, source_summary = self.service.source_critic.assess_sources(aggregated_sources)
        analyzer = self.service.analyzer
        _, evidence_summary = self.service.evidence_mapper.build_evidence_groups(
            aggregated_sources,
            stopwords=getattr(analyzer, "STOPWORDS", AnalyzerAgent.STOPWORDS),
            generic_tokens=getattr(analyzer, "CONFLICT_GENERIC_TOKENS", AnalyzerAgent.CONFLICT_GENERIC_TOKENS),
            negation_tokens=getattr(analyzer, "NEGATION_TOKENS", AnalyzerAgent.NEGATION_TOKENS),
            max_groups=5,
        )
        recommendations = self.service.replan_agent.suggest_follow_up(
            state["prompt"],
            state["depth"],
            state["tasks"],
            source_summary=source_summary,
        )
        should_replan = bool(recommendations) and state["replan_attempts"] < settings.langgraph_replan_max_loops
        return {
            **state,
            "source_summary": source_summary.model_dump(),
            "evidence_summary": evidence_summary.model_dump(),
            "replan_recommendations": [item.model_dump() for item in recommendations],
            "should_replan": should_replan,
        }

    def _apply_replan(self, state: FinalizeGraphState) -> FinalizeGraphState:
        recommendations = state.get("replan_recommendations") or []
        if not recommendations:
            return {**state, "should_replan": False}
        query_hints: list[str] = []
        for recommendation in recommendations:
            query_hints.extend(recommendation.get("suggested_queries") or [])
        hint_block = "\n".join(f"- {query}" for query in query_hints[:6])
        effective_prompt = (
            f"{state['prompt']}\n\n"
            "Uncovered angles to prioritize during synthesis:\n"
            f"{hint_block}"
        )
        return {
            **state,
            "effective_prompt": effective_prompt,
            "replan_attempts": state["replan_attempts"] + 1,
            "should_replan": False,
        }

    def _analyze(self, state: FinalizeGraphState) -> FinalizeGraphState:
        report = self.service.analyzer.run_analysis(
            state["effective_prompt"],
            state["tasks"],
            depth=state["depth"],
        )
        return {
            **state,
            "report": report,
            "analyze_attempts": state["analyze_attempts"] + 1,
        }

    def _verify(self, state: FinalizeGraphState) -> FinalizeGraphState:
        report = state.get("report") or ""
        should_retry = False
        if state["analyze_attempts"] <= settings.langgraph_verification_max_retries:
            should_retry = self._report_needs_retry(report)
        if should_retry:
            effective_prompt = (
                f"{state['effective_prompt']}\n\n"
                "The previous draft still had report notes or weak-support issues. "
                "Prioritize higher-confidence evidence, reduce overconfident wording, and improve citation discipline."
            )
        else:
            effective_prompt = state["effective_prompt"]
        return {
            **state,
            "effective_prompt": effective_prompt,
            "should_retry_analysis": should_retry,
        }

    def _report_needs_retry(self, report: str) -> bool:
        normalized = report.lower()
        return (
            "## report notes" in normalized
            or "## примечания к отчёту" in normalized
            or "## примечания к отчету" in normalized
            or "weakly supported" in normalized
            or "слабо подтверж" in normalized
        )

    def _next_after_context(self, state: FinalizeGraphState) -> str:
        return "replan" if state.get("should_replan") else "analyze"

    def _next_after_verify(self, state: FinalizeGraphState) -> str:
        return "analyze" if state.get("should_retry_analysis") else END

    def _run_fallback(self, state: FinalizeGraphState) -> str:
        state = self._collect_context(state)
        if state.get("should_replan"):
            state = self._apply_replan(state)
        state = self._analyze(state)
        state = self._verify(state)
        if state.get("should_retry_analysis"):
            state = self._analyze(state)
        logger.info(
            "langgraph_finalize_runner_completed replan_attempts=%s analyze_attempts=%s used_langgraph=%s",
            state.get("replan_attempts", 0),
            state.get("analyze_attempts", 0),
            False,
        )
        return state["report"]

    def _run_langgraph(self, state: FinalizeGraphState) -> str:  # pragma: no cover - optional dependency
        workflow = StateGraph(FinalizeGraphState)
        workflow.add_node("collect_context", self._collect_context)
        workflow.add_node("replan", self._apply_replan)
        workflow.add_node("analyze", self._analyze)
        workflow.add_node("verify", self._verify)
        workflow.set_entry_point("collect_context")
        workflow.add_conditional_edges("collect_context", self._next_after_context, {"replan": "replan", "analyze": "analyze"})
        workflow.add_edge("replan", "analyze")
        workflow.add_edge("analyze", "verify")
        workflow.add_conditional_edges("verify", self._next_after_verify, {"analyze": "analyze", END: END})
        compiled = workflow.compile()
        result = compiled.invoke(state)
        logger.info(
            "langgraph_finalize_runner_completed replan_attempts=%s analyze_attempts=%s used_langgraph=%s",
            result.get("replan_attempts", 0),
            result.get("analyze_attempts", 0),
            True,
        )
        return result["report"]
