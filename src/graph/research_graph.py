from __future__ import annotations

import logging

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import ReplanRecommendation, SearchTask
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

    def run(self, research_id: str, prompt: str, tasks: list[SearchTask], depth) -> str:
        state: FinalizeGraphState = {
            "research_id": research_id,
            "prompt": prompt,
            "effective_prompt": prompt,
            "depth": depth,
            "tasks": tasks,
            "analyze_attempts": 0,
            "replan_attempts": 0,
            "tie_break_attempts": 0,
            "should_replan": False,
            "should_tie_break": False,
            "should_retry_analysis": False,
        }
        if StateGraph is not None:
            return self._run_langgraph(state)
        return self._run_fallback(state)

    def _collect_context(self, state: FinalizeGraphState) -> FinalizeGraphState:
        aggregated_sources = self.service._build_research_source_pool(state["tasks"])
        _, source_summary = self.service.source_critic.assess_sources(aggregated_sources)
        analyzer = self.service.analyzer
        detect_conflicts = getattr(analyzer, "_detect_conflicts", None) if self._supports_conflict_detection(analyzer) else None
        max_sources = getattr(analyzer, "MAX_ANALYZER_SOURCES", 24)
        if not isinstance(max_sources, int):
            max_sources = 24
        conflict_pool = self._build_conflict_pool(aggregated_sources[:max_sources])
        conflicts = detect_conflicts(conflict_pool) if callable(detect_conflicts) else []
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
        ) if self._supports_graph_branching(analyzer) else []
        should_replan = bool(recommendations) and state["replan_attempts"] < settings.langgraph_replan_max_loops
        return {
            **state,
            "detected_conflicts": conflicts,
            "source_summary": source_summary.model_dump(),
            "evidence_summary": evidence_summary.model_dump(),
            "replan_recommendations": [item.model_dump() for item in recommendations],
            "should_replan": should_replan,
        }

    def _build_conflict_pool(self, aggregated_sources: list[dict]) -> list[dict]:
        return [
            {
                "source_id": f"S{index}",
                "content": item.get("content", ""),
                "url": item.get("url"),
                "domain": item.get("domain"),
                "title": item.get("title"),
                "source_quality": item.get("source_quality"),
            }
            for index, item in enumerate(aggregated_sources, start=1)
            if item.get("content")
        ]

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

        created_tasks = self.service.execute_replan_search_pass(
            state["research_id"],
            state["depth"],
            [
                ReplanRecommendation.model_validate(recommendation)
                for recommendation in recommendations
            ],
        )
        return {
            **state,
            "effective_prompt": effective_prompt,
            "replan_attempts": state["replan_attempts"] + 1,
            "should_replan": False,
            "tasks": state["tasks"] + created_tasks,
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

    def _apply_tie_break(self, state: FinalizeGraphState) -> FinalizeGraphState:
        recommendations = state.get("tie_break_recommendations") or []
        if not recommendations:
            return {**state, "should_tie_break": False}

        query_hints: list[str] = []
        for recommendation in recommendations:
            query_hints.extend(recommendation.get("suggested_queries") or [])
        hint_block = "\n".join(f"- {query}" for query in query_hints[:6])
        effective_prompt = (
            f"{state['effective_prompt']}\n\n"
            "Tie-breaker evidence to resolve disputed or weakly supported points:\n"
            f"{hint_block}"
        )
        created_tasks = self.service.execute_replan_search_pass(
            state["research_id"],
            state["depth"],
            [ReplanRecommendation.model_validate(recommendation) for recommendation in recommendations],
        )
        return {
            **state,
            "effective_prompt": effective_prompt,
            "tie_break_attempts": state["tie_break_attempts"] + 1,
            "should_tie_break": False,
            "tasks": state["tasks"] + created_tasks,
        }

    def _verify(self, state: FinalizeGraphState) -> FinalizeGraphState:
        report = state.get("report") or ""
        should_retry = False
        weak_support = self._report_needs_retry(report)
        has_conflicts = bool(state.get("detected_conflicts"))
        should_tie_break = False
        if (
            self._supports_graph_branching(self.service.analyzer)
            and state["tie_break_attempts"] < settings.langgraph_tie_break_max_loops
            and (weak_support or has_conflicts)
        ):
            should_tie_break = True
        tie_break_recommendations = self.service.replan_agent.suggest_tie_breakers(
            state["prompt"],
            conflicts=state.get("detected_conflicts") or [],
            weak_support=weak_support,
        ) if should_tie_break else []
        if state["analyze_attempts"] <= settings.langgraph_verification_max_retries:
            should_retry = weak_support
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
            "tie_break_recommendations": [item.model_dump() for item in tie_break_recommendations],
            "should_tie_break": should_tie_break and bool(tie_break_recommendations),
            "should_retry_analysis": should_retry,
        }

    def _supports_graph_branching(self, analyzer) -> bool:
        return isinstance(analyzer, AnalyzerAgent) or getattr(analyzer, "enable_graph_branching", False) is True

    def _supports_conflict_detection(self, analyzer) -> bool:
        return isinstance(analyzer, AnalyzerAgent)

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
        if state.get("should_tie_break"):
            return "tie_break"
        return "analyze" if state.get("should_retry_analysis") else END

    def _run_fallback(self, state: FinalizeGraphState) -> str:
        state = self._collect_context(state)
        if state.get("should_replan"):
            state = self._apply_replan(state)
        state = self._analyze(state)
        state = self._verify(state)
        if state.get("should_tie_break"):
            state = self._apply_tie_break(state)
            state = self._collect_context(state)
            state = self._analyze(state)
            state = self._verify(state)
        if state.get("should_retry_analysis"):
            state = self._analyze(state)
        logger.info(
            "langgraph_finalize_runner_completed replan_attempts=%s tie_break_attempts=%s analyze_attempts=%s used_langgraph=%s",
            state.get("replan_attempts", 0),
            state.get("tie_break_attempts", 0),
            state.get("analyze_attempts", 0),
            False,
        )
        return state["report"]

    def _run_langgraph(self, state: FinalizeGraphState) -> str:  # pragma: no cover - optional dependency
        workflow = StateGraph(FinalizeGraphState)
        workflow.add_node("collect_context", self._collect_context)
        workflow.add_node("replan", self._apply_replan)
        workflow.add_node("tie_break", self._apply_tie_break)
        workflow.add_node("analyze", self._analyze)
        workflow.add_node("verify", self._verify)
        workflow.set_entry_point("collect_context")
        workflow.add_conditional_edges("collect_context", self._next_after_context, {"replan": "replan", "analyze": "analyze"})
        workflow.add_edge("replan", "analyze")
        workflow.add_edge("analyze", "verify")
        workflow.add_edge("tie_break", "collect_context")
        workflow.add_conditional_edges("verify", self._next_after_verify, {"tie_break": "tie_break", "analyze": "analyze", END: END})
        compiled = workflow.compile()
        result = compiled.invoke(state)
        logger.info(
            "langgraph_finalize_runner_completed replan_attempts=%s tie_break_attempts=%s analyze_attempts=%s used_langgraph=%s",
            result.get("replan_attempts", 0),
            result.get("tie_break_attempts", 0),
            result.get("analyze_attempts", 0),
            True,
        )
        return result["report"]
