from __future__ import annotations

import inspect
import logging
import time
from time import perf_counter

from langgraph.graph import END, StateGraph

from src.agents.analyzer import AnalyzerAgent
from src.agents.cross_language import detect_language
from src.domain import ReplanRecommendation, ResearchStatus, SearchDepth, SearchTask
from src.config import settings
from src.graph.metrics import (
    record_graph_analyze,
    record_graph_completed_run,
    record_graph_replan,
    record_graph_resume,
    record_graph_step,
    record_graph_step_failure,
    record_graph_tie_break,
)
from src.graph.state import FinalizeGraphState

logger = logging.getLogger(__name__)


class FinalizeCancelled(RuntimeError):
    """Raised between graph steps after a user cancels the research."""


class FinalizeLeaseLost(RuntimeError):
    """Raised when stale-job recovery fences off a previous finalize runner."""


GRAPH_STEP_METADATA: dict[str, dict[str, Any]] = {
    "collect_context": {
        "agent": "SourceCriticAgent",
        "phase": "critic",
        "action": "evaluate_sources",
        "detail": "Оценка достоверности источников, структурирование доказательств и выявление белых пятен",
    },
    "replan": {
        "agent": "ReplanAgent",
        "phase": "plan",
        "action": "gap_analysis_loop",
        "detail": "↩ Обнаружены пробелы в данных: возврат на допоиск источников для полноты картины",
    },
    "analyze": {
        "agent": "AnalyzerAgent",
        "phase": "synthesis",
        "action": "synthesize_report",
        "detail": "Глубокий синтез аналитического отчёта, сведение фактов и разметка цитат",
    },
    "tie_break": {
        "agent": "ReplanAgent",
        "phase": "critic",
        "action": "conflict_tie_break",
        "detail": "↩ Обнаружены противоречия между источниками: запуск арбитражного поиска (Tie-Break)",
    },
    "verify": {
        "agent": "ReportCriticAgent",
        "phase": "verify",
        "action": "verify_claims",
        "detail": "Верификация утверждений отчёта, контроль точности цитирования и рецензирование",
    },
    "verify_retry": {
        "agent": "ReportCriticAgent",
        "phase": "verify",
        "action": "critic_revision_loop",
        "detail": "↩ Рецензент вернул отчёт на доработку в AnalyzerAgent: устранение слабых мест и усиление доказательств",
    },
}


class FinalizeGraphRunner:
    def __init__(self, service):
        self.service = service

    def run(
        self,
        research_id: str,
        prompt: str,
        tasks: list[SearchTask],
        depth,
        finalize_job_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> tuple[str, list[dict] | None, str, list[SearchTask]]:
        state = self._build_initial_state(
            research_id,
            prompt,
            tasks,
            depth,
            finalize_job_id=finalize_job_id,
            lease_epoch=lease_epoch,
        )
        # One encoding for every run (GRAPH-TOPOLOGY): fresh and resumed executions
        # walk the SAME langgraph topology — resume just enters at a different node,
        # so a run's behavior no longer depends on whether a worker crashed.
        if state.get("resume_from_step") == "complete":
            logger.info("langgraph_finalize_resume_complete_noop")
            return self._result_from_state(state)
        return self._run_langgraph(state)

    def _build_initial_state(
        self,
        research_id: str,
        prompt: str,
        tasks: list[SearchTask],
        depth,
        *,
        finalize_job_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> FinalizeGraphState:
        state: FinalizeGraphState = {
            "research_id": research_id,
            "prompt": prompt,
            "effective_prompt": prompt,
            "depth": depth,
            "tasks": tasks,
            "analyze_attempts": 0,
            "replan_attempts": 0,
            "tie_break_attempts": 0,
            "finalize_deadline": time.time() + settings.finalize_budget_max_seconds,
            "should_replan": False,
            "should_tie_break": False,
            "should_retry_analysis": False,
        }
        if finalize_job_id is not None and lease_epoch is not None:
            state["finalize_job_id"] = finalize_job_id
            state["finalize_lease_epoch"] = lease_epoch
        research = self.service.task_store.get_research(research_id)
        graph_state = (research.graph_state if research else None) or {}
        # User-selected model (persisted at creation); carried through the graph runtime.
        state["model"] = graph_state.get("model")
        stored_language = getattr(research, "language", None)
        state["language"] = (
            stored_language
            if stored_language and stored_language != "unknown"
            else detect_language(prompt)
        )
        step = graph_state.get("step")
        if not step:
            return state
        if step == "complete":
            saved_report = graph_state.get("report") or ""
            if saved_report:
                logger.info("langgraph_finalize_already_complete skip_rerun research_id=%s", research_id)
                return {
                    **state,
                    "effective_prompt": graph_state.get("effective_prompt") or prompt,
                    "canonical_sources": graph_state.get("canonical_sources") or [],
                    "report": saved_report,
                    "resume_from_step": "complete",
                }
            return state

        resumed_state = {
            **state,
            "effective_prompt": graph_state.get("effective_prompt") or prompt,
            "analyze_attempts": int(graph_state.get("analyze_attempts") or 0),
            "replan_attempts": int(graph_state.get("replan_attempts") or 0),
            "tie_break_attempts": int(graph_state.get("tie_break_attempts") or 0),
            "should_replan": bool(graph_state.get("should_replan")),
            "should_tie_break": bool(graph_state.get("should_tie_break")),
            "should_retry_analysis": bool(graph_state.get("should_retry_analysis")),
            "replan_recommendations": graph_state.get("replan_recommendations") or [],
            "tie_break_recommendations": graph_state.get("tie_break_recommendations") or [],
            "detected_conflicts": graph_state.get("detected_conflicts") or [],
            "source_summary": graph_state.get("source_summary") or {},
            "evidence_summary": graph_state.get("evidence_summary") or {},
            "canonical_sources": graph_state.get("canonical_sources") or [],
            "report": graph_state.get("report") or "",
            "resume_from_step": step,
        }
        logger.info("langgraph_finalize_resume step=%s", step)
        record_graph_resume()
        return resumed_state

    def _checkpoint(self, state: FinalizeGraphState, step: str, detail: str) -> None:
        self.service.ensure_finalize_job_lease(
            state.get("finalize_job_id"),
            state.get("finalize_lease_epoch"),
        )
        snapshot = {
            "step": step,
            "prompt": state.get("prompt"),
            "effective_prompt": state.get("effective_prompt"),
            "depth": getattr(state.get("depth"), "value", state.get("depth")),
            "model": state.get("model"),
            "task_ids": [task.id for task in state.get("tasks", [])],
            "analyze_attempts": state.get("analyze_attempts", 0),
            "replan_attempts": state.get("replan_attempts", 0),
            "tie_break_attempts": state.get("tie_break_attempts", 0),
            "should_replan": state.get("should_replan", False),
            "should_tie_break": state.get("should_tie_break", False),
            "should_retry_analysis": state.get("should_retry_analysis", False),
            "replan_recommendations": state.get("replan_recommendations", []),
            "tie_break_recommendations": state.get("tie_break_recommendations", []),
            "detected_conflicts": state.get("detected_conflicts", []),
            "source_summary": state.get("source_summary", {}),
            "evidence_summary": state.get("evidence_summary", {}),
            "canonical_sources": state.get("canonical_sources", []),
            "report": state.get("report", ""),
        }
        meta = GRAPH_STEP_METADATA.get(step, {})
        event = {
            "step": step,
            "agent": meta.get("agent", "FinalizeRunner"),
            "phase": meta.get("phase", "synthesis"),
            "action": meta.get("action", step),
            "detail": detail,
        }
        self.service.checkpoint_graph_state(state["research_id"], snapshot, event)

    def _emit_trail(
        self,
        research_id: str,
        step: str,
        agent: str | None = None,
        phase: str | None = None,
        action: str | None = None,
        detail: str | None = None,
        metrics: dict | None = None,
    ) -> None:
        """Surface this finalize step on the live progress trail (streamed via SSE) so the
        trace keeps moving during synthesis instead of freezing after the search phase.
        Step names reuse the existing trace.* i18n labels (collect_context/analyze/…)."""
        store = getattr(self.service, "task_store", None)
        if not research_id or store is None or not hasattr(store, "append_research_graph_event"):
            return
        try:
            meta = GRAPH_STEP_METADATA.get(step, {})
            event = {
                "step": step,
                "agent": agent or meta.get("agent", "FinalizeRunner"),
                "phase": phase or meta.get("phase", "synthesis"),
                "action": action or meta.get("action", step),
                "detail": detail or meta.get("detail", ""),
            }
            if metrics:
                event["metrics"] = metrics
            store.append_research_graph_event(research_id, event)
        except Exception:  # progress events must never break finalize
            pass

    def _run_timed_step(self, step_name: str, action, research_id: str):
        research = self.service.task_store.get_research(research_id)
        if research is not None and research.status == ResearchStatus.CANCELLED:
            logger.info("langgraph_finalize_cancelled_before_step step=%s", step_name)
            raise FinalizeCancelled(research_id)
        self._emit_trail(research_id, step_name)
        started_at = perf_counter()
        try:
            result = action()
        except Exception:
            elapsed_ms = (perf_counter() - started_at) * 1000
            record_graph_step_failure(step_name, elapsed_ms, research_id=research_id)
            logger.exception("langgraph_finalize_step_failed step=%s elapsed_ms=%.2f", step_name, elapsed_ms)
            raise
        elapsed_ms = (perf_counter() - started_at) * 1000
        record_graph_step(step_name, elapsed_ms, research_id=research_id)
        logger.info("langgraph_finalize_step_completed step=%s elapsed_ms=%.2f", step_name, elapsed_ms)
        return result

    def _budget_ok(self, state: FinalizeGraphState) -> bool:
        """True while the finalize budget (wall-clock) still has room for another branch/pass."""
        deadline = state.get("finalize_deadline")
        return deadline is None or time.time() < deadline

    def _is_deep_loop(self, state: FinalizeGraphState) -> bool:
        """Only HARD runs the extra deep-research branches (replan / tie-break / retry).

        EASY/MEDIUM finalize in a single analyze pass — the deep loop adds several more
        multi-minute LLM calls + extra searches, which is far too slow for light depths.
        """
        depth = state.get("depth")
        return getattr(depth, "value", depth) == SearchDepth.HARD.value

    def _pool_size(self, tasks) -> int:
        """Count of unique (deduped-by-URL) sources across tasks — for no-progress detection."""
        return len(self.service._build_research_source_pool(tasks))

    def _collect_context(self, state: FinalizeGraphState) -> FinalizeGraphState:
        def action() -> FinalizeGraphState:
            aggregated_sources = self.service._build_research_source_pool(state["tasks"])
            _, source_summary = self.service.source_critic.assess_sources(aggregated_sources)
            analyzer = self.service.analyzer
            _, evidence_summary = self.service.evidence_mapper.build_evidence_groups(
                aggregated_sources,
                max_groups=5,
            )
            branch_possible = (
                self._supports_graph_branching(analyzer)
                and self._is_deep_loop(state)
                and state["replan_attempts"] < settings.langgraph_replan_max_loops
                and self._budget_ok(state)
                and not state.get("branch_stalled")
            )
            recommendations = self.service.replan_agent.suggest_follow_up(
                state["prompt"],
                state["depth"],
                state["tasks"],
                source_summary=source_summary,
            ) if branch_possible else []
            should_replan = branch_possible and bool(recommendations)
            next_state = {
                **state,
                "source_summary": source_summary.model_dump(),
                "evidence_summary": evidence_summary.model_dump(),
                "replan_recommendations": [item.model_dump() for item in recommendations],
                "should_replan": should_replan,
            }
            self._checkpoint(
                next_state,
                "collect_context",
                f"Collected {len(aggregated_sources)} sources, replan_needed={should_replan}",
            )
            return next_state

        return self._run_timed_step("collect_context", action, state["research_id"])

    def _apply_replan(self, state: FinalizeGraphState) -> FinalizeGraphState:
        def action() -> FinalizeGraphState:
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
            combined_tasks = state["tasks"] + created_tasks
            # No-progress: if the follow-up wave added no new unique sources, stop further branching.
            stalled = self._pool_size(combined_tasks) <= self._pool_size(state["tasks"])
            next_state = {
                **state,
                "effective_prompt": effective_prompt,
                "replan_attempts": state["replan_attempts"] + 1,
                "should_replan": False,
                "tasks": combined_tasks,
                "branch_stalled": stalled,
            }
            self._checkpoint(
                next_state,
                "replan",
                f"Created {len(created_tasks)} follow-up tasks from {len(recommendations)} recommendations",
            )
            record_graph_replan()
            return next_state

        return self._run_timed_step("replan", action, state["research_id"])

    def _supported_analysis_kwargs(self, candidate: dict) -> dict:
        """Filter ``candidate`` kwargs down to those ``run_analysis`` accepts.

        AnalyzerAgent accepts all of them; minimal analyzers (stub/static) may not.
        """
        run = getattr(self.service.analyzer, "run_analysis", None)
        try:
            params = inspect.signature(run).parameters
        except (TypeError, ValueError):
            return {}
        accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        if accepts_var_kw:
            return dict(candidate)
        return {key: value for key, value in candidate.items() if key in params}

    def _analyze(self, state: FinalizeGraphState) -> FinalizeGraphState:
        def action() -> FinalizeGraphState:
            _last_len = [0]
            _last_rlen = [0]

            def _streaming_callback(partial: str) -> None:
                # Save every +500 chars; also save on a "restart" (partial shorter than
                # last) so the streamed synthesis replaces the section drafts live.
                if len(partial) < _last_len[0] or len(partial) - _last_len[0] >= 500:
                    _last_len[0] = len(partial)
                    self.service.task_store.save_partial_report(state["research_id"], partial)

            def _reasoning_callback(partial: str) -> None:
                if len(partial) - _last_rlen[0] >= 300:
                    _last_rlen[0] = len(partial)
                    save = getattr(self.service.task_store, "save_partial_reasoning", None)
                    if save is not None:
                        save(state["research_id"], partial)

            # Only pass kwargs the analyzer actually accepts — keeps the runner
            # compatible with minimal analyzers (stub/static) that omit them.
            call_kwargs = self._supported_analysis_kwargs(
                {
                    "depth": state["depth"],
                    "model": state.get("model"),
                    "streaming_callback": _streaming_callback,
                    "reasoning_callback": _reasoning_callback,
                    "language": state.get("language"),
                }
            )
            analysis_result = self.service.analyzer.run_analysis(
                state["effective_prompt"],
                state["tasks"],
                **call_kwargs,
            )
            if (
                isinstance(analysis_result, tuple)
                and len(analysis_result) in {2, 3}
                and isinstance(analysis_result[1], list)
            ):
                report, aggregated_data = analysis_result[:2]
                canonical_sources = self.service._canonical_source_table(aggregated_data)
                detected_conflicts = (
                    analysis_result[2]
                    if len(analysis_result) == 3 and isinstance(analysis_result[2], list)
                    else state.get("detected_conflicts", [])
                )
            else:
                report = analysis_result
                aggregated_data = state.get("aggregated_data")
                canonical_sources = state.get("canonical_sources", [])
                detected_conflicts = state.get("detected_conflicts", [])
            next_state = {
                **state,
                "report": report,
                "aggregated_data": aggregated_data,
                "canonical_sources": canonical_sources,
                "detected_conflicts": detected_conflicts,
                "analyze_attempts": state["analyze_attempts"] + 1,
            }
            self._checkpoint(
                next_state,
                "analyze",
                f"Analyzer run completed. analyze_attempt={next_state['analyze_attempts']}",
            )
            record_graph_analyze()
            return next_state

        return self._run_timed_step("analyze", action, state["research_id"])

    def _apply_tie_break(self, state: FinalizeGraphState) -> FinalizeGraphState:
        def action() -> FinalizeGraphState:
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
            combined_tasks = state["tasks"] + created_tasks
            stalled = self._pool_size(combined_tasks) <= self._pool_size(state["tasks"])
            next_state = {
                **state,
                "effective_prompt": effective_prompt,
                "tie_break_attempts": state["tie_break_attempts"] + 1,
                "should_tie_break": False,
                "tasks": combined_tasks,
                "branch_stalled": stalled,
            }
            self._checkpoint(
                next_state,
                "tie_break",
                f"Created {len(created_tasks)} tie-break tasks from {len(recommendations)} recommendations",
            )
            record_graph_tie_break()
            return next_state

        return self._run_timed_step("tie_break", action, state["research_id"])

    def _verify(self, state: FinalizeGraphState) -> FinalizeGraphState:
        def action() -> FinalizeGraphState:
            report = state.get("report") or ""
            should_retry = False
            weak_support = self._report_needs_retry(report)
            has_conflicts = bool(state.get("detected_conflicts"))
            should_tie_break = False
            if (
                self._supports_graph_branching(self.service.analyzer)
                and self._is_deep_loop(state)
                and state["tie_break_attempts"] < settings.langgraph_tie_break_max_loops
                and (weak_support or has_conflicts)
                and self._budget_ok(state)
                and not state.get("branch_stalled")
            ):
                should_tie_break = True
            tie_break_recommendations = self.service.replan_agent.suggest_tie_breakers(
                state["prompt"],
                conflicts=state.get("detected_conflicts") or [],
                weak_support=weak_support,
            ) if should_tie_break else []
            if (
                self._is_deep_loop(state)
                and state["analyze_attempts"] <= settings.langgraph_verification_max_retries
                and state["analyze_attempts"] < settings.finalize_budget_max_analyze_passes
                and self._budget_ok(state)
            ):
                should_retry = weak_support
            if should_retry:
                effective_prompt = (
                    f"{state['effective_prompt']}\n\n"
                    "The previous draft still had report notes or weak-support issues. "
                    "Prioritize higher-confidence evidence, reduce overconfident wording, and improve citation discipline."
                )
                self._emit_trail(
                    state["research_id"],
                    "verify_retry",
                    agent="ReportCriticAgent",
                    phase="verify",
                    action="critic_revision_loop",
                    detail="↩ Рецензент вернул отчёт на доработку в AnalyzerAgent: устранение слабых мест и усиление доказательств",
                    metrics={"attempt": state["analyze_attempts"] + 1},
                )
            elif should_tie_break and tie_break_recommendations:
                effective_prompt = state["effective_prompt"]
                self._emit_trail(
                    state["research_id"],
                    "tie_break",
                    agent="ReplanAgent",
                    phase="critic",
                    action="conflict_tie_break",
                    detail=f"↩ Обнаружены противоречия в источниках ({len(state.get('detected_conflicts') or [])}): запуск арбитражного поиска (Tie-Break)",
                    metrics={"recommendations": len(tie_break_recommendations)},
                )
            else:
                effective_prompt = state["effective_prompt"]
            next_state = {
                **state,
                "effective_prompt": effective_prompt,
                "tie_break_recommendations": [item.model_dump() for item in tie_break_recommendations],
                "should_tie_break": should_tie_break and bool(tie_break_recommendations),
                "should_retry_analysis": should_retry,
            }
            self._checkpoint(
                next_state,
                "verify",
                f"weak_support={weak_support} conflicts={len(state.get('detected_conflicts') or [])} retry={should_retry} tie_break={next_state['should_tie_break']}",
            )
            return next_state

        return self._run_timed_step("verify", action, state["research_id"])

    def _supports_graph_branching(self, analyzer) -> bool:
        return isinstance(analyzer, AnalyzerAgent) or getattr(analyzer, "enable_graph_branching", False) is True

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

    def _resume_entry(self, state: FinalizeGraphState) -> str:
        """First node for a resumed run, derived from the last checkpointed step.

        Each checkpoint marks a COMPLETED step, so resume re-enters at that step's
        successor — expressed as routing inside the same graph instead of a
        separate sequential runner."""
        step = state.get("resume_from_step")
        if not step or step == "collect_context":
            return "collect_context"
        if step == "replan":
            return "analyze"
        if step == "analyze":
            # The analyze checkpoint carries the report — skip straight to verify.
            return "verify" if state.get("report") else "analyze"
        if step == "verify":
            if state.get("should_tie_break"):
                return "tie_break"
            if state.get("should_retry_analysis"):
                return "analyze"
            return END if state.get("report") else "analyze"
        if step == "tie_break":
            return "collect_context"
        return "collect_context"

    @staticmethod
    def _resume_route(state: FinalizeGraphState) -> FinalizeGraphState:
        return state  # pass-through: the conditional edge does the routing

    def _result_from_state(
        self,
        state: FinalizeGraphState,
    ) -> tuple[str, list[dict] | None, str, list[SearchTask]]:
        aggregated_data = state.get("aggregated_data")
        if aggregated_data is None:
            research = self.service.task_store.get_research(state["research_id"])
            if research is not None:
                aggregated_data = self.service._aggregated_sources(research, state["tasks"])
        return (
            state.get("report", ""),
            aggregated_data,
            state.get("effective_prompt") or state["prompt"],
            state["tasks"],
        )

    def _complete_run(
        self,
        state: FinalizeGraphState,
    ) -> tuple[str, list[dict] | None, str, list[SearchTask]]:
        """Log completion metrics, checkpoint, and return the final synthesis inputs."""
        logger.info(
            "langgraph_finalize_runner_completed replan_attempts=%s tie_break_attempts=%s analyze_attempts=%s",
            state.get("replan_attempts", 0),
            state.get("tie_break_attempts", 0),
            state.get("analyze_attempts", 0),
        )
        self._checkpoint(
            state,
            "complete",
            f"Finalize graph completed with {state.get('analyze_attempts', 0)} analyze passes",
        )
        record_graph_completed_run()
        return self._result_from_state(state)

    # ── execution ─────────────────────────────────────────────────────────────

    def _run_langgraph(
        self,
        state: FinalizeGraphState,
    ) -> tuple[str, list[dict] | None, str, list[SearchTask]]:
        workflow = StateGraph(FinalizeGraphState)
        workflow.add_node("resume_route", self._resume_route)
        workflow.add_node("collect_context", self._collect_context)
        workflow.add_node("replan", self._apply_replan)
        workflow.add_node("tie_break", self._apply_tie_break)
        workflow.add_node("analyze", self._analyze)
        workflow.add_node("verify", self._verify)
        workflow.set_entry_point("resume_route")
        # Fresh runs route to collect_context; resumed runs enter at the successor
        # of their last checkpointed step — same topology either way.
        workflow.add_conditional_edges(
            "resume_route",
            self._resume_entry,
            {
                "collect_context": "collect_context",
                "analyze": "analyze",
                "verify": "verify",
                "tie_break": "tie_break",
            },
        )
        workflow.add_conditional_edges("collect_context", self._next_after_context, {"replan": "replan", "analyze": "analyze"})
        workflow.add_edge("replan", "analyze")
        workflow.add_edge("analyze", "verify")
        workflow.add_edge("tie_break", "collect_context")
        workflow.add_conditional_edges("verify", self._next_after_verify, {"tie_break": "tie_break", "analyze": "analyze", END: END})
        compiled = workflow.compile()
        result = compiled.invoke(state)
        return self._complete_run(result)
