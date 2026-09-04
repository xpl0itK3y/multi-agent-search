import json
import logging
import time
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from src.domain.errors import ConflictError, NotFoundError, ServiceUnavailableError

from src.agents.analyzer import AnalyzerAgent
from src.agents.claim_verifier import ClaimVerifierAgent
from src.agents.report_critic import ReportCriticAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.replan import ReplanAgent
from src.agents.citation_audit import CitationAuditAgent
from src.agents.source_independence import SourceIndependenceAgent
from src.agents.source_reputation import SourceReputationAgent
from src.agents.retraction import RetractionAgent
from src.agents.cross_language import CrossLanguageAgent, detect_language
from src.agents.numeric_check import NumericCheckAgent
from src.agents.confidence import ConfidenceAgent
from src.agents.search import SearchAgent
from src.agents.source_critic import SourceCriticAgent
from src.brokers.redis_broker import RedisBroker
from src.services.auth_mixin import AuthMixin
from src.services.operational_health_mixin import OperationalHealthMixin
from src.services.export_mixin import ExportMixin
from src.services.job_queue_mixin import JobQueueMixin
from src.services.trust_report_mixin import TrustReportMixin
from src.services.share_mixin import ShareMixin
from src.domain import (
    AuthUser,
    JobCleanupResponse,
    MaintenanceSummary,
    OperationalHealth,
    DecomposeResponse,
    FinalizeJobStatus,
    GraphAlert,
    GraphAlertHistoryEntry,
    GraphAlertTrend,
    GraphMetrics,
    JobRecoveryResponse,
    QueueMetrics,
    QueueMaintenanceResponse,
    ResearchHistoryItem,
    ResearchRecord,
    ResearchGraphResponse,
    ResearchRequest,
    ResearchResponse,
    ResearchReportResponse,
    ChatMessage,
    Clarification,
    ResearchConflict,
    ResearchPlan,
    CitationAudit,
    SourceIndependence,
    SourceReputation,
    SourceIntegrity,
    CrossLanguageReport,
    LanguageCount,
    StanceBalance,
    NumericCheck,
    ConfidenceReport,
    AuditTrail,
    ShareInfo,
    PublicReport,
    AuditQuery,
    AuditSource,
    AuditStep,
    ComparisonTable,
    RedTeamReport,
    ResearchPlanItem,
    ResearchPlanUpdate,
    ResearchSummary,
    VerificationReport,
    ResearchStatusSummary,
    ResearchStatus,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchSourcePreview,
    SearchTaskJob,
    SearchTaskSummary,
    SearchDepth,
    SearchTask,
    TaskUpdate,
    TaskStatus,
    WorkerHeartbeat,
    ReplanRecommendation,
)
from src.config import settings
from src.graph import FinalizeCancelled, FinalizeGraphRunner
from src.model_catalog import resolve_model_id
from src.graph.metrics import get_graph_metrics_snapshot, get_graph_step_events_snapshot
from src.observability import bind_observability_context, set_queue_metrics
from src.providers.search import get_extraction_metrics_snapshot
from src.repositories.protocols import TaskStore
from src.search_depth_profiles import get_depth_profile

logger = logging.getLogger(__name__)


class ResearchService(
    AuthMixin, OperationalHealthMixin, ExportMixin, JobQueueMixin, TrustReportMixin, ShareMixin
):
    TASK_SUMMARY_LOG_LIMIT = 6
    TASK_SUMMARY_SOURCE_LIMIT = 4
    GRAPH_STEP_WARNING_MS = 1500.0
    GRAPH_STEP_CRITICAL_MS = 5000.0
    GRAPH_STEP_FAILURE_WARNING_COUNT = 1
    GRAPH_STEP_FAILURE_CRITICAL_COUNT = 3
    GRAPH_ANALYZE_RETRY_WARNING_COUNT = 3
    GRAPH_ANALYZE_RETRY_CRITICAL_COUNT = 6
    MAINTENANCE_GROWING_WARNING_RECENT_AVG = 5.0
    MAINTENANCE_GROWING_CRITICAL_RECENT_AVG = 10.0
    MAINTENANCE_COMPACTED_WARNING_AVG = 3.0
    MAINTENANCE_COMPACTED_CRITICAL_AVG = 8.0
    MAINTENANCE_STALE_WARNING_SECONDS = 1800
    MAINTENANCE_STALE_CRITICAL_SECONDS = 7200
    RUNBOOK_UNRESOLVED_WARNING_COUNT = 3
    RUNBOOK_UNRESOLVED_CRITICAL_COUNT = 6
    RUNBOOK_RESOLUTION_WARNING_HOURS = 6.0
    RUNBOOK_RESOLUTION_CRITICAL_HOURS = 24.0
    RUNBOOK_REAPPEARED_WARNING_COUNT = 2
    RUNBOOK_REAPPEARED_CRITICAL_COUNT = 4
    OPERATIONAL_WORSENING_WARNING_DELTA = 8.0
    OPERATIONAL_WORSENING_CRITICAL_DELTA = 18.0
    OPERATIONAL_CRITICAL_STATE_WARNING_COUNT = 2
    OPERATIONAL_CRITICAL_STATE_CRITICAL_COUNT = 3
    OPERATIONAL_RECOMMENDATION_EVENT_LIMIT = 40

    def __init__(
        self,
        task_store: TaskStore,
        optimizer: PromptOptimizerAgent | None = None,
        orchestrator: OrchestratorAgent | None = None,
        analyzer: AnalyzerAgent | None = None,
        source_critic: SourceCriticAgent | None = None,
        evidence_mapper: EvidenceMapperAgent | None = None,
        claim_verifier: ClaimVerifierAgent | None = None,
        report_critic: ReportCriticAgent | None = None,
        replan_agent: ReplanAgent | None = None,
        chat_agent=None,
        clarifier=None,
        red_team_agent=None,
        comparison_agent=None,
        stance_agent=None,
        cross_language_agent=None,
        broker: RedisBroker | None = None,
    ):
        self.task_store = task_store
        self.optimizer = optimizer
        self.orchestrator = orchestrator
        self.analyzer = analyzer
        self.source_critic = source_critic or SourceCriticAgent()
        self.evidence_mapper = evidence_mapper or EvidenceMapperAgent()
        self.claim_verifier = claim_verifier or ClaimVerifierAgent()
        self.report_critic = report_critic or ReportCriticAgent()
        self.replan_agent = replan_agent or ReplanAgent()
        self.chat_agent = chat_agent
        self.clarifier = clarifier
        self.red_team_agent = red_team_agent
        self.comparison_agent = comparison_agent
        self.stance_agent = stance_agent
        self.cross_language_agent = cross_language_agent
        self.citation_auditor = CitationAuditAgent()
        self.independence_auditor = SourceIndependenceAgent()
        self.reputation_auditor = SourceReputationAgent()
        self.retraction_agent = RetractionAgent()
        self._crossref_cache: dict[str, dict | None] = {}
        self.numeric_checker = NumericCheckAgent()
        self.confidence_agent = ConfidenceAgent()
        self.broker = broker
        self.finalize_graph_runner = FinalizeGraphRunner(self)

    # ── auth ──────────────────────────────────────────────────────────────────
    # (auth methods extracted to AuthMixin — src/services/auth_mixin.py)

    def require_agent(self, agent, agent_name: str):
        if agent is None:
            raise ServiceUnavailableError(
                f"{agent_name} is unavailable. Check service configuration."
            )
        return agent

    def optimize_prompt(self, prompt: str) -> str:
        optimizer = self.require_agent(self.optimizer, "Prompt optimizer")
        return optimizer.run(prompt)

    def list_tasks(self, limit: int = 100, offset: int = 0) -> list[SearchTask]:
        return self.task_store.get_all_tasks(limit=limit, offset=offset)

    def get_task(self, task_id: str) -> SearchTask | None:
        return self.task_store.get_task(task_id)

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        return self.task_store.update_task(task_id, update)

    def decompose_prompt(
        self,
        prompt: str,
        depth: SearchDepth,
    ) -> DecomposeResponse:
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        tasks_raw = orchestrator.run_decompose(prompt, depth)

        registered_tasks = []
        for task_dict in tasks_raw:
            task = self.task_store.add_task(task_dict)
            registered_tasks.append(task)
            if task.status == TaskStatus.PENDING and task.queries:
                job = self.task_store.add_search_task_job(task.id, depth.value, settings.job_max_attempts)
                if self.broker:
                    self.broker.push_search_job(job.id)

        return DecomposeResponse(tasks=registered_tasks, depth=depth)

    # Researches actively consuming search/LLM resources (vs. terminal or waiting on user).
    _RUNNING_STATUSES = {ResearchStatus.PROCESSING, ResearchStatus.ANALYZING}
    # A user's "in flight" research includes a queued one, so the per-user guard counts it.
    _IN_FLIGHT_STATUSES = _RUNNING_STATUSES | {ResearchStatus.QUEUED}

    def _count_active(self, user_id: str | None, statuses: set) -> int:
        # Only count researches still making progress; a stalled one (dead worker /
        # hung provider) must not lock the user (or the global slot) out forever.
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.research_stale_active_seconds)
        count = 0
        for item in self.task_store.list_researches(limit=200, user_id=user_id):
            if item.status not in statuses:
                continue
            # Queued items never go stale — they're waiting, not running.
            if item.status == ResearchStatus.QUEUED:
                count += 1
                continue
            updated = item.updated_at
            if updated is not None and updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if updated is None or updated >= cutoff:
                count += 1
        return count

    def _active_research_count(self, user_id: str | None) -> int:
        return self._count_active(user_id, self._IN_FLIGHT_STATUSES)

    def _global_running_count(self) -> int:
        """Researches actively RUNNING across all users (excludes queued) — for admission."""
        return self._count_active(None, self._RUNNING_STATUSES)

    def _queued_ordered(self) -> list:
        """All QUEUED researches across users, oldest-first (FIFO order)."""
        queued = [
            item for item in self.task_store.list_researches(limit=200, user_id=None)
            if item.status == ResearchStatus.QUEUED
        ]
        return sorted(queued, key=lambda r: r.created_at)

    def _queue_position(self, research_id: str) -> int | None:
        for index, item in enumerate(self._queued_ordered(), start=1):
            if item.id == research_id:
                return index
        return None

    def promote_queued_researches(self) -> int:
        """Start queued researches (oldest first) while running slots are free under the cap."""
        cap = settings.max_global_active_researches
        if cap <= 0:
            return 0
        import threading as _threading

        promoted = 0
        for item in self._queued_ordered():
            if self._global_running_count() >= cap:
                break
            # Atomic claim: only the process that flips QUEUED->PROCESSING runs decompose,
            # so concurrent workers/API processes never double-promote the same research.
            if not self.task_store.try_claim_queued_research(item.id):
                continue
            research = self.task_store.get_research(item.id)
            payload = (research.graph_state or {}).get("decompose_payload") if research else None
            if not payload:
                self.task_store.update_research_status(item.id, ResearchStatus.FAILED, "Queued research had no plan to run.")
                continue
            try:
                request = ResearchRequest.model_validate(payload)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("promote_payload_invalid research_id=%s error=%s", item.id, exc)
                self.task_store.update_research_status(item.id, ResearchStatus.FAILED, "Queued research plan was invalid.")
                continue
            _threading.Thread(
                target=self.decompose_and_enqueue, args=(item.id, request),
                daemon=True, name=f"promote-{item.id[:8]}",
            ).start()
            promoted += 1
            logger.info("research_promoted_from_queue research_id=%s", item.id)
        return promoted

    def start_research(
        self, request: ResearchRequest, user_id: str | None = None
    ) -> tuple["ResearchResponse", str]:
        """Create research record immediately and return. Decompose runs in background."""
        limit = settings.max_concurrent_researches
        if limit > 0 and self._active_research_count(user_id) >= limit:
            raise ConflictError(
                "A research is already in progress. Please wait for it to finish before starting another."
            )
        # Global admission control: queue the research when the system is at capacity.
        global_cap = settings.max_global_active_researches
        queued = global_cap > 0 and self._global_running_count() >= global_cap

        research = self.task_store.add_research(request, task_ids=[], user_id=user_id)
        logger.info("research_created research_id=%s depth=%s queued=%s", research.id, request.depth.value, queued)
        # Persist decompose intent + webhook_url together so crash-recovery can retry (R-1).
        # update_research_graph_state replaces the entire dict, so merge everything in one call.
        # Group researches into a conversation thread; a new id starts a new thread.
        thread_id = (request.thread_id or "").strip() or str(uuid.uuid4())
        graph_state: dict = {
            "decompose_pending": True,
            "decompose_payload": request.model_dump(mode="json"),
            # Persist the validated model choice (falls back to default if unknown/unsafe).
            "model": resolve_model_id(request.model, settings.deepseek_model),
            "thread_id": thread_id,
        }
        if request.webhook_url:
            graph_state["webhook_url"] = str(request.webhook_url)
        self.task_store.update_research_graph_state(research.id, graph_state)
        if queued:
            # Park it; decompose_and_enqueue will defer, and promote_queued_researches
            # will start it when a slot frees up.
            self.task_store.update_research_status(research.id, ResearchStatus.QUEUED)
        return ResearchResponse(
            research_id=research.id,
            status="queued" if queued else "success",
            message="Queued — the system is at capacity; it will start automatically." if queued
            else "Research created. Task decomposition in progress…",
            thread_id=thread_id,
        ), research.id

    def decompose_and_enqueue(self, research_id: str, request: ResearchRequest) -> None:
        """Background: run LLM decompose, create tasks, push to worker queue."""
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        with bind_observability_context(research_id=research_id):
            try:
                research = self.task_store.get_research(research_id)
                # Admission control: a queued research waits for promote_queued_researches.
                if research is not None and research.status == ResearchStatus.QUEUED:
                    logger.info("decompose_deferred_queued research_id=%s", research_id)
                    return
                # User cancelled before decompose ran — don't spend the LLM call or enqueue jobs.
                if research is not None and research.status in self._TERMINAL_STATUSES:
                    logger.info("decompose_skipped_terminal research_id=%s status=%s", research_id, research.status.value)
                    return
                graph_state = (research.graph_state if research else None) or {}
                # Clarify step (plan-first only, once): ask up to 3 questions before planning.
                if (
                    request.plan_first
                    and self.clarifier is not None
                    and not graph_state.get("clarified")
                ):
                    questions = self.clarifier.generate_questions(request.prompt)
                    if questions:
                        self._store_clarifications_for_review(research_id, questions)
                        logger.info(
                            "research_clarify_ready research_id=%s question_count=%s",
                            research_id, len(questions),
                        )
                        return
                effective_prompt = self._augment_prompt_with_clarifications(request.prompt, graph_state)
                tasks_raw = orchestrator.run_decompose(effective_prompt, request.depth)
                self._maybe_add_cross_language_task(research_id, effective_prompt, tasks_raw)
                # Cancellation can land while decompose was running — re-check before we
                # create tasks and flood the queue with search jobs.
                current = self.task_store.get_research(research_id)
                if current is not None and current.status in self._TERMINAL_STATUSES:
                    logger.info("decompose_aborted_terminal research_id=%s status=%s", research_id, current.status.value)
                    return
                if request.plan_first:
                    # Store an editable plan and wait for user approval (no tasks/jobs yet).
                    self._store_plan_for_review(research_id, tasks_raw)
                    logger.info(
                        "research_plan_ready research_id=%s item_count=%s depth=%s",
                        research_id, len(tasks_raw), request.depth.value,
                    )
                    return
                task_ids = []
                registered_tasks = []
                for task_dict in tasks_raw:
                    task_dict["research_id"] = research_id
                    task = self.task_store.add_task(task_dict)
                    registered_tasks.append(task)
                    task_ids.append(task.id)
                self.task_store.set_research_task_ids(research_id, task_ids)
                enqueued_jobs = 0
                for task in registered_tasks:
                    if task.status == TaskStatus.PENDING and task.queries:
                        job = self.task_store.add_search_task_job(task.id, request.depth.value, settings.job_max_attempts)
                        if self.broker:
                            self.broker.push_search_job(job.id)
                        enqueued_jobs += 1
                # Clear crash-recovery marker now that decompose ran.
                self._clear_decompose_pending(research_id)
                if enqueued_jobs == 0:
                    # Decompose produced no searchable queries (e.g. the model was
                    # unavailable / a degenerate fallback came back). Fail cleanly instead
                    # of leaving the research stuck in 'processing' forever.
                    logger.warning(
                        "research_decompose_no_queries research_id=%s task_count=%s",
                        research_id, len(registered_tasks),
                    )
                    self.task_store.update_research_status(
                        research_id,
                        ResearchStatus.FAILED,
                        "Could not generate a search plan (no searchable queries). "
                        "Check the model/API key and try again.",
                    )
                    return
                logger.info(
                    "research_decomposed research_id=%s task_count=%s depth=%s",
                    research_id,
                    len(registered_tasks),
                    request.depth.value,
                )
            except Exception as exc:
                logger.error("research_decompose_failed research_id=%s error=%s", research_id, str(exc))
                self.task_store.update_research_status(research_id, ResearchStatus.FAILED, self._failure_message(exc))

    def _clear_decompose_pending(self, research_id: str) -> None:
        """Remove the crash-recovery marker from graph_state after decompose completes."""
        research = self.task_store.get_research(research_id)
        if not research:
            return
        state = dict(research.graph_state or {})
        state.pop("decompose_pending", None)
        state.pop("decompose_payload", None)
        self.task_store.update_research_graph_state(research_id, state)

    def recover_pending_decompositions(self) -> int:
        """Re-schedule decompositions lost during a process crash.

        Scans recent PROCESSING researches for those that have ``decompose_pending=True``
        in graph_state, have no tasks yet, and were created more than
        ``settings.decompose_recovery_minutes`` minutes ago.  For each such research a
        fresh daemon thread is launched to replay ``decompose_and_enqueue``.

        Returns the number of researches for which recovery was triggered.
        """
        import threading as _threading

        stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=settings.decompose_recovery_minutes)
        recent = self.task_store.list_researches(limit=50)
        recovered = 0

        for item in recent:
            research = self.task_store.get_research(item.id)
            if not research:
                continue
            graph_state = research.graph_state or {}
            if not graph_state.get("decompose_pending"):
                continue
            # Tasks already exist — decompose ran; the flag is just stale.
            if research.task_ids:
                self._clear_decompose_pending(research.id)
                continue
            # Too recent — the background task may still be running.
            created_at = research.created_at
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                except Exception:
                    continue
            if created_at and created_at > stale_threshold:
                continue
            payload = graph_state.get("decompose_payload")
            if not payload:
                continue
            try:
                req = ResearchRequest.model_validate(payload)
            except Exception as exc:
                logger.warning(
                    "decompose_recovery_invalid_payload research_id=%s error=%s",
                    research.id, exc,
                )
                continue
            logger.info("decompose_recovery_triggered research_id=%s", research.id)
            _threading.Thread(
                target=self.decompose_and_enqueue,
                args=(research.id, req),
                daemon=True,
                name=f"decompose-recovery-{research.id[:8]}",
            ).start()
            recovered += 1

        if recovered:
            logger.info("decompose_recovery_completed count=%d", recovered)
        return recovered

    def list_researches(self, limit: int = 20, user_id: str | None = None) -> list[ResearchHistoryItem]:
        return self.task_store.list_researches(limit=limit, user_id=user_id)

    def list_thread(self, thread_id: str, user_id: str | None = None) -> list[ResearchHistoryItem]:
        """All researches in a conversation thread, oldest first."""
        return self.task_store.list_thread_researches(thread_id, user_id=user_id)

    def _ensure_research_access(self, research_id: str, user_id: str | None) -> ResearchRecord:
        """Load a research and 404 unless it belongs to this user (when scoping is on).

        When auth is enabled (user_id is not None) an unowned/NULL-owner research is NOT
        accessible — legacy rows created under AUTH_DISABLED stay private until ownership is
        backfilled (AUD-011); previously the None exemption made them cross-readable.
        """
        research = self.task_store.get_research(research_id)
        if not research or (user_id is not None and research.user_id != user_id):
            raise NotFoundError("Research not found")
        return research

    def delete_research(self, research_id: str, user_id: str | None = None) -> bool:
        if user_id is not None:
            self._ensure_research_access(research_id, user_id)
        return self.task_store.delete_research(research_id)

    _TERMINAL_STATUSES = {ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED}

    def cancel_research(self, research_id: str, user_id: str | None = None) -> ResearchRecord:
        """Mark a running research cancelled. Finalize/decompose bail out on a cancelled status,
        so no report is produced; in-flight search jobs simply finish without being used."""
        research = self._ensure_research_access(research_id, user_id) if user_id is not None else self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if research.status in self._TERMINAL_STATUSES:
            return research
        updated = self.task_store.update_research_status(research_id, ResearchStatus.CANCELLED, "Cancelled by user.")
        logger.info("research_cancelled research_id=%s", research_id)
        return updated or research

    def rename_research(self, research_id: str, title: str, user_id: str | None = None) -> ResearchRecord:
        research = self._ensure_research_access(research_id, user_id)
        if not research:
            raise NotFoundError("Research not found")
        state = dict(research.graph_state or {})
        state["title"] = title.strip()
        self.task_store.update_research_graph_state(research_id, state)
        return self.task_store.get_research(research_id)

    def get_research_status(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        return research

    def get_task_summary(self, task_id: str) -> SearchTaskSummary:
        task = self.task_store.get_task(task_id)
        if not task:
            raise NotFoundError("Task not found")
        return self._build_task_summary(task)

    def get_research_summary(self, research_id: str) -> ResearchSummary:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        tasks = self.task_store.get_tasks_by_research(research_id)
        task_summaries = [self._build_task_summary(task) for task in tasks]
        completed_tasks = sum(1 for task in tasks if task.status == TaskStatus.COMPLETED)
        pending_tasks = sum(1 for task in tasks if task.status == TaskStatus.PENDING)
        running_tasks = sum(1 for task in tasks if task.status == TaskStatus.RUNNING)
        failed_tasks = sum(1 for task in tasks if task.status == TaskStatus.FAILED)
        collected_sources = sum(len(task.result or []) for task in tasks)
        total_candidates = sum(task.search_metrics.candidate_count for task in tasks)
        total_extraction_attempts = sum(task.search_metrics.extraction_attempts for task in tasks)
        total_extraction_success_count = sum(task.search_metrics.extraction_success_count for task in tasks)
        total_extraction_failure_count = sum(task.search_metrics.extraction_failure_count for task in tasks)
        total_selected_source_count = sum(task.search_metrics.selected_source_count for task in tasks)
        task_count = len(tasks)
        avg_sources_per_task = round(collected_sources / task_count, 1) if task_count else 0.0
        finalize_ready = task_count > 0 and pending_tasks == 0 and running_tasks == 0
        aggregated_sources = self._build_research_source_pool(tasks)
        _, source_critic_summary = self.source_critic.assess_sources(aggregated_sources)
        _, evidence_coverage_summary = self.evidence_mapper.build_evidence_groups(
            aggregated_sources,
            stopwords=AnalyzerAgent.STOPWORDS,
            generic_tokens=AnalyzerAgent.CONFLICT_GENERIC_TOKENS,
            negation_tokens=AnalyzerAgent.NEGATION_TOKENS,
            max_groups=5,
        )
        claim_verification_summary = self.claim_verifier.verify_and_downgrade(
            research.final_report or "",
            self._detect_report_language(research.prompt, research.final_report),
            [],
            [],
        )[1]
        # Follow-up recommendations require LLM calls and only make sense once every task branch
        # has finished — skip them while the research is still in progress (AUD-022) so an
        # on-demand /summary fetch mid-run doesn't burn LLM calls on premature suggestions.
        replan_recommendations = (
            self.replan_agent.suggest_follow_up(
                research.prompt,
                research.depth,
                tasks,
                source_summary=source_critic_summary,
            )
            if finalize_ready
            else []
        )
        graph_execution_summary = self._build_graph_execution_summary(tasks)

        graph_state = research.graph_state or {}
        partial_report = graph_state.get("partial_report") if not research.final_report else None

        return ResearchSummary(
            id=research.id,
            prompt=research.prompt,
            depth=research.depth,
            status=research.status,
            task_ids=research.task_ids,
            created_at=research.created_at,
            updated_at=research.updated_at,
            has_final_report=bool(research.final_report),
            partial_report=partial_report,
            task_count=task_count,
            completed_tasks=completed_tasks,
            pending_tasks=pending_tasks,
            running_tasks=running_tasks,
            failed_tasks=failed_tasks,
            collected_sources=collected_sources,
            avg_sources_per_task=avg_sources_per_task,
            total_candidates=total_candidates,
            total_extraction_attempts=total_extraction_attempts,
            total_extraction_success_count=total_extraction_success_count,
            total_extraction_failure_count=total_extraction_failure_count,
            total_selected_source_count=total_selected_source_count,
            finalize_ready=finalize_ready,
            source_critic_summary=source_critic_summary,
            evidence_coverage_summary=evidence_coverage_summary,
            claim_verification_summary=claim_verification_summary,
            replan_recommendations=replan_recommendations,
            graph_execution_summary=graph_execution_summary,
            latest_finalize_job=self.task_store.get_latest_research_finalize_job(research_id),
            tasks=task_summaries,
            llm_token_usage=(research.graph_state or {}).get("llm_token_usage", {}),
        )

    def get_research_status_summary(self, research_id: str) -> ResearchStatusSummary:
        """Cheap status snapshot for polling — no source-critic/evidence/claim/replan/LLM."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        tasks = self.task_store.get_tasks_by_research(research_id)
        completed = sum(1 for task in tasks if task.status == TaskStatus.COMPLETED)
        pending = sum(1 for task in tasks if task.status == TaskStatus.PENDING)
        running = sum(1 for task in tasks if task.status == TaskStatus.RUNNING)
        failed = sum(1 for task in tasks if task.status == TaskStatus.FAILED)
        collected = sum(len(task.result or []) for task in tasks)
        task_count = len(tasks)
        avg_sources = round(collected / task_count, 1) if task_count else 0.0
        finalize_ready = task_count > 0 and pending == 0 and running == 0
        graph_state = research.graph_state or {}
        partial_report = graph_state.get("partial_report") if not research.final_report else None

        return ResearchStatusSummary(
            id=research.id,
            prompt=research.prompt,
            depth=research.depth,
            status=research.status,
            created_at=research.created_at,
            updated_at=research.updated_at,
            has_final_report=bool(research.final_report),
            partial_report=partial_report,
            task_count=task_count,
            completed_tasks=completed,
            pending_tasks=pending,
            running_tasks=running,
            failed_tasks=failed,
            collected_sources=collected,
            avg_sources_per_task=avg_sources,
            finalize_ready=finalize_ready,
            latest_finalize_job=self.task_store.get_latest_research_finalize_job(research_id),
            llm_token_usage=graph_state.get("llm_token_usage", {}),
            queue_position=self._queue_position(research_id) if research.status == ResearchStatus.QUEUED else None,
        )

    def _store_clarifications_for_review(self, research_id: str, questions: list[str]) -> None:
        """Persist clarifying questions and set status CLARIFYING (awaiting user answers)."""
        research = self.task_store.get_research(research_id)
        state = dict((research.graph_state if research else None) or {})
        state.pop("decompose_pending", None)  # wait for user; keep decompose_payload for the re-run
        state["clarifications"] = {"questions": list(questions), "answers": []}
        self.task_store.update_research_graph_state(research_id, state)
        self.task_store.update_research_status(research_id, ResearchStatus.CLARIFYING)

    def _augment_prompt_with_clarifications(self, prompt: str, graph_state: dict) -> str:
        qa = (graph_state.get("clarifications") or {}).get("qa") or []
        answered = [
            f"- {item.get('question')}: {item.get('answer')}"
            for item in qa
            if item.get("answer")
        ]
        if not answered:
            return prompt
        return prompt + "\n\nClarifications from the user:\n" + "\n".join(answered)

    def get_research_clarifications(self, research_id: str) -> Clarification:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        clar = (research.graph_state or {}).get("clarifications") or {}
        return Clarification(
            research_id=research.id,
            status=research.status,
            questions=clar.get("questions") or [],
            answers=clar.get("answers") or [],
        )

    def submit_clarifications(self, research_id: str, answers: list[str]) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if research.status != ResearchStatus.CLARIFYING:
            raise ConflictError("Research is not awaiting clarification")

        state = dict(research.graph_state or {})
        clar = dict(state.get("clarifications") or {})
        questions = clar.get("questions") or []
        answers = list(answers or [])
        clar["answers"] = answers
        clar["qa"] = [
            {"question": question, "answer": (answers[i] if i < len(answers) else "")}
            for i, question in enumerate(questions)
        ]
        state["clarifications"] = clar
        state["clarified"] = True
        state["decompose_pending"] = True
        self.task_store.update_research_graph_state(research_id, state)
        self.task_store.update_research_status(research_id, ResearchStatus.PROCESSING)

        payload = state.get("decompose_payload")
        try:
            request = ResearchRequest.model_validate(payload) if payload else ResearchRequest(
                prompt=research.prompt, depth=research.depth, plan_first=True
            )
        except Exception:
            request = ResearchRequest(prompt=research.prompt, depth=research.depth, plan_first=True)

        import threading

        threading.Thread(
            target=self.decompose_and_enqueue,
            args=(research_id, request),
            daemon=True,
            name=f"clarify-decompose-{research_id[:8]}",
        ).start()
        return self.task_store.get_research(research_id)

    def _store_plan_for_review(self, research_id: str, tasks_raw: list[dict]) -> None:
        """Persist a decomposed plan into graph_state and set status PLAN_REVIEW."""
        plan = [
            {
                "id": item.get("id") or str(uuid.uuid4()),
                "description": item.get("description", ""),
                "queries": list(item.get("queries") or []),
            }
            for item in tasks_raw
        ]
        research = self.task_store.get_research(research_id)
        state = dict((research.graph_state if research else None) or {})
        state.pop("decompose_pending", None)
        state.pop("decompose_payload", None)
        state["plan"] = plan
        self.task_store.update_research_graph_state(research_id, state)
        self.task_store.update_research_status(research_id, ResearchStatus.PLAN_REVIEW)

    def get_research_plan(self, research_id: str) -> ResearchPlan:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        items = (research.graph_state or {}).get("plan") or []
        return ResearchPlan(
            research_id=research.id,
            status=research.status,
            items=[ResearchPlanItem.model_validate(item) for item in items],
        )

    def update_research_plan(self, research_id: str, update: ResearchPlanUpdate) -> ResearchPlan:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if research.status != ResearchStatus.PLAN_REVIEW:
            raise ConflictError("Plan can only be edited while awaiting approval")
        state = dict(research.graph_state or {})
        state["plan"] = [item.model_dump() for item in update.items]
        self.task_store.update_research_graph_state(research_id, state)
        return self.get_research_plan(research_id)

    def approve_research_plan(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        if research.status != ResearchStatus.PLAN_REVIEW:
            raise ConflictError("Research is not awaiting plan approval")
        plan = (research.graph_state or {}).get("plan") or []
        if not plan:
            raise ConflictError("No plan to approve")

        registered_tasks = []
        task_ids = []
        for item in plan:
            queries = [query for query in (item.get("queries") or []) if query]
            task = self.task_store.add_task(
                {
                    "id": item.get("id") or str(uuid.uuid4()),
                    "research_id": research_id,
                    "description": item.get("description", ""),
                    "queries": queries,
                    "status": TaskStatus.PENDING,
                }
            )
            registered_tasks.append(task)
            task_ids.append(task.id)
        self.task_store.set_research_task_ids(research_id, task_ids)
        self.task_store.update_research_status(research_id, ResearchStatus.PROCESSING)
        for task in registered_tasks:
            if task.status == TaskStatus.PENDING and task.queries:
                job = self.task_store.add_search_task_job(task.id, research.depth.value, settings.job_max_attempts)
                if self.broker:
                    self.broker.push_search_job(job.id)
        logger.info("research_plan_approved research_id=%s task_count=%s", research_id, len(registered_tasks))
        return self.task_store.get_research(research_id)

    def list_research_messages(self, research_id: str) -> list[ChatMessage]:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        messages = (research.graph_state or {}).get("messages") or []
        return [ChatMessage.model_validate(message) for message in messages]

    def append_research_message(self, research_id: str, role: str, content: str) -> None:
        research = self.task_store.get_research(research_id)
        if not research:
            return
        state = dict(research.graph_state or {})
        messages = list(state.get("messages") or [])
        messages.append({"role": role, "content": content})
        state["messages"] = messages[-40:]  # cap conversation history
        self.task_store.update_research_graph_state(research_id, state)

    _CHAT_STOPWORDS = {
        "what", "which", "where", "when", "about", "could", "would", "should", "there",
        "their", "these", "those", "have", "this", "that", "with", "from", "into", "your",
        "tell", "explain", "give", "does", "they", "them", "than", "then",
        "что", "как", "какие", "какой", "почему", "когда", "где", "расскажи", "объясни", "дай",
    }

    def _question_needs_search(self, question: str, pool: list[dict]) -> bool:
        """Heuristic: True if the question's key terms aren't covered by the source pool."""
        import re

        tokens = [
            token
            for token in re.findall(r"[^\W\d_]{4,}", (question or "").lower(), flags=re.UNICODE)
            if token not in self._CHAT_STOPWORDS
        ]
        if not tokens:
            return False  # nothing concrete to look up — answer from existing context
        if not pool:
            return True
        haystack = " ".join((item.get("content") or "").lower() for item in pool)
        covered = sum(1 for token in set(tokens) if token in haystack)
        return covered / len(set(tokens)) < 0.34

    def _chat_tokens(self, text: str) -> list[str]:
        import re

        return [
            token
            for token in re.findall(r"[^\W\d_]{4,}", (text or "").lower(), flags=re.UNICODE)
            if token not in self._CHAT_STOPWORDS
        ]

    def _rank_sources_for_question(self, question: str, pool: list[dict], k: int) -> list[dict]:
        """Retrieve the k sources most relevant to the question instead of the first k.

        Lightweight lexical retrieval (frequency-weighted term overlap over title+content);
        a drop-in step that a pgvector embedding search can later replace.
        """
        from collections import Counter

        q_tokens = set(self._chat_tokens(question))
        if not q_tokens or len(pool) <= k:
            return pool[:k]
        scored: list[tuple[float, int, dict]] = []
        for index, item in enumerate(pool):
            counts = Counter(self._chat_tokens((item.get("title") or "") + " " + (item.get("content") or "")))
            score = float(sum(counts.get(token, 0) for token in q_tokens))
            scored.append((score, index, item))  # index keeps the sort stable
        scored.sort(key=lambda s: (s[0], -s[1]), reverse=True)
        ranked = [item for score, _, item in scored if score > 0][:k]
        if len(ranked) < k:  # pad with the rest to keep some breadth
            chosen = {id(it) for it in ranked}
            ranked += [item for _, _, item in scored if id(item) not in chosen][: k - len(ranked)]
        return ranked

    def _mini_search_for_chat(self, research_id: str, question: str, depth: SearchDepth) -> list[dict]:
        """Run a small follow-up web search for a chat question; persists results as a task."""
        task = self.task_store.add_task(
            {
                "id": f"chat-{uuid.uuid4()}",
                "research_id": research_id,
                "description": f"Follow-up search: {question[:80]}",
                "queries": [question],
                "status": TaskStatus.PENDING,
                "logs": ["Generated by chat follow-up"],
            }
        )
        research = self.task_store.get_research(research_id)
        if research is not None:
            self.task_store.set_research_task_ids(research_id, list(research.task_ids) + [task.id])
        agent = SearchAgent(
            task_store=self.task_store,
            max_sources=4,
            search_results_per_query=6,
            max_candidate_urls=8,
            extraction_concurrency=settings.search_extraction_concurrency,
            extraction_timeout_seconds=settings.search_extraction_timeout_seconds,
        )
        try:
            agent.run_task(task.id)
        except Exception as exc:
            logger.warning("chat_mini_search_failed research_id=%s error=%s", research_id, exc)
        refreshed = self.task_store.get_task(task.id)
        return (refreshed.result if refreshed else None) or []

    def generate_research_answer(
        self,
        research_id: str,
        question: str,
        streaming_callback=None,
        status_callback=None,
    ) -> str:
        """Grounded follow-up answer. Escalates to a mini web search when the existing
        source pool does not cover the question, then answers over the enriched pool."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        chat = self.require_agent(self.chat_agent, "Chat")

        tasks = self.task_store.get_tasks_by_research(research_id)
        pool = self._build_research_source_pool(tasks)
        if self._question_needs_search(question, pool):
            if status_callback:
                status_callback("searching")
            self._mini_search_for_chat(research_id, question, research.depth)
            tasks = self.task_store.get_tasks_by_research(research_id)
            pool = self._build_research_source_pool(tasks)
        # Retrieve the most relevant sources for this question (not just the first 12).
        pool = self._rank_sources_for_question(question, pool, 12)
        sources = [
            {
                "source_id": f"S{index}",
                "title": item.get("title"),
                "domain": item.get("domain"),
                "url": item.get("url"),
                "content": (item.get("content") or "")[:800],
            }
            for index, item in enumerate(pool, start=1)
        ]
        history = list((research.graph_state or {}).get("messages") or [])
        model = (research.graph_state or {}).get("model")
        return chat.answer(
            question,
            research.final_report or "",
            sources,
            history,
            model=model,
            streaming_callback=streaming_callback,
        )

    def get_research_sources(self, research_id: str) -> list[SearchSourcePreview]:
        """Cheap aggregated source list (deduped by URL) for the artifact panel — no LLM."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        tasks = self.task_store.get_tasks_by_research(research_id)
        pool = self._build_research_source_pool(tasks)
        return [
            SearchSourcePreview(
                url=item.get("url", ""),
                title=item.get("title"),
                domain=item.get("domain"),
                source_quality=item.get("source_quality"),
                snippet=((item.get("content") or "")[:280] or None),
            )
            for item in pool
            if item.get("url")
        ]

    def get_research_conflicts(self, research_id: str) -> list[ResearchConflict]:
        """Structured source conflicts for the artifact panel (no LLM).

        Prefers the conflicts computed during finalization (graph_state); if absent,
        recomputes from the source pool when a full AnalyzerAgent is available.
        """
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        raw = (research.graph_state or {}).get("detected_conflicts")
        if raw is None:
            analyzer = self.analyzer
            if isinstance(analyzer, AnalyzerAgent):
                tasks = self.task_store.get_tasks_by_research(research_id)
                pool = self._build_research_source_pool(tasks)
                conflict_pool = [
                    {
                        "source_id": f"S{index}",
                        "content": item.get("content", ""),
                        "url": item.get("url"),
                        "domain": item.get("domain"),
                        "title": item.get("title"),
                        "source_quality": item.get("source_quality"),
                    }
                    for index, item in enumerate(pool, start=1)
                    if item.get("content")
                ]
                raw = analyzer._detect_conflicts(conflict_pool) if conflict_pool else []
            else:
                raw = []
        return [ResearchConflict.model_validate(item) for item in (raw or [])]

    def get_research_verification(self, research_id: str) -> VerificationReport:
        """P3 verifier view (no LLM): per-claim confidence + plan-vs-report coverage.

        Recomputed on demand from the finalized report, the task plan and the
        source pool — same cheap, deterministic pattern as ``get_research_conflicts``.
        """
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        tasks = self.task_store.get_tasks_by_research(research_id)
        pool = self._build_research_source_pool(tasks)
        evidence_pool = [
            {"source_id": f"S{index}", "content": item.get("content", "")}
            for index, item in enumerate(pool, start=1)
            if item.get("content")
        ]
        evidence_groups, _ = self.evidence_mapper.build_evidence_groups(
            evidence_pool,
            stopwords=AnalyzerAgent.STOPWORDS,
            generic_tokens=AnalyzerAgent.CONFLICT_GENERIC_TOKENS,
            negation_tokens=AnalyzerAgent.NEGATION_TOKENS,
            max_groups=6,
        )
        report = research.final_report or ""
        language = self._detect_report_language(research.prompt, research.final_report)
        claim_summary = self.claim_verifier.verify_and_downgrade(report, language, [], [])[1]
        return self.report_critic.build(
            research_id,
            tasks,
            evidence_groups,
            report,
            claim_summary=claim_summary,
        )

    def get_research_report(self, research_id: str) -> ResearchReportResponse:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        # Safety net for reports finalized before the notes-strip landed: clean on read
        # so legacy reports never surface the internal "Report Notes" section either.
        from src.ui.report_utils import clean_report

        return ResearchReportResponse(
            research_id=research.id,
            status=research.status,
            final_report=clean_report(research.final_report) if research.final_report else research.final_report,
        )

    def get_research_graph(self, research_id: str) -> ResearchGraphResponse:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        return ResearchGraphResponse(
            research_id=research.id,
            status=research.status,
            graph_state=research.graph_state,
            graph_trail=research.graph_trail,
        )

    def _build_task_summary(self, task: SearchTask) -> SearchTaskSummary:
        results = task.result or []
        preview = [
            SearchSourcePreview(
                url=result.get("url", ""),
                title=result.get("title"),
                domain=result.get("domain"),
                source_quality=result.get("source_quality"),
                extraction_status=result.get("extraction_status"),
                snippet=(result.get("snippet") or result.get("content") or "")[:280] or None,
            )
            for result in results[: self.TASK_SUMMARY_SOURCE_LIMIT]
            if result.get("url")
        ]
        return SearchTaskSummary(
            id=task.id,
            research_id=task.research_id,
            description=task.description,
            queries=task.queries,
            status=task.status,
            created_at=task.created_at,
            updated_at=task.updated_at,
            result_count=len(results),
            log_count=len(task.logs or []),
            recent_logs=(task.logs or [])[-self.TASK_SUMMARY_LOG_LIMIT :],
            source_preview=preview,
            search_metrics=task.search_metrics,
            latest_search_job=self.task_store.get_latest_search_task_job(task.id),
        )

    def _build_research_source_pool(self, tasks: list[SearchTask]) -> list[dict]:
        """Aggregate sources across all tasks, deduplicating by URL (Q-6)."""
        aggregated_sources: list[dict] = []
        seen_urls: set[str] = set()
        for task in tasks:
            for result in task.result or []:
                url = result.get("url")
                content = result.get("content")
                if not url or not content:
                    continue
                # Normalise: strip trailing slash, lowercase scheme+host
                normalised = url.rstrip("/").lower().split("?")[0].split("#")[0]
                if normalised in seen_urls:
                    continue
                seen_urls.add(normalised)
                aggregated_sources.append(
                    {
                        "url": url,
                        "domain": result.get("domain"),
                        "title": result.get("title"),
                        "content": content,
                        "source_quality": result.get("source_quality"),
                    }
                )
        return aggregated_sources

    def execute_replan_search_pass(
        self,
        research_id: str,
        depth: SearchDepth,
        recommendations: list[ReplanRecommendation],
    ) -> list[SearchTask]:
        research = self.task_store.get_research(research_id)
        if research is None:
            raise NotFoundError("Research not found")

        created_tasks: list[SearchTask] = []
        existing_task_ids = list(research.task_ids)
        for recommendation in recommendations[:3]:
            queries = [query for query in recommendation.suggested_queries if query]
            if not queries:
                continue
            task = self.task_store.add_task(
                {
                    "id": f"replan-{uuid.uuid4()}",
                    "research_id": research_id,
                    "description": f"Follow-up evidence pass: {recommendation.reason}",
                    "queries": queries,
                    "status": TaskStatus.PENDING,
                    "logs": [f"Generated by ReplanAgent: {recommendation.reason}"],
                }
            )
            created_tasks.append(task)
            existing_task_ids.append(task.id)

        if not created_tasks:
            return []

        self.task_store.set_research_task_ids(research_id, existing_task_ids)

        if self.broker:
            # Push to queue — let the parallel workers handle them concurrently
            for task in created_tasks:
                job = self.task_store.add_search_task_job(task.id, depth.value, settings.job_max_attempts)
                self.broker.push_search_job(job.id)
                logger.info("replan_task_enqueued task_id=%s job_id=%s", task.id, job.id)

            # Poll until all replan tasks finish (timeout: same as search job timeout)
            deadline = time.monotonic() + settings.search_job_timeout_seconds
            poll_interval = 2.0
            while time.monotonic() < deadline:
                pending = [
                    t for t in created_tasks
                    if (self.task_store.get_task(t.id) or t).status
                    not in (TaskStatus.COMPLETED, TaskStatus.FAILED)
                ]
                if not pending:
                    break
                logger.debug("replan_waiting pending_count=%d", len(pending))
                time.sleep(poll_interval)
        else:
            # No broker — fallback to sequential execution in current thread
            for task in created_tasks:
                self.run_search_task(task.id, depth)

        return [
            refreshed
            for refreshed in (self.task_store.get_task(task.id) for task in created_tasks)
            if refreshed is not None
        ]

    def _detect_report_language(self, prompt: str, report: str | None) -> str:
        text = (report or prompt).lower()
        if any("а" <= char <= "я" or char == "ё" for char in text):
            return "ru"
        if any(token in text for token in (" el ", " la ", " para ", " según ")):
            return "es"
        return "en"

    def _build_graph_execution_summary(self, tasks: list[SearchTask]) -> dict:
        follow_up_tasks = [task for task in tasks if task.id.startswith("replan-")]
        replan_tasks = [
            task
            for task in follow_up_tasks
            if any("generated by replanagent" in (log or "").lower() for log in (task.logs or []))
            and not any("resolve conflicting evidence" in (log or "").lower() for log in (task.logs or []))
        ]
        tie_break_tasks = [
            task
            for task in follow_up_tasks
            if any("resolve conflicting evidence" in (log or "").lower() for log in (task.logs or []))
        ]
        follow_up_queries: list[str] = []
        for task in follow_up_tasks:
            for query in task.queries or []:
                if query not in follow_up_queries:
                    follow_up_queries.append(query)
        return {
            "branching_active": bool(follow_up_tasks),
            "follow_up_task_count": len(follow_up_tasks),
            "replan_task_count": len(replan_tasks),
            "tie_break_task_count": len(tie_break_tasks),
            "follow_up_query_count": len(follow_up_queries),
            "follow_up_queries": follow_up_queries[:8],
        }

    def checkpoint_graph_state(self, research_id: str, graph_state: dict, event: dict | None = None) -> None:
        # The finalize graph's state doesn't carry user-facing metadata (thread_id, title,
        # model, …). Merge over the existing graph_state so a checkpoint can't wipe it.
        research = self.task_store.get_research(research_id)
        if research and research.graph_state:
            graph_state = {**research.graph_state, **graph_state}
        self.task_store.update_research_graph_state(research_id, graph_state)
        if event is not None:
            self.task_store.append_research_graph_event(research_id, event)

    # ── adversarial red-team pass ───────────────────────────────────────────────

    _RED_TEAM_VERDICT_LABELS = {
        "ru": {
            "refuted": "Опровергнуто",
            "contested": "Оспаривается",
            "qualified": "С оговоркой",
            "holds": "Устояло",
        },
        "en": {
            "refuted": "Refuted",
            "contested": "Contested",
            "qualified": "Qualified",
            "holds": "Holds",
        },
    }

    def _maybe_red_team(self, report: str, research, tasks: list) -> str:
        """HARD-only adversarial pass: stress-test the report's claims, append findings.

        Searches for counter-evidence to the load-bearing claims, judges each, stores the
        structured result in graph_state, and appends a 'weaknesses' section to the report.
        Never raises — a red-team failure must not break finalization.
        """
        if not settings.red_team_enabled or not (report or "").strip():
            return report
        if getattr(research.depth, "value", research.depth) != SearchDepth.HARD.value:
            return report  # deep pass only — too slow/expensive for EASY/MEDIUM
        agent = self.red_team_agent
        if agent is None:
            return report
        try:
            language = self._detect_report_language(research.prompt, report)
            search_agent = SearchAgent(
                task_store=self.task_store,
                max_sources=4,
                search_results_per_query=5,
                max_candidate_urls=8,
                extraction_concurrency=settings.search_extraction_concurrency,
                extraction_timeout_seconds=settings.search_extraction_timeout_seconds,
            )
            red_team = agent.challenge(
                research.prompt,
                report,
                search_agent.search_query,
                language=language,
                model=settings.red_team_model,
                max_claims=settings.red_team_max_claims,
            )
            if not red_team.findings:
                return report
            red_team.research_id = research.id
            self._store_red_team(research.id, red_team)
            return report.rstrip() + "\n\n" + self._render_red_team_section(red_team, language)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("red_team_pass_failed research_id=%s error=%s", research.id, exc)
            return report

    def get_research_confidence(self, research_id: str) -> ConfidenceReport:
        """Honesty meter (no LLM): fuse citation grounding, claim verification, red-team and
        source independence into one calibrated confidence. Recomputed on demand, same cheap
        pattern as verification/conflicts — so it always reflects the latest stored signals.
        """
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        verification = self.get_research_verification(research_id)
        citations = self.get_research_citation_audit(research_id)
        red_team = self.get_research_red_team(research_id)
        independence = self.get_research_source_independence(research_id)
        confidence = self.confidence_agent.compose(verification, citations, red_team, independence)
        confidence.research_id = research_id
        return confidence

    # ── structured comparison table ─────────────────────────────────────────────

    _COMPARISON_SIGNALS = (
        "сравни", "сравнение", "что лучше", "лучше ли", " против ", " или ", "разница между",
        "vs", "versus", "compare", "comparison", "difference between", " or ", "better",
    )

    def _looks_like_comparison(self, prompt: str) -> bool:
        lowered = f" {(prompt or '').lower()} "
        return any(signal in lowered for signal in self._COMPARISON_SIGNALS)

    def _maybe_build_comparison(self, report: str, research) -> None:
        """If the query compares named options, extract a scored table and store it.

        Heuristic-gated (only comparison-shaped prompts run the LLM). Never raises.
        """
        if self.comparison_agent is None or not (report or "").strip():
            return
        if not self._looks_like_comparison(research.prompt):
            return
        try:
            language = self._detect_report_language(research.prompt, report)
            table = self.comparison_agent.build(
                research.prompt, report, language=language, model=settings.red_team_model
            )
            if not table.has_table:
                return
            table.research_id = research.id
            state = dict((self.task_store.get_research(research.id).graph_state) or {})
            state["comparison"] = table.model_dump()
            self.task_store.update_research_graph_state(research.id, state)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("comparison_build_failed research_id=%s error=%s", research.id, exc)

    def get_research_comparison(self, research_id: str) -> ComparisonTable:
        """Stored comparison table for the artifact panel (empty when not a comparison)."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("comparison")
        if not data:
            return ComparisonTable(research_id=research_id)
        return ComparisonTable.model_validate(data)

    def _render_red_team_section(self, red_team: RedTeamReport, language: str) -> str:
        labels = self._RED_TEAM_VERDICT_LABELS.get(language, self._RED_TEAM_VERDICT_LABELS["en"])
        heading = "## Слабые места и контраргументы" if language == "ru" else "## Weaknesses & counter-arguments"
        intro = (
            "Ключевые утверждения отчёта проверены на опровержение."
            if language == "ru"
            else "The report's key claims were stress-tested against counter-evidence."
        )
        lines = [heading, "", intro, ""]
        for finding in red_team.findings:
            verdict = labels.get(finding.verdict, finding.verdict)
            lines.append(f"- **{verdict}** — {finding.claim}")
            if finding.challenge:
                lines.append(f"  {finding.challenge}")
        return "\n".join(lines)

    def _inject_graph_execution_trail(self, report: str, research_id: str) -> str:
        research = self.task_store.get_research(research_id)
        if not research or not research.graph_trail:
            return report

        graph_state = research.graph_state or {}
        if (
            graph_state.get("replan_attempts", 0) <= 0
            and graph_state.get("tie_break_attempts", 0) <= 0
            and graph_state.get("analyze_attempts", 0) <= 1
        ):
            return report

        language = self._detect_report_language(research.prompt, report)
        heading = "## Трасса выполнения графа" if language == "ru" else "## Graph Execution Trail"
        step_label = "Шаг" if language == "ru" else "Step"
        detail_label = "Детали" if language == "ru" else "Details"
        lines = [heading]
        # Exclude live search-progress steps — the trail in the report is the finalize graph.
        finalize_entries = [e for e in research.graph_trail if e.get("step") != "search"]
        for entry in finalize_entries[-8:]:
            step = entry.get("step") or "unknown"
            detail = entry.get("detail") or ""
            lines.append(f"- {step_label}: {step}. {detail_label}: {detail}")
        return f"{report.rstrip()}\n\n" + "\n".join(lines)

    def _get_research_for_finalization(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        if research.status in [
            ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED
        ]:
            return research

        tasks = self.task_store.get_tasks_by_research(research_id)
        all_done = all(t.status in [TaskStatus.COMPLETED, TaskStatus.FAILED] for t in tasks)
        any_failed = any(t.status == TaskStatus.FAILED for t in tasks)

        if not tasks:
            raise ConflictError("Research has no tasks to finalize")

        if not all_done:
            raise ConflictError("Research tasks are still in progress")

        if any_failed and all(t.status == TaskStatus.FAILED for t in tasks):
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.FAILED,
                "All tasks failed.",
            )
            return self.task_store.get_research(research_id)

        return research

    def _fire_webhook(self, url: str, research_id: str, payload: dict[str, Any]) -> None:
        """POST completion notification to user-supplied webhook URL (best-effort).

        Guards against SSRF: by default the connection is pinned to a validated public IP, so a
        user cannot make the server reach internal/loopback/metadata endpoints — and a host that
        DNS-rebinds between validation and connect still can't be reached (SEC-007).
        """
        if not settings.webhook_allow_private_targets:
            from src.net_safety import safe_post_json

            if safe_post_json(url, payload, timeout=10.0):
                logger.info("webhook_fired url=%s research_id=%s", url, research_id)
            return
        try:
            import httpx
            httpx.post(url, json=payload, timeout=10.0)
            logger.info("webhook_fired url=%s research_id=%s", url, research_id)
        except Exception as exc:
            logger.warning("webhook_failed url=%s error=%s", url, exc)

    def _emit_finalize_progress(self, research_id: str, step: str) -> None:
        """Append a finalize-phase step to the live trail so the progress trace keeps moving
        during synthesis (and surfaces the trust/verification work as it happens). Labelled
        by the frontend via trace.{step}; failures must never break finalization."""
        store = self.task_store
        if not research_id or not hasattr(store, "append_research_graph_event"):
            return
        try:
            store.append_research_graph_event(research_id, {"step": step})
        except Exception:
            pass

    def complete_research_finalization(self, research_id: str) -> ResearchRecord:
        with bind_observability_context(research_id=research_id):
            research = self.task_store.get_research(research_id)
            if not research:
                raise NotFoundError("Research not found")
            if research.status == ResearchStatus.CANCELLED:
                logger.info("finalize_skipped_cancelled research_id=%s", research_id)
                # user cancelled — don't spend the analysis call or overwrite the status; return
                # the (cancelled) record so the contract stays ResearchRecord, never None (AUD-033)
                return research

            tasks = self.task_store.get_tasks_by_research(research_id)
            analyzer = self.require_agent(self.analyzer, "Analyzer")

            # reset token counter before this analysis run
            # (analyzers may have no .llm — e.g. stub/static report agents)
            analyzer_llm = getattr(analyzer, "llm", None)
            if analyzer_llm is not None and hasattr(analyzer_llm, "reset_usage"):
                analyzer_llm.reset_usage()

            if settings.use_langgraph_finalize_graph:
                try:
                    report = self.finalize_graph_runner.run(
                        research_id,
                        research.prompt,
                        tasks,
                        research.depth,
                    )
                except FinalizeCancelled:
                    latest = self.task_store.get_research(research_id)
                    if latest is None:
                        raise NotFoundError("Research not found")
                    logger.info("finalize_stopped_cancelled research_id=%s", research_id)
                    return latest
            else:
                report = analyzer.run_analysis(
                    research.prompt,
                    tasks,
                    depth=research.depth,
                    model=(research.graph_state or {}).get("model"),
                )
            self._emit_finalize_progress(research_id, "redteam")
            report = self._maybe_red_team(report, research, tasks)
            self._emit_finalize_progress(research_id, "audit")
            # Reconstruct the analyzer's source numbering ONCE and share it across the trust
            # steps — each used to recompute it independently (AUD-013).
            aggregated = self._aggregated_sources(research, tasks)
            self._audit_citations(report, research, tasks, aggregated=aggregated)
            self._analyze_source_independence(research, tasks, aggregated=aggregated)
            self._assess_source_reputation(research, tasks, aggregated=aggregated)
            self._check_numbers(report, research, tasks, aggregated=aggregated)
            self._check_retractions(research, tasks, aggregated=aggregated)
            self._maybe_build_comparison(report, research)
            self._emit_finalize_progress(research_id, "viewpoints")
            self._maybe_assess_stance(research, tasks, aggregated=aggregated)
            self._analyze_cross_language(research, tasks, aggregated=aggregated)
            report = self._inject_graph_execution_trail(report, research_id)
            # The "Report Notes" / "Примечания к отчёту" section is an INTERNAL quality
            # signal (the finalize graph re-drafts while it's present). It must never
            # reach the reader — strip it (plus any LLM preamble) before persisting, so
            # every downstream path (SSE, API, export, public share) serves the clean report.
            from src.ui.report_utils import clean_report

            report = clean_report(report)
            # The user may have cancelled while this analysis was running — honour it
            # rather than overwriting CANCELLED with a COMPLETED report.
            latest = self.task_store.get_research(research_id)
            if latest is not None and latest.status == ResearchStatus.CANCELLED:
                logger.info("finalize_discarded_cancelled research_id=%s", research_id)
                return latest
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.COMPLETED,
                report,
            )

            # persist token usage into graph_state (U-3)
            if analyzer_llm is not None and hasattr(analyzer_llm, "token_usage"):
                usage = analyzer_llm.token_usage
                logger.info(
                    "research_token_usage prompt=%d completion=%d cost_usd=%.4f",
                    usage.get("prompt_tokens", 0),
                    usage.get("completion_tokens", 0),
                    usage.get("estimated_cost_usd", 0),
                )
                # Atomic merge — the finalize trust steps wrote other graph_state keys after
                # `research` was captured; merge_research_graph_state row-locks so this can't
                # wipe them (AUD-014), replacing the old re-fetch-then-replace workaround.
                self.task_store.merge_research_graph_state(research_id, {"llm_token_usage": usage})

            logger.info("research_finalize_completed")

            # fire webhook if configured (F-1)
            gs = (research.graph_state or {})
            webhook_url = gs.get("webhook_url")
            if webhook_url:
                self._fire_webhook(
                    webhook_url,
                    research_id,
                    {"research_id": research_id, "status": "completed"},
                )

            return self.task_store.get_research(research_id)

    @staticmethod
    def _failure_message(exc: Exception) -> str:
        """Turn an internal exception into a clean, non-leaky failure reason.

        Avoids dumping raw provider errors (which may carry keys/credentials) into
        the stored report; classifies the common cases for an actionable message.
        """
        text = str(exc)
        low = text.lower()
        if "401" in text or "authentication" in low or ("api key" in low and "invalid" in low):
            return (
                "Research failed: the language model rejected the request "
                "(authentication error). Check that a valid API key is configured."
            )
        if "429" in text or "rate limit" in low or "rate-limit" in low:
            return "Research failed: the language model is rate-limited. Please try again shortly."
        if "timeout" in low or "timed out" in low:
            return "Research failed: the analysis timed out. Please try again."
        return "Research failed during analysis. Please try again."

    def enqueue_research_finalization(self, research_id: str) -> tuple[ResearchRecord, ResearchFinalizeJob | None]:
        research = self._get_research_for_finalization(research_id)
        self.require_agent(self.analyzer, "Analyzer")
        # Atomic single-winner transition into ANALYZING. Concurrent callers across replicas
        # (or a re-delivered search job completing the same research) lose the CAS and must NOT
        # enqueue a duplicate finalize job. Also rejects terminal/already-finalizing states.
        if not self.task_store.try_begin_finalization(research_id):
            return research, None

        with bind_observability_context(research_id=research_id):
            job = self.task_store.add_research_finalize_job(research_id, settings.job_max_attempts)
            if self.broker:
                self.broker.push_finalize_job(job.id)
            logger.info("research_finalize_enqueued finalize_job_id=%s", job.id)
            return self.task_store.get_research(research_id), job

    def process_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.task_store.get_research_finalize_job(job_id)
        if job is None:
            return None

        with bind_observability_context(job_id=job.id, research_id=job.research_id):
            try:
                logger.info("finalize_job_processing")
                self.complete_research_finalization(job.research_id)
                completed_job = self.task_store.update_research_finalize_job(
                    job_id,
                    FinalizeJobStatus.COMPLETED,
                )
                logger.info("finalize_job_completed")
                return completed_job
            except Exception as exc:
                failed_job = self.task_store.record_research_finalize_job_failure(job_id, str(exc))
                logger.warning(
                    "finalize_job_failed error=%s next_status=%s",
                    str(exc),
                    failed_job.status.value if failed_job else "missing",
                )
                if failed_job and failed_job.status == FinalizeJobStatus.PENDING and self.broker:
                    self.broker.push_finalize_job(failed_job.id)
                    logger.info("finalize_job_retry_scheduled")
                if failed_job and failed_job.status == FinalizeJobStatus.DEAD_LETTER:
                    self.task_store.update_research_status(
                        job.research_id,
                        ResearchStatus.FAILED,
                        self._failure_message(exc),
                    )
                    logger.error("finalize_job_dead_letter")
                return failed_job
            finally:
                # A running slot likely just freed — admit the next queued research.
                try:
                    self.promote_queued_researches()
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("promote_after_finalize_failed error=%s", exc)

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None:
        heartbeat = self.task_store.get_worker_heartbeat(worker_name)
        if not heartbeat:
            return None
        step_events = self._filter_graph_step_events(worker_name=worker_name)
        maintenance_summary = self._build_maintenance_summary(heartbeat.maintenance_summary)
        graph_alerts = self._build_graph_alerts(heartbeat.graph_metrics)
        return heartbeat.model_copy(
            update={
                "graph_alerts": graph_alerts,
                "graph_alert_trend": self._build_graph_alert_trend(step_events),
                "maintenance_summary": maintenance_summary,
                "operational_health": self._build_operational_health(
                    QueueMetrics(),
                    graph_alerts,
                    maintenance_summary,
                ),
            }
        )

    def touch_worker_heartbeat(
        self,
        worker_name: str,
        processed_jobs: int,
        status: str,
        last_error: str | None = None,
        extraction_metrics: dict | None = None,
        graph_metrics: dict | None = None,
        graph_step_events: list[dict] | None = None,
        maintenance_summary: dict | None = None,
    ) -> WorkerHeartbeat:
        return self.task_store.upsert_worker_heartbeat(
            worker_name,
            processed_jobs,
            status,
            last_error,
            extraction_metrics if extraction_metrics is not None else get_extraction_metrics_snapshot(),
            graph_metrics if graph_metrics is not None else get_graph_metrics_snapshot(),
            graph_step_events if graph_step_events is not None else get_graph_step_events_snapshot(),
            maintenance_summary or {},
        )

    def get_queue_metrics(self) -> QueueMetrics:
        metrics = self.task_store.get_queue_metrics()
        maintenance_heartbeat = self.task_store.get_worker_heartbeat("maintenance")
        graph_alerts = self._build_graph_alerts(metrics.graph_metrics)
        maintenance_summary = (
            self._build_maintenance_summary(maintenance_heartbeat.maintenance_summary)
            if maintenance_heartbeat
            else MaintenanceSummary()
        )
        enriched_metrics = metrics.model_copy(
            update={
                "graph_alerts": graph_alerts,
                "graph_alert_trend": self._build_graph_alert_trend(self._filter_graph_step_events()),
                "maintenance_summary": maintenance_summary,
                "operational_health": self._build_operational_health(metrics, graph_alerts, maintenance_summary),
            }
        )
        set_queue_metrics(enriched_metrics)
        return enriched_metrics

    def acknowledge_operational_recommendation(
        self,
        code: str,
    ) -> OperationalHealth.RecommendationEntry:
        return self._update_operational_recommendation_state(
            code,
            acknowledged=True,
        )

    def resolve_operational_recommendation(
        self,
        code: str,
        note: str | None = None,
    ) -> OperationalHealth.RecommendationEntry:
        normalized_note = " ".join((note or "").split()) or None
        return self._update_operational_recommendation_state(
            code,
            acknowledged=True,
            resolved=True,
            resolution_note=normalized_note,
        )

    def _update_operational_recommendation_state(
        self,
        code: str,
        *,
        acknowledged: bool | None = None,
        resolved: bool | None = None,
        resolution_note: str | None = None,
    ) -> OperationalHealth.RecommendationEntry:
        heartbeat = self.task_store.get_worker_heartbeat("maintenance")
        if heartbeat is None:
            raise NotFoundError("Maintenance heartbeat not found")

        maintenance_summary = heartbeat.maintenance_summary.model_dump(mode="json")
        recommendations = list(maintenance_summary.get("recent_operational_recommendations") or [])
        updated_recommendation: dict | None = None
        current_timestamp = datetime.now(timezone.utc).isoformat()

        for item in recommendations:
            if str(item.get("code") or "") != code:
                continue
            if acknowledged is not None:
                item["acknowledged"] = acknowledged
                item["acknowledged_at"] = current_timestamp if acknowledged else None
            if resolved is not None:
                item["resolved"] = resolved
                item["resolved_at"] = current_timestamp if resolved else None
            if resolution_note is not None:
                item["resolution_note"] = resolution_note
            updated_recommendation = item
            break

        if updated_recommendation is None:
            raise NotFoundError("Operational recommendation not found")

        if updated_recommendation is not None:
            event_type = "acknowledged"
            event_note = None
            if resolved:
                event_type = "resolved"
                event_note = resolution_note
            maintenance_summary["recent_operational_recommendations"] = recommendations
            maintenance_summary["recent_operational_recommendation_events"] = self._append_operational_recommendation_event(
                maintenance_summary.get("recent_operational_recommendation_events") or [],
                code=str(updated_recommendation.get("code") or code),
                event_type=event_type,
                message=str(updated_recommendation.get("message") or ""),
                timestamp=current_timestamp,
                note=event_note,
            )
        self.touch_worker_heartbeat(
            "maintenance",
            heartbeat.processed_jobs,
            heartbeat.status,
            heartbeat.last_error,
            heartbeat.extraction_metrics.model_dump(mode="json"),
            heartbeat.graph_metrics.model_dump(mode="json"),
            maintenance_summary=maintenance_summary,
        )
        return OperationalHealth.RecommendationEntry.model_validate(updated_recommendation)

    def get_health_status(self) -> dict:
        # Probe dependencies first so /health is a real readiness signal (AUD-036).
        db_ok = self.task_store.ping()
        redis_status = "disabled" if self.broker is None else ("ok" if self.broker.ping() else "down")
        dependencies = {"database": "ok" if db_ok else "down", "redis": redis_status}
        if not db_ok:
            return {"status": "degraded", "dependencies": dependencies}
        graph_metrics = GraphMetrics.model_validate(get_graph_metrics_snapshot())
        step_events = self._filter_graph_step_events()
        queue_metrics = self.get_queue_metrics()
        return {
            "status": "ok" if redis_status != "down" else "degraded",
            "dependencies": dependencies,
            "extraction_metrics": get_extraction_metrics_snapshot(),
            "graph_metrics": graph_metrics.model_dump(),
            "graph_alerts": [alert.model_dump() for alert in self._build_graph_alerts(graph_metrics)],
            "graph_alert_trend": self._build_graph_alert_trend(step_events).model_dump(),
            "operational_health": queue_metrics.operational_health.model_dump(),
        }

    def _build_graph_alerts(self, graph_metrics: GraphMetrics) -> list[GraphAlert]:
        alerts: list[GraphAlert] = []
        for step_name, step_metrics in graph_metrics.steps.items():
            if step_metrics.run_count <= 0:
                continue

            if step_metrics.avg_ms >= self.GRAPH_STEP_CRITICAL_MS:
                alerts.append(
                    GraphAlert(
                        code="high_avg_ms",
                        severity="critical",
                        step=step_name,
                        current_value=step_metrics.avg_ms,
                        threshold=self.GRAPH_STEP_CRITICAL_MS,
                        hint=self._graph_alert_hint("high_avg_ms", step_name),
                    )
                )
            elif step_metrics.avg_ms >= self.GRAPH_STEP_WARNING_MS:
                alerts.append(
                    GraphAlert(
                        code="high_avg_ms",
                        severity="warning",
                        step=step_name,
                        current_value=step_metrics.avg_ms,
                        threshold=self.GRAPH_STEP_WARNING_MS,
                        hint=self._graph_alert_hint("high_avg_ms", step_name),
                    )
                )

            if step_metrics.failure_count >= self.GRAPH_STEP_FAILURE_CRITICAL_COUNT:
                alerts.append(
                    GraphAlert(
                        code="step_failures",
                        severity="critical",
                        step=step_name,
                        current_value=float(step_metrics.failure_count),
                        threshold=float(self.GRAPH_STEP_FAILURE_CRITICAL_COUNT),
                        hint=self._graph_alert_hint("step_failures", step_name),
                    )
                )
            elif step_metrics.failure_count >= self.GRAPH_STEP_FAILURE_WARNING_COUNT:
                alerts.append(
                    GraphAlert(
                        code="step_failures",
                        severity="warning",
                        step=step_name,
                        current_value=float(step_metrics.failure_count),
                        threshold=float(self.GRAPH_STEP_FAILURE_WARNING_COUNT),
                        hint=self._graph_alert_hint("step_failures", step_name),
                    )
                )

        analyze_runs = graph_metrics.steps["analyze"].run_count
        completed_runs = max(graph_metrics.completed_run_count, 1)
        analyze_retry_count = max(analyze_runs - completed_runs, 0)
        if analyze_retry_count >= self.GRAPH_ANALYZE_RETRY_CRITICAL_COUNT:
            alerts.append(
                GraphAlert(
                    code="analyze_retries",
                    severity="critical",
                    step="analyze",
                    current_value=float(analyze_retry_count),
                    threshold=float(self.GRAPH_ANALYZE_RETRY_CRITICAL_COUNT),
                    hint=self._graph_alert_hint("analyze_retries", "analyze"),
                )
            )
        elif analyze_retry_count >= self.GRAPH_ANALYZE_RETRY_WARNING_COUNT:
            alerts.append(
                GraphAlert(
                    code="analyze_retries",
                    severity="warning",
                    step="analyze",
                    current_value=float(analyze_retry_count),
                    threshold=float(self.GRAPH_ANALYZE_RETRY_WARNING_COUNT),
                    hint=self._graph_alert_hint("analyze_retries", "analyze"),
                )
            )
        return alerts

    def _filter_graph_step_events(self, worker_name: str | None = None, research_id: str | None = None) -> list[dict]:
        events = self.task_store.get_graph_step_events(worker_name=worker_name)
        filtered = []
        for event in events:
            if research_id and event.get("research_id") != research_id:
                continue
            filtered.append(event)
        return filtered

    def finalize_research(self, research_id: str) -> ResearchRecord:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research

        self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
        return self.complete_research_finalization(research_id)

    def run_search_task(self, task_id: str, depth: SearchDepth):
        with bind_observability_context(task_id=task_id):
            profile = get_depth_profile(depth)
            agent = SearchAgent(
                task_store=self.task_store,
                max_sources=profile["source_limit"],
                search_results_per_query=profile["search_results_per_query"],
                max_candidate_urls=profile["max_candidate_urls"],
                extraction_concurrency=settings.search_extraction_concurrency,
                extraction_timeout_seconds=settings.search_extraction_timeout_seconds,
            )
            agent.run_task(task_id)
            # Auto-finalize once every search task for this research is done — nothing
            # else triggers it, so without this the research stalls in 'processing'.
            task = self.task_store.get_task(task_id)
            if task and task.research_id:
                self._maybe_enqueue_finalization(task.research_id)

    def _maybe_enqueue_finalization(self, research_id: str) -> None:
        research = self.task_store.get_research(research_id)
        if research is None or research.status in (
            ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED
        ):
            return
        tasks = self.task_store.get_tasks_by_research(research_id)
        if not tasks:
            return
        if all(t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED) for t in tasks):
            logger.info("research_search_complete_auto_finalize research_id=%s task_count=%s", research_id, len(tasks))
            self.enqueue_research_finalization(research_id)  # idempotent via status guard

    def process_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        job = self.task_store.get_search_task_job(job_id)
        if job is None:
            return None

        task = self.task_store.get_task(job.task_id)
        if task is None:
            logger.error("search_job_task_missing job_id=%s task_id=%s", job_id, job.task_id)
            return self.task_store.update_search_task_job(
                job_id,
                SearchJobStatus.FAILED,
                "Task not found",
            )

        with bind_observability_context(job_id=job.id, task_id=task.id, research_id=task.research_id):
            # If the user cancelled (or the research otherwise ended) while this job sat in
            # the queue, don't spend search/extraction on it — drain the job and move on.
            research = self.task_store.get_research(task.research_id) if task.research_id else None
            if research is not None and research.status in self._TERMINAL_STATUSES:
                logger.info("search_job_skipped_terminal status=%s", research.status.value)
                return self.task_store.update_search_task_job(
                    job_id, SearchJobStatus.COMPLETED, "Research no longer active — search skipped"
                )
            try:
                logger.info("search_job_processing depth=%s", job.depth.value)
                self.run_search_task(task.id, job.depth)
                task = self.task_store.get_task(task.id)
                if task is not None and task.status == TaskStatus.FAILED:
                    failed_job = self.task_store.record_search_task_job_failure(
                        job_id,
                        task.logs[-1] if task.logs else "Search task failed",
                    )
                    logger.warning(
                        "search_job_failed next_status=%s",
                        failed_job.status.value if failed_job else "missing",
                    )
                    if failed_job and failed_job.status == SearchJobStatus.PENDING:
                        self.task_store.update_task(
                            task.id,
                            TaskUpdate(
                                status=TaskStatus.PENDING,
                                log="Search job scheduled for retry",
                            ),
                        )
                        if self.broker:
                            self.broker.push_search_job(failed_job.id)
                        logger.info("search_job_retry_scheduled")
                    if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                        logger.error("search_job_dead_letter")
                    return failed_job

                completed_job = self.task_store.update_search_task_job(job_id, SearchJobStatus.COMPLETED)
                logger.info("search_job_completed")
                return completed_job
            except Exception as exc:
                failed_job = self.task_store.record_search_task_job_failure(job_id, str(exc))
                logger.warning(
                    "search_job_exception error=%s next_status=%s",
                    str(exc),
                    failed_job.status.value if failed_job else "missing",
                )
                if failed_job and failed_job.status == SearchJobStatus.PENDING:
                    self.task_store.update_task(
                        task.id,
                        TaskUpdate(
                            status=TaskStatus.PENDING,
                            log="Search job scheduled for retry",
                        ),
                    )
                    if self.broker:
                        self.broker.push_search_job(failed_job.id)
                    logger.info("search_job_retry_scheduled")
                if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                    logger.error("search_job_dead_letter")
                return failed_job
