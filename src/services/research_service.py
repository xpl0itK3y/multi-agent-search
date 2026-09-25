import hashlib
import inspect
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from src.domain.errors import ConflictError, NotFoundError, ServiceUnavailableError, UnprocessableError

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
from src.agents.cross_language import detect_language
from src.agents.numeric_check import NumericCheckAgent
from src.agents.confidence import ConfidenceAgent
from src.agents.search import SearchAgent
from src.agents.source_critic import SourceCriticAgent
from src.agents.trail_text import TRAIL_DETAILS, research_language, trail_detail
from src.brokers.redis_broker import RedisBroker
from src.services.auth_mixin import AuthMixin
from src.services.operational_health_mixin import OperationalHealthMixin
from src.services.export_mixin import ExportMixin
from src.services.job_queue_mixin import JobQueueMixin
from src.services.trust_report_mixin import TrustReportMixin
from src.services.share_mixin import ShareMixin
from src.agents.catalog import AGENTS_CATALOG
from src.domain import (
    AdminAuditLogItem,
    AdminDryRunResult,
    AdminOverviewResponse,
    AdminTokenAnalyticsResponse,
    AgentMetadataItem,
    MaintenanceActionRequest,
    MaintenanceSummary,
    OperationalHealth,
    DecomposeResponse,
    FinalizeJobStatus,
    GraphAlert,
    GraphMetrics,
    QueueMetrics,
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
    ConfidenceReport,
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
    SourceCriticSummary,
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
from src.graph import FinalizeCancelled, FinalizeGraphRunner, FinalizeLeaseLost
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
        llm_available: bool = True,
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
        # (time.monotonic() when computed, response): see get_admin_token_analytics.
        self._token_analytics_cache: tuple[float, AdminTokenAnalyticsResponse] | None = None
        self.numeric_checker = NumericCheckAgent()
        self.confidence_agent = ConfidenceAgent()
        self.broker = broker
        self.llm_available = llm_available
        self.finalize_graph_runner = FinalizeGraphRunner(self)

    # ── auth ──────────────────────────────────────────────────────────────────
    # (auth methods extracted to AuthMixin — src/services/auth_mixin.py)

    def require_agent(self, agent, agent_name: str):
        if agent is None:
            raise ServiceUnavailableError(
                f"{agent_name} is unavailable. Check service configuration."
            )
        return agent

    @staticmethod
    def _accepts_keyword(callable_object, keyword: str) -> bool:
        """Keep custom/test agents compatible while adding optional pipeline context."""
        try:
            parameters = inspect.signature(callable_object).parameters.values()
        except (TypeError, ValueError):
            return False
        return any(
            parameter.name == keyword
            or parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )

    def _run_decompose(
        self,
        orchestrator,
        prompt: str,
        depth: SearchDepth,
        language: str,
    ) -> list:
        kwargs = {"language": language} if self._accepts_keyword(
            orchestrator.run_decompose, "language"
        ) else {}
        return orchestrator.run_decompose(prompt, depth, **kwargs)

    def optimize_prompt(self, prompt: str, user_id: str | None = None) -> str:
        optimizer = self.require_agent(self.optimizer, "Prompt optimizer")
        with bind_observability_context(user_id=user_id or "local"):
            return optimizer.run(prompt)

    def list_tasks(
        self,
        limit: int = 100,
        offset: int = 0,
        user_id: str | None = None,
    ) -> list[SearchTask]:
        return self.task_store.get_all_tasks(limit=limit, offset=offset, user_id=user_id)

    def get_task(self, task_id: str, user_id: str | None = None) -> SearchTask | None:
        return self.task_store.get_task(task_id, user_id=user_id)

    def update_task(
        self,
        task_id: str,
        update: TaskUpdate,
        user_id: str | None = None,
    ) -> SearchTask | None:
        return self.task_store.update_task(task_id, update, user_id=user_id)

    def decompose_prompt(
        self,
        prompt: str,
        depth: SearchDepth,
        user_id: str | None = None,
    ) -> DecomposeResponse:
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        with bind_observability_context(user_id=user_id or "local"):
            language = detect_language(prompt)
            tasks_raw = self._run_decompose(orchestrator, prompt, depth, language)

        # This endpoint is a preview. Persisting these tasks or enqueueing jobs would
        # create orphaned work because no ResearchRecord owns the returned plan.
        return DecomposeResponse(
            tasks=[SearchTask.model_validate(task) for task in tasks_raw],
            depth=depth,
        )

    # Researches actively consuming search/LLM resources (vs. terminal or waiting on user).
    _RUNNING_STATUSES = {ResearchStatus.PROCESSING, ResearchStatus.ANALYZING}
    # A user's "in flight" research includes a queued one, so the per-user guard counts it.
    # Fresh parked plan-first researches hold the slot too — otherwise N parked plans
    # could be mass-activated past the limit via /plan/approve (ADMIT-ATOMIC).
    _IN_FLIGHT_STATUSES = (
        _RUNNING_STATUSES
        | {ResearchStatus.QUEUED, ResearchStatus.CLARIFYING, ResearchStatus.PLAN_REVIEW}
    )

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

    def _admission_cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(
            seconds=settings.research_stale_active_seconds
        )

    def _admit_or_raise(
        self,
        research_id: str,
        expected_status: ResearchStatus,
    ) -> None:
        admitted = self.task_store.try_admit_research(
            research_id,
            expected_status,
            settings.max_concurrent_researches,
            settings.max_global_active_researches,
            self._admission_cutoff(),
        )
        if not admitted:
            raise ConflictError(
                "Research capacity is currently full or the research state changed. Please retry."
            )

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
            # Atomic claim: only the process that flips QUEUED->PROCESSING runs decompose,
            # so concurrent workers/API processes never double-promote the same research.
            if not self.task_store.try_claim_queued_research(
                item.id, cap, self._admission_cutoff()
            ):
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
            self._mark_decompose_requested(item.id)  # it may have queued for longer than recovery waits
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
        # Persist decompose intent + webhook_url together so crash-recovery can retry (R-1).
        # Group researches into a conversation thread; a new id starts a new thread.
        thread_id = (request.thread_id or "").strip() or str(uuid.uuid4())
        language = detect_language(request.prompt)
        graph_state: dict = {
            "decompose_pending": True,
            "decompose_payload": request.model_dump(mode="json"),
            # Persist the validated model choice (falls back to default if unknown/unsafe).
            "model": resolve_model_id(request.model, settings.deepseek_model),
            "thread_id": thread_id,
        }
        if request.webhook_url:
            graph_state["webhook_url"] = str(request.webhook_url)
        research = self.task_store.add_research_if_under_limit(
            request,
            task_ids=[],
            user_id=user_id,
            graph_state=graph_state,
            per_user_limit=settings.max_concurrent_researches,
            global_limit=settings.max_global_active_researches,
            stale_before=self._admission_cutoff(),
            language=language,
        )
        if research is None:
            raise ConflictError(
                "A research is already in progress. Please wait for it to finish before starting another."
            )
        queued = research.status == ResearchStatus.QUEUED
        logger.info("research_created research_id=%s depth=%s queued=%s", research.id, request.depth.value, queued)
        return ResearchResponse(
            research_id=research.id,
            status="queued" if queued else "success",
            message="Queued — the system is at capacity; it will start automatically." if queued
            else "Research created. Task decomposition in progress…",
            thread_id=thread_id,
        ), research.id

    def _emit_plan_progress(
        self,
        research_id: str,
        step: str,
        agent: str = "OrchestratorAgent",
        phase: str = "plan",
        action: str = "plan",
        detail: str = "",
        metrics: dict | None = None,
    ) -> None:
        if not research_id or not hasattr(self.task_store, "append_research_graph_event"):
            return
        event = {
            "step": step,
            "agent": agent,
            "phase": phase,
            "action": action,
            "detail": detail,
        }
        if metrics:
            event["metrics"] = metrics
        try:
            self.task_store.append_research_graph_event(research_id, event)
        except Exception:
            pass

    def decompose_and_enqueue(self, research_id: str, request: ResearchRequest) -> None:
        """Background: run LLM decompose, create tasks, push to worker queue."""
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        research_context = self.task_store.get_research(research_id)
        with bind_observability_context(
            research_id=research_id,
            user_id=(research_context.user_id if research_context else None) or "local",
        ):
            try:
                research = research_context
                # Admission control: a queued research waits for promote_queued_researches.
                if research is not None and research.status == ResearchStatus.QUEUED:
                    logger.info("decompose_deferred_queued research_id=%s", research_id)
                    return
                # User cancelled before decompose ran — don't spend the LLM call or enqueue jobs.
                if research is not None and research.status in self._TERMINAL_STATUSES:
                    logger.info("decompose_skipped_terminal research_id=%s status=%s", research_id, research.status.value)
                    return
                graph_state = (research.graph_state if research else None) or {}
                language = self._research_language(research) if research else detect_language(
                    request.prompt
                )
                self._emit_plan_progress(
                    research_id,
                    "plan_start",
                    agent="OrchestratorAgent",
                    phase="plan",
                    action="analyze_prompt",
                    detail=trail_detail("plan_start", language),
                )
                # Clarify step (plan-first only, once): ask up to 3 questions before planning.
                if (
                    request.plan_first
                    and self.clarifier is not None
                    and not graph_state.get("clarified")
                ):
                    questions = self.clarifier.generate_questions(request.prompt)
                    if questions:
                        self._store_clarifications_for_review(research_id, questions)
                        self._emit_plan_progress(
                            research_id,
                            "clarify",
                            agent="ClarifierAgent",
                            phase="plan",
                            action="generate_questions",
                            detail=trail_detail("clarify", language, count=len(questions)),
                            metrics={"question_count": len(questions)},
                        )
                        logger.info(
                            "research_clarify_ready research_id=%s question_count=%s",
                            research_id, len(questions),
                        )
                        return
                effective_prompt = self._augment_prompt_with_clarifications(request.prompt, graph_state)
                self._emit_plan_progress(
                    research_id,
                    "decompose",
                    agent="OrchestratorAgent",
                    phase="plan",
                    action="decompose_topics",
                    detail=trail_detail("decompose", language, depth=request.depth.value),
                    metrics={"depth": request.depth.value},
                )
                tasks_raw = self._run_decompose(
                    orchestrator, effective_prompt, request.depth, language
                )
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
                    self._emit_plan_progress(
                        research_id,
                        "plan_review",
                        agent="OrchestratorAgent",
                        phase="plan",
                        action="awaiting_approval",
                        detail=trail_detail("plan_review", language, count=len(tasks_raw)),
                        metrics={"task_count": len(tasks_raw)},
                    )
                    logger.info(
                        "research_plan_ready research_id=%s item_count=%s depth=%s",
                        research_id, len(tasks_raw), request.depth.value,
                    )
                    return
                if not any(self._is_searchable(task_dict) for task_dict in tasks_raw):
                    self._fail_unsearchable_plan(research_id, len(tasks_raw))
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
                # Clear the crash-recovery marker (and the stored request) only now that the
                # plan has search jobs: a research failed before this point keeps its request,
                # so a retry decomposes it again instead of searching a plan with nothing in it.
                self._clear_decompose_pending(research_id)
                self._emit_plan_progress(
                    research_id,
                    "plan_ready",
                    agent="OrchestratorAgent",
                    phase="plan",
                    action="tasks_enqueued",
                    detail=trail_detail("plan_ready", language, count=len(registered_tasks)),
                    metrics={"task_count": len(registered_tasks), "enqueued_jobs": enqueued_jobs},
                )
                logger.info(
                    "research_decomposed research_id=%s task_count=%s depth=%s",
                    research_id,
                    len(registered_tasks),
                    request.depth.value,
                )
            except Exception as exc:
                logger.error("research_decompose_failed research_id=%s error=%s", research_id, str(exc))
                self.task_store.update_research_status(research_id, ResearchStatus.FAILED, self._failure_message(exc))

    NO_SEARCH_PLAN_REPORT = (
        "Could not generate a search plan (no searchable queries). "
        "Check the model/API key and try again."
    )

    @staticmethod
    def _is_searchable(task_raw: dict) -> bool:
        """A planned task that gets a search job: PENDING with at least one query (the
        orchestrator's parse fallback is a FAILED task, some plans come back without queries)."""
        status = TaskStatus(task_raw.get("status") or TaskStatus.PENDING)
        return status == TaskStatus.PENDING and bool(task_raw.get("queries"))

    def _fail_unsearchable_plan(self, research_id: str, task_count: int) -> None:
        """The decomposition produced nothing to search (non-JSON model output, tasks without
        queries). Fail cleanly instead of leaving the research stuck in 'processing', and
        persist none of the plan: the stored request stays, so a retry decomposes again."""
        logger.warning("research_decompose_no_queries research_id=%s task_count=%s", research_id, task_count)
        # Guarded: a cancel that landed during the decomposition stands.
        self.task_store.transition_research_status(
            research_id, [ResearchStatus.PROCESSING], ResearchStatus.FAILED, self.NO_SEARCH_PLAN_REPORT
        )
        self.task_store.merge_research_graph_state(
            research_id, remove_keys=["decompose_pending", "decompose_requested_at"]
        )

    def _clear_decompose_pending(self, research_id: str) -> None:
        """Remove the crash-recovery marker from graph_state after decompose completes."""
        self.task_store.merge_research_graph_state(
            research_id, remove_keys=["decompose_pending", "decompose_payload", "decompose_requested_at"]
        )

    @staticmethod
    def _decompose_marker(request: ResearchRequest | None = None) -> dict[str, Any]:
        """The crash-recovery marker for a decomposition starting now. Recovery ages it on
        decompose_requested_at: a research that waited in the queue, for clarification
        answers or for a retry has an old created_at, and aging on that started a second,
        concurrent decomposition next to the one just launched."""
        marker: dict[str, Any] = {
            "decompose_pending": True,
            "decompose_requested_at": datetime.now(timezone.utc).isoformat(),
        }
        if request is not None:
            marker["decompose_payload"] = request.model_dump(mode="json")
        return marker

    def _mark_decompose_requested(self, research_id: str, request: ResearchRequest | None = None) -> None:
        self.task_store.merge_research_graph_state(research_id, self._decompose_marker(request))

    @staticmethod
    def _decompose_requested_at(research: ResearchRecord) -> datetime | None:
        stamp = (research.graph_state or {}).get("decompose_requested_at") or research.created_at
        if isinstance(stamp, str):
            try:
                stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            except ValueError:
                return None
        if stamp is not None and stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return stamp

    def recover_pending_decompositions(self) -> int:
        """Re-schedule decompositions lost during a process crash.

        Scans recent PROCESSING researches for those that have ``decompose_pending=True``
        in graph_state, have no tasks yet, and whose decomposition was requested (or, for
        a research without ``decompose_requested_at``, created) more than
        ``settings.decompose_recovery_minutes`` minutes ago.  For each such research the
        request time is re-stamped and a fresh daemon thread replays
        ``decompose_and_enqueue``, so the next pass does not start a second one.

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
            requested_at = self._decompose_requested_at(research)
            if requested_at is None or requested_at > stale_threshold:
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
            self._mark_decompose_requested(research.id)
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

    # What a failed finalization leaves in graph_state: its graph checkpoint (a retry runs
    # a fresh pass rather than resuming the attempt that failed) and the trust artifacts
    # the panels would otherwise keep serving. The decompose marker goes too; a retry that
    # decomposes again sets it anew with decompose_requested_at, which recovery ages on.
    # canonical_sources goes back to "not computed": any stored list is the authoritative
    # [Sn] table, so the failed attempt's would keep serving /sources, verification and
    # chat until the retried analyze replaced it (never, if the retry fails before that).
    # llm_token_usage is the per-attempt figure; the retried finalize writes its own.
    _RETRY_RESET_GRAPH_STATE_KEYS = (
        "canonical_sources",
        "llm_token_usage",
        "error",
        "report",
        "step",
        "resume_after_stale_recovery",
        "analyze_attempts",
        "replan_attempts",
        "tie_break_attempts",
        "should_replan",
        "should_tie_break",
        "should_retry_analysis",
        "branch_stalled",
        "finalize_deadline",
        "replan_recommendations",
        "tie_break_recommendations",
        "detected_conflicts",
        "source_summary",
        "evidence_summary",
        "red_team",
        "citation_audit",
        "source_integrity",
        "source_independence",
        "source_reputation",
        "numeric_check",
        "comparison",
        "stance_balance",
        "cross_language",
        "decompose_pending",
        "decompose_requested_at",
    )

    def retry_research(
        self,
        research_id: str,
        user_id: str | None = None,
        background_tasks: Any | None = None,
    ) -> ResearchRecord:
        """Retry a failed research from where it failed: finalization when every search
        task completed, the unfinished search tasks otherwise, and decomposition when no
        task was ever created or dispatched.

        The retry is admitted like any activation (ADMIT-ATOMIC): FAILED -> PROCESSING is a
        CAS under the admission lock that counts against the per-user and global limits,
        so of two concurrent retries exactly one wins and the other gets 409."""
        research = (
            self._ensure_research_access(research_id, user_id)
            if user_id is not None
            else self.task_store.get_research(research_id)
        )
        if not research:
            raise NotFoundError("Research not found")
        if research.status != ResearchStatus.FAILED:
            raise ConflictError("Only failed research can be retried")

        # Chat follow-up searches are not part of the report: a failed one must not force the
        # search path (it would be redispatched as a report search), nor a completed one feed
        # the retried report, and they survive a replan along with the chat they answered.
        tasks = self._report_tasks(self.task_store.get_tasks_by_research(research_id))
        # A plan none of whose tasks was ever dispatched is what a failed decomposition used
        # to persist ('Could not generate a search plan'): searching it would only run the
        # parse fallback or empty tasks, so it is dropped and the request decomposed again.
        replan = not tasks or all(self._never_dispatched(task) for task in tasks)
        finalize_only = not replan and all(task.status == TaskStatus.COMPLETED for task in tasks)
        # Fail fast, before taking a capacity slot, when the retry path cannot run here.
        if replan:
            self.require_agent(self.orchestrator, "Orchestrator")
        elif finalize_only:
            self.require_agent(self.analyzer, "Analyzer")

        self._admit_or_raise(research_id, ResearchStatus.FAILED)
        reset = self.task_store.reset_research_for_retry(
            research_id,
            ResearchStatus.PROCESSING,
            list(self._RETRY_RESET_GRAPH_STATE_KEYS),
        )
        if reset is None:
            raise ConflictError("Research state changed. Please retry.")

        if replan:
            if tasks:
                self.task_store.delete_research_tasks(research_id, [task.id for task in tasks])
            self._retry_decomposition(reset, background_tasks)
        elif finalize_only:
            self._retry_finalization(research_id)
        else:
            self._redispatch_search_tasks(tasks, reset.depth)
        logger.info(
            "research_retried research_id=%s path=%s",
            research_id,
            "decompose" if replan else "finalize" if finalize_only else "search",
        )
        return self.task_store.get_research(research_id) or reset

    def _never_dispatched(self, task: SearchTask) -> bool:
        """Planned but never sent to a search worker: no job, no results and no log line
        (every search, retry or recovery writes one; job rows are cleaned up after a day)."""
        return (
            task.status in (TaskStatus.PENDING, TaskStatus.FAILED)
            and not task.result
            and not task.logs
            and self.task_store.get_latest_search_task_job(task.id) is None
        )

    def _retry_finalization(self, research_id: str) -> None:
        # The CAS is taken from PROCESSING (the admission state); pre-setting ANALYZING
        # would make try_begin_finalization refuse and leave no job at all.
        if not self.task_store.try_begin_finalization(research_id):
            return  # cancelled between the admission and here
        try:
            job = self._dispatch_finalize_job(research_id)
        except Exception as exc:
            # ANALYZING with no job would be stuck (stale recovery needs a RUNNING job and
            # retry a FAILED research): hand it back as FAILED so it can be retried at once.
            # A crash here instead is caught by the stalled-research sweep.
            self.task_store.transition_research_status(
                research_id, [ResearchStatus.ANALYZING], ResearchStatus.FAILED, self._failure_message(exc)
            )
            raise
        logger.info("research_retry_finalization finalize_job_id=%s", job.id)

    def _redispatch_search_tasks(self, tasks: list[SearchTask], depth: SearchDepth) -> None:
        """Send every unfinished task back to the search workers: FAILED, PENDING, and
        RUNNING without a running job (a replan/tie-break task runs inline in the finalize
        worker with no job, so a dead worker leaves it RUNNING for good). Every one of them
        is PENDING before the first job row is created or requeued: Postgres-polling workers
        claim a row at once, and a search finishing while a sibling was still FAILED would
        finalize the research without that sibling and drain its retry."""
        redispatch: list[tuple[SearchTask, SearchTaskJob | None]] = []
        for task in tasks:
            if task.status == TaskStatus.COMPLETED:
                continue
            job = self.task_store.get_latest_search_task_job(task.id)
            if job is not None and job.status == SearchJobStatus.RUNNING:
                continue  # a worker still holds it: its outcome (or stale recovery) settles it
            self.task_store.update_task(task.id, TaskUpdate(status=TaskStatus.PENDING, log="Task retried"))
            redispatch.append((task, job))
        for task, job in redispatch:
            # The job row always exists (Postgres-polling workers claim from it); the broker
            # push is only the wake-up for Redis-mode workers.
            if job is None or job.status == SearchJobStatus.COMPLETED:
                job = self.task_store.add_search_task_job(task.id, depth.value, settings.job_max_attempts)
            elif job.status != SearchJobStatus.PENDING:
                job = self.task_store.requeue_search_task_job(job.id) or job
            if self.broker:
                self.broker.push_search_job(job.id)

    def _retry_decomposition(self, research: ResearchRecord, background_tasks: Any | None) -> None:
        # Replay the original request (plan_first, model, thread) when it is still stored.
        graph_state = research.graph_state or {}
        payload = graph_state.get("decompose_payload")
        try:
            request = ResearchRequest.model_validate(payload) if payload else None
        except ValueError:
            request = None
        if request is None:
            request = ResearchRequest(
                prompt=research.prompt,
                depth=research.depth,
                model=graph_state.get("model"),
                thread_id=graph_state.get("thread_id"),
            )
        # The decomposition runs in an API background task: if that process dies first,
        # queue maintenance replays it from this marker (payload included).
        self._mark_decompose_requested(research.id, request)
        if background_tasks is not None:
            background_tasks.add_task(self.decompose_and_enqueue, research.id, request)
        else:
            self.decompose_and_enqueue(research.id, request)

    def rename_research(self, research_id: str, title: str, user_id: str | None = None) -> ResearchRecord:
        research = self._ensure_research_access(research_id, user_id)
        if not research:
            raise NotFoundError("Research not found")
        updated = self.task_store.merge_research_graph_state(
            research_id, {"title": title.strip()}
        )
        return updated or self.task_store.get_research(research_id)

    def get_research_status(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        return research

    def get_task_summary(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskSummary:
        task = self.task_store.get_task(task_id, user_id=user_id)
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
            max_groups=5,
        )
        claim_verification_summary = self.claim_verifier.verify_and_downgrade(
            research.final_report or "",
            self._research_language(research),
            [],
            [],
        )[1]
        # Follow-up recommendations require LLM calls and only make sense once every task branch
        # has finished — skip them while the research is still in progress (AUD-022) so an
        # on-demand /summary fetch mid-run doesn't burn LLM calls on premature suggestions.
        replan_recommendations = (
            self._summary_follow_up(research, tasks, source_critic_summary)
            if finalize_ready
            else []
        )
        graph_execution_summary = self._build_graph_execution_summary(tasks)

        partial_report = research.partial_report if not research.final_report else None

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

    # graph_state key for the /summary follow-ups. Not the graph's "replan_recommendations",
    # which the finalize graph writes only when a replan branch is possible (else []).
    _SUMMARY_FOLLOW_UP_KEY = "summary_follow_up"
    # A result that fell back to template queries because the LLM failed is kept only this
    # long: long enough that an outage does not cost LLM calls on every GET, short enough
    # that the templates are not served for good once the LLM is back.
    _SUMMARY_FOLLOW_UP_DEGRADED_TTL_SECONDS = 300

    @staticmethod
    def _tasks_fingerprint(tasks: list[SearchTask]) -> str:
        """Changes whenever a task is added or changes status — the inputs the follow-up
        recommendations are derived from (results only change with the status)."""
        digest = hashlib.sha256()
        for task in sorted(tasks, key=lambda item: item.id):
            digest.update(f"{task.id}:{task.status.value};".encode("utf-8"))
        return digest.hexdigest()

    def _summary_follow_up(
        self,
        research: ResearchRecord,
        tasks: list[SearchTask],
        source_summary: SourceCriticSummary,
    ) -> list[ReplanRecommendation]:
        """Compute the follow-up recommendations once per task set and store them (SUMMARY-LLM):
        they cost up to three LLM calls, and /summary used to pay that on every GET. A result
        degraded by LLM failures carries `retry_after` and is recomputed once that passes."""
        fingerprint = self._tasks_fingerprint(tasks)
        stored = (research.graph_state or {}).get(self._SUMMARY_FOLLOW_UP_KEY) or {}
        if stored.get("fingerprint") == fingerprint and not self._follow_up_retry_due(stored):
            return [ReplanRecommendation.model_validate(item) for item in stored.get("recommendations") or []]
        llm_failures: list[str] = []
        kwargs: dict[str, Any] = {"source_summary": source_summary}
        if self._accepts_keyword(self.replan_agent.suggest_follow_up, "llm_failures"):
            kwargs["llm_failures"] = llm_failures
        # Runs in the API process: bind the owner so the calls land in llm_usage_logs.
        with bind_observability_context(research_id=research.id, user_id=research.user_id or "local"):
            recommendations = self.replan_agent.suggest_follow_up(
                research.prompt,
                research.depth,
                tasks,
                **kwargs,
            )
        entry: dict[str, Any] = {
            "fingerprint": fingerprint,
            "recommendations": [item.model_dump() for item in recommendations],
        }
        if llm_failures:
            entry["retry_after"] = (
                datetime.now(timezone.utc)
                + timedelta(seconds=self._SUMMARY_FOLLOW_UP_DEGRADED_TTL_SECONDS)
            ).isoformat()
            logger.info(
                "summary_follow_up_degraded research_id=%s failed_gaps=%s",
                research.id, len(llm_failures),
            )
        self.task_store.merge_research_graph_state(research.id, {self._SUMMARY_FOLLOW_UP_KEY: entry})
        return recommendations

    @staticmethod
    def _follow_up_retry_due(stored: dict) -> bool:
        retry_after = stored.get("retry_after")
        if not retry_after:
            return False
        try:
            due = datetime.fromisoformat(str(retry_after))
        except ValueError:
            return True
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= due

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
        partial_report = research.partial_report if not research.final_report else None

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
        # Wait for the user; keep decompose_payload for the re-run.
        self.task_store.merge_research_graph_state(
            research_id,
            {"clarifications": {"questions": list(questions), "answers": []}},
            remove_keys=["decompose_pending"],
        )
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

        clar = dict((research.graph_state or {}).get("clarifications") or {})
        questions = clar.get("questions") or []
        answers = list(answers or [])
        clar["answers"] = answers
        clar["qa"] = [
            {"question": question, "answer": (answers[i] if i < len(answers) else "")}
            for i, question in enumerate(questions)
        ]

        payload = (research.graph_state or {}).get("decompose_payload")
        try:
            request = ResearchRequest.model_validate(payload) if payload else ResearchRequest(
                prompt=research.prompt, depth=research.depth, plan_first=True
            )
        except Exception:
            request = ResearchRequest(prompt=research.prompt, depth=research.depth, plan_first=True)

        self._admit_or_raise(research_id, ResearchStatus.CLARIFYING)
        self.task_store.merge_research_graph_state(
            research_id,
            {"clarifications": clar, "clarified": True, **self._decompose_marker()},
        )

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
        self.task_store.merge_research_graph_state(
            research_id,
            {"plan": plan},
            remove_keys=["decompose_pending", "decompose_payload"],
        )
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
        self.task_store.merge_research_graph_state(
            research_id, {"plan": [item.model_dump() for item in update.items]}
        )
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

        self._admit_or_raise(research_id, ResearchStatus.PLAN_REVIEW)

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

    _CHAT_HISTORY_LIMIT = 40  # cap conversation history

    def append_research_message(
        self,
        research_id: str,
        role: str,
        content: str,
        sources: list[SearchSourcePreview] | None = None,
    ) -> None:
        # Appended under the row lock: two concurrent chat turns each computing the list
        # from their own earlier read would drop one another's message.
        self.task_store.append_research_graph_state_item(
            research_id,
            "messages",
            ChatMessage(role=role, content=content, sources=sources or []).model_dump(),
            max_items=self._CHAT_HISTORY_LIMIT,
        )

    _CHAT_STOPWORDS = {
        "what", "which", "where", "when", "about", "could", "would", "should", "there",
        "their", "these", "those", "have", "this", "that", "with", "from", "into", "your",
        "tell", "explain", "give", "does", "they", "them", "than", "then",
        "что", "как", "какие", "какой", "почему", "когда", "где", "расскажи", "объясни", "дай",
    }

    def _question_needs_search(self, question: str, pool: list[dict]) -> bool:
        """Heuristic: True if the question's key terms aren't covered by the source pool."""
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
                "id": f"{self._CHAT_TASK_PREFIX}{uuid.uuid4()}",
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

    _CITED_SOURCE_ID = re.compile(r"\[(S\d+)\\?\]")

    def generate_research_answer(
        self,
        research_id: str,
        question: str,
        streaming_callback=None,
        status_callback=None,
    ) -> ChatMessage:
        """Grounded follow-up answer. Escalates to a mini web search when the existing
        source pool does not cover the question, then answers over the enriched pool."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        chat = self.require_agent(self.chat_agent, "Chat")

        tasks = self.task_store.get_tasks_by_research(research_id)
        # A copy: the report's pool is read-only here. Mini-search results are numbered after
        # its ids and live only in this answer's own sources, never in the canonical table.
        pool = list(self._report_source_pool(research, tasks))
        if self._question_needs_search(question, pool):
            if status_callback:
                status_callback("searching")
            new_sources = self._mini_search_for_chat(research_id, question, research.depth)
            seen_urls = {source.get("url") for source in pool if source.get("url")}
            source_numbers = [
                int(source_id[1:])
                for source in pool
                if (source_id := str(source.get("source_id") or "")).startswith("S")
                and source_id[1:].isdigit()
            ]
            next_source_number = max(source_numbers, default=0) + 1
            for source in new_sources:
                url = source.get("url")
                if not url or url in seen_urls:
                    continue
                pool.append({"source_id": f"S{next_source_number}", **source})
                seen_urls.add(url)
                next_source_number += 1
        # Retrieve the most relevant sources for this question (not just the first 12).
        ranked = self._rank_sources_for_question(question, pool, 12)
        sources = [
            {
                "source_id": item.get("source_id"),
                "title": item.get("title"),
                "domain": item.get("domain"),
                "url": item.get("url"),
                "source_quality": item.get("source_quality"),
                "extraction_status": item.get("extraction_status"),
                "content": (item.get("content") or "")[:800],
            }
            for item in ranked
            if item.get("source_id")
        ]
        history = list((research.graph_state or {}).get("messages") or [])
        model = (research.graph_state or {}).get("model")
        with bind_observability_context(
            research_id=research_id,
            user_id=research.user_id or "local",
        ):
            answer = chat.answer(
                question,
                research.final_report or "",
                sources,
                history,
                model=model,
                streaming_callback=streaming_callback,
            )
        # The model also sees the whole report, so it can cite report ids outside the
        # ranked 12; give every cited id its url/title (no content) so each one links.
        cited_ids = set(self._CITED_SOURCE_ID.findall(answer or ""))
        sent_ids = {source["source_id"] for source in sources}
        cited_only = [
            {key: item.get(key) for key in ("source_id", "title", "domain", "url", "source_quality", "extraction_status")}
            for item in pool
            if item.get("source_id") in cited_ids and item.get("source_id") not in sent_ids
        ]
        return ChatMessage(
            role="assistant",
            content=answer,
            sources=[
                SearchSourcePreview(
                    source_id=source["source_id"],
                    url=source.get("url") or "",
                    title=source.get("title"),
                    domain=source.get("domain"),
                    source_quality=source.get("source_quality"),
                    extraction_status=source.get("extraction_status"),
                    snippet=((source.get("content") or "")[:280] or None),
                )
                for source in sources + cited_only
                if source.get("url")
            ],
        )

    def get_research_sources(self, research_id: str) -> list[SearchSourcePreview]:
        """Canonical report source list for the artifact panel — no LLM."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        tasks = self.task_store.get_tasks_by_research(research_id)
        pool = self._report_source_pool(research, tasks)
        return [
            SearchSourcePreview(
                url=item.get("url", ""),
                source_id=item.get("source_id", ""),
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

        Returns only conflicts adjudicated and persisted during finalization. Legacy
        runs without an adjudicated result return an empty list.
        """
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")

        raw = (research.graph_state or {}).get("detected_conflicts")
        if raw is None:
            # Legacy runs have no adjudicated result. Do not expose heuristic candidates
            # as facts or trigger a surprise LLM call from this read-only endpoint.
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
        pool = self._report_source_pool(research, tasks)
        evidence_pool = [
            {"source_id": item.get("source_id", ""), "content": item.get("content", "")}
            for item in pool
            if item.get("source_id")
            if item.get("content")
        ]
        evidence_groups, _ = self.evidence_mapper.build_evidence_groups(
            evidence_pool,
            max_groups=6,
        )
        report = research.final_report or ""
        language = self._research_language(research)
        claim_summary = self.claim_verifier.verify_and_downgrade(report, language, [], [])[1]
        return self.report_critic.build(
            research_id,
            # Plan coverage is over the report's plan; a chat follow-up search is not a
            # sub-question the report was meant to answer.
            self._report_tasks(tasks),
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

        # A finalize worker must not enqueue follow-up work and then block waiting for the
        # same shared worker pool to consume it. Running the bounded replan wave inline
        # guarantees progress even when every worker is currently finalizing a HARD run.
        for task in created_tasks:
            self.run_search_task(task.id, depth)

        return [
            refreshed
            for refreshed in (self.task_store.get_task(task.id) for task in created_tasks)
            if refreshed is not None
        ]

    @staticmethod
    def _research_language(research: ResearchRecord) -> str:
        """Return the language stored at creation, with a fallback for legacy rows."""
        return research_language(research)

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
        self.task_store.merge_research_graph_state(research_id, graph_state)
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
            language = self._research_language(research)
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
            language = self._research_language(research)
            table = self.comparison_agent.build(
                research.prompt, report, language=language, model=settings.red_team_model
            )
            if not table.has_table:
                return
            table.research_id = research.id
            self.task_store.merge_research_graph_state(
                research.id, {"comparison": table.model_dump()}
            )
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

    _GRAPH_TRAIL_LABELS = {
        "ru": ("## Трасса выполнения графа", "Шаг", "Детали"),
        "en": ("## Graph Execution Trail", "Step", "Details"),
        "es": ("## Traza de ejecución del grafo", "Paso", "Detalles"),
    }

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

        heading, step_label, detail_label = self._GRAPH_TRAIL_LABELS.get(
            self._research_language(research), self._GRAPH_TRAIL_LABELS["en"]
        )
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

        # Chat follow-up tasks neither gate nor feed the report (see _report_tasks).
        tasks = self._report_tasks(self.task_store.get_tasks_by_research(research_id))
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
        target = self._webhook_log_target(url)
        if not settings.webhook_allow_private_targets:
            from src.net_safety import safe_post_json

            if safe_post_json(url, payload, timeout=10.0):
                logger.info("webhook_fired target=%s research_id=%s", target, research_id)
            return
        try:
            import httpx
            httpx.post(url, json=payload, timeout=10.0)
            logger.info("webhook_fired target=%s research_id=%s", target, research_id)
        except Exception as exc:
            # The exception text is not logged: httpx repeats the request URL in it.
            logger.warning(
                "webhook_failed target=%s research_id=%s error=%s", target, research_id, type(exc).__name__
            )

    @staticmethod
    def _webhook_log_target(url: str) -> str:
        """scheme://host[:port] of a webhook URL for the logs. Incoming-webhook URLs carry
        their credential in the path, query or userinfo, so none of those is ever logged."""
        from urllib.parse import urlsplit

        try:
            parts = urlsplit(url or "")
            host = parts.hostname or ""
            port = parts.port
        except ValueError:
            return "invalid-url"
        if not host:
            return "invalid-url"
        if ":" in host:
            host = f"[{host}]"  # IPv6 literal
        return f"{parts.scheme}://{host}" + (f":{port}" if port else "")

    def ensure_finalize_job_lease(
        self,
        job_id: str | None,
        lease_epoch: int | None,
    ) -> None:
        """Renew a finalize lease or stop a runner fenced off by stale-job recovery."""
        if job_id is None or lease_epoch is None:
            return
        if not self.task_store.renew_research_finalize_job_lease(job_id, lease_epoch):
            raise FinalizeLeaseLost(f"Finalize job {job_id} lease was lost")

    _FINALIZE_STEP_METADATA = {
        "redteam": {
            "agent": "RedTeamAgent",
            "phase": "verify",
            "action": "stress_test",
        },
        "audit": {
            "agent": "CitationAuditAgent",
            "phase": "verify",
            "action": "audit_citations",
        },
        "viewpoints": {
            "agent": "StanceAgent",
            "phase": "verify",
            "action": "stance_detection",
        },
        "completed": {
            "agent": "System",
            "phase": "complete",
            "action": "finish",
        },
    }

    def _emit_finalize_progress(
        self,
        research_id: str,
        step: str,
        finalize_job_id: str | None = None,
        lease_epoch: int | None = None,
        agent: str | None = None,
        phase: str | None = None,
        action: str | None = None,
        detail: str | None = None,
        metrics: dict | None = None,
        language: str | None = None,
    ) -> None:
        """Append a finalize-phase step to the live trail so the progress trace keeps moving
        during synthesis (and surfaces the trust/verification work as it happens). Labelled
        by the frontend via trace.{step}; the detail is written in the research's
        ``language``. Trail failures are best-effort; a lost lease must stop the fenced
        runner before it can publish a result."""
        self.ensure_finalize_job_lease(finalize_job_id, lease_epoch)
        store = self.task_store
        if not research_id or not hasattr(store, "append_research_graph_event"):
            return
        meta = self._FINALIZE_STEP_METADATA.get(step, {})
        event = {
            "step": step,
            "agent": agent or meta.get("agent", "FinalizeRunner"),
            "phase": phase or meta.get("phase", "verify"),
            "action": action or meta.get("action", step),
            "detail": detail or (trail_detail(step, language) if step in TRAIL_DETAILS else ""),
        }
        if metrics:
            event["metrics"] = metrics
        try:
            store.append_research_graph_event(research_id, event)
        except Exception:
            pass

    def _raise_if_cancelled(self, research_id: str) -> None:
        research = self.task_store.get_research(research_id)
        if research is not None and research.status == ResearchStatus.CANCELLED:
            raise FinalizeCancelled(research_id)

    def _finalize_stopped_cancelled(self, research_id: str) -> ResearchRecord:
        latest = self.task_store.get_research(research_id)
        if latest is None:
            raise NotFoundError("Research not found")
        logger.info("finalize_stopped_cancelled research_id=%s", research_id)
        return latest

    def _run_trust_suite(
        self,
        report: str,
        research: ResearchRecord,
        tasks: list[SearchTask],
        aggregated: list[dict] | None,
        finalize_job_id: str | None,
        lease_epoch: int | None,
    ) -> str:
        """Red-team, audits and viewpoint passes over the finished draft. A cancel is
        checked before every step (CANCEL-TRUST-SUITE), so a research cancelled during the
        last graph step stops spending here too; FinalizeCancelled ends the suite."""
        research_id = research.id
        language = self._research_language(research)
        self._raise_if_cancelled(research_id)
        self._emit_finalize_progress(research_id, "redteam", finalize_job_id, lease_epoch, language=language)
        report = self._maybe_red_team(report, research, tasks)
        self._raise_if_cancelled(research_id)
        self._emit_finalize_progress(research_id, "audit", finalize_job_id, lease_epoch, language=language)
        # Share the exact analyzer output across the trust steps. Legacy/custom analyzers
        # fall back to the persisted canonical table or deterministic reconstruction.
        if aggregated is None:
            aggregated = self._aggregated_sources(research, tasks)
        audit_steps = (
            lambda: self._audit_citations(report, research, tasks, aggregated=aggregated),
            lambda: self._analyze_source_independence(research, tasks, aggregated=aggregated),
            lambda: self._assess_source_reputation(research, tasks, aggregated=aggregated),
            lambda: self._check_numbers(report, research, tasks, aggregated=aggregated),
            lambda: self._check_retractions(research, tasks, aggregated=aggregated),
            lambda: self._maybe_build_comparison(report, research),
        )
        for step in audit_steps:
            self._raise_if_cancelled(research_id)
            step()
        self._raise_if_cancelled(research_id)
        self._emit_finalize_progress(research_id, "viewpoints", finalize_job_id, lease_epoch, language=language)
        self._maybe_assess_stance(research, tasks, aggregated=aggregated)
        self._raise_if_cancelled(research_id)
        self._analyze_cross_language(research, tasks, aggregated=aggregated)
        return report

    def complete_research_finalization(
        self,
        research_id: str,
        finalize_job_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> ResearchRecord:
        research_context = self.task_store.get_research(research_id)
        with bind_observability_context(
            research_id=research_id,
            user_id=(research_context.user_id if research_context else None) or "local",
        ):
            research = research_context
            if not research:
                raise NotFoundError("Research not found")
            if research.status == ResearchStatus.CANCELLED:
                logger.info("finalize_skipped_cancelled research_id=%s", research_id)
                # user cancelled — don't spend the analysis call or overwrite the status; return
                # the (cancelled) record so the contract stays ResearchRecord, never None (AUD-033)
                return research

            # The report is built from its own plan only, never from chat follow-up searches.
            tasks = self._report_tasks(self.task_store.get_tasks_by_research(research_id))
            analyzer = self.require_agent(self.analyzer, "Analyzer")

            # reset token counter before this analysis run
            # (analyzers may have no .llm — e.g. stub/static report agents)
            analyzer_llm = getattr(analyzer, "llm", None)
            if analyzer_llm is not None and hasattr(analyzer_llm, "reset_usage"):
                analyzer_llm.reset_usage()

            try:
                graph_result = self.finalize_graph_runner.run(
                    research_id,
                    research.prompt,
                    tasks,
                    research.depth,
                    finalize_job_id=finalize_job_id,
                    lease_epoch=lease_epoch,
                )
                if isinstance(graph_result, tuple) and len(graph_result) == 4:
                    report, aggregated, effective_prompt, final_tasks = graph_result
                else:  # compatibility with custom/test graph runners
                    report = graph_result
                    aggregated = None
                    effective_prompt = research.prompt
                    final_tasks = tasks
            except FinalizeCancelled:
                return self._finalize_stopped_cancelled(research_id)

            tasks = final_tasks
            source_state: dict[str, Any] = {
                "effective_prompt": effective_prompt,
                "task_ids": [task.id for task in tasks],
            }
            if aggregated is not None:
                source_state["canonical_sources"] = self._canonical_source_table(aggregated)
            self.ensure_finalize_job_lease(finalize_job_id, lease_epoch)
            self.task_store.merge_research_graph_state(research_id, source_state)
            research = self.task_store.get_research(research_id) or research

            try:
                report = self._run_trust_suite(
                    report, research, tasks, aggregated, finalize_job_id, lease_epoch
                )
            except FinalizeCancelled:
                return self._finalize_stopped_cancelled(research_id)
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
            # persist token usage into graph_state (U-3). This is the per-research figure the
            # UI shows; llm_usage_logs gets one row per call from the provider's usage sink,
            # so nothing is written there from here (that would count the calls twice).
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
                self.ensure_finalize_job_lease(finalize_job_id, lease_epoch)
                self.task_store.merge_research_graph_state(research_id, {"llm_token_usage": usage})

            if finalize_job_id is not None and lease_epoch is not None:
                completed_job = self.task_store.complete_research_finalize_job(
                    finalize_job_id,
                    research_id,
                    lease_epoch,
                    report,
                )
                if completed_job is None:
                    raise FinalizeLeaseLost(
                        f"Finalize job {finalize_job_id} lease was lost before commit"
                    )
            else:
                self.task_store.update_research_status(
                    research_id,
                    ResearchStatus.COMPLETED,
                    report,
                )

            # The commit above was the last fenced write; the job is no longer RUNNING, so
            # a lease renewal from here on could only fail (LEASE-COMPLETED). Post-commit
            # work is best-effort and must never reach the caller's failure or lease-lost
            # handling for a finalization that has already been committed.
            try:
                return self._after_finalize_commit(research_id, research)
            except Exception as exc:
                logger.warning("finalize_post_commit_failed error=%s", exc)
                return research

    def _after_finalize_commit(self, research_id: str, research: ResearchRecord) -> ResearchRecord:
        finalized_research = self.task_store.get_research(research_id) or research
        if finalized_research.status == ResearchStatus.CANCELLED:
            logger.info("finalize_discarded_cancelled research_id=%s", research_id)
            return finalized_research

        self._emit_finalize_progress(
            research_id, "completed", language=self._research_language(finalized_research)
        )
        logger.info("research_finalize_completed")

        # fire webhook if configured (F-1)
        webhook_url = (research.graph_state or {}).get("webhook_url")
        if webhook_url:
            self._fire_webhook(
                webhook_url,
                research_id,
                {"research_id": research_id, "status": "completed"},
            )

        return finalized_research

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
            job = self._dispatch_finalize_job(research_id)
            logger.info("research_finalize_enqueued finalize_job_id=%s", job.id)
            return self.task_store.get_research(research_id), job

    def _dispatch_finalize_job(self, research_id: str) -> ResearchFinalizeJob:
        """The finalize job for a research that just won the ANALYZING CAS: its latest job
        while that is still queued or held by a runner (never a second one), the latest one
        requeued when it stopped (DEAD_LETTER/FAILED: store-guarded, lease bumped), else a
        fresh job. Reusing the stopped job keeps it from lingering in the dead-letter list
        after a retry, where requeueing it would rewind the research."""
        latest = self.task_store.get_latest_research_finalize_job(research_id)
        job = None
        if latest is not None and latest.status in (FinalizeJobStatus.PENDING, FinalizeJobStatus.RUNNING):
            job = latest
        elif latest is not None:
            job = self.task_store.requeue_research_finalize_job(latest.id)  # None when COMPLETED
        if job is None:
            job = self.task_store.add_research_finalize_job(research_id, settings.job_max_attempts)
        if self.broker and job.status == FinalizeJobStatus.PENDING:
            self.broker.push_finalize_job(job.id)
        return job

    def process_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.task_store.get_research_finalize_job(job_id)
        if job is None:
            return None
        if job.status == FinalizeJobStatus.PENDING:
            job = self.task_store.claim_research_finalize_job_by_id(job_id)
            if job is None:
                return self.task_store.get_research_finalize_job(job_id)
        if job.status != FinalizeJobStatus.RUNNING:
            return job
        lease_epoch = job.lease_epoch

        with bind_observability_context(job_id=job.id, research_id=job.research_id):
            try:
                logger.info("finalize_job_processing")
                finalized_research = self.complete_research_finalization(
                    job.research_id,
                    finalize_job_id=job.id,
                    lease_epoch=lease_epoch,
                )
                completed_job = self.task_store.get_research_finalize_job(job_id)
                if finalized_research.status == ResearchStatus.CANCELLED and (
                    completed_job is not None
                    and completed_job.status == FinalizeJobStatus.RUNNING
                ):
                    completed_job = self.task_store.update_research_finalize_job(
                        job_id,
                        FinalizeJobStatus.COMPLETED,
                        lease_epoch=lease_epoch,
                    )
                if completed_job is None or completed_job.status != FinalizeJobStatus.COMPLETED:
                    raise FinalizeLeaseLost(f"Finalize job {job_id} lease was lost")
                logger.info("finalize_job_completed")
                return completed_job
            except FinalizeLeaseLost:
                logger.warning(
                    "finalize_job_lease_lost lease_epoch=%s",
                    lease_epoch,
                )
                return self.task_store.get_research_finalize_job(job_id)
            except Exception as exc:
                failed_job = self.task_store.record_research_finalize_job_failure(
                    job_id,
                    str(exc),
                    lease_epoch=lease_epoch,
                )
                if failed_job is None:
                    logger.warning(
                        "finalize_job_failure_discarded_after_lease_loss lease_epoch=%s",
                        lease_epoch,
                    )
                    return self.task_store.get_research_finalize_job(job_id)
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
            events = self._append_operational_recommendation_event(
                maintenance_summary.get("recent_operational_recommendation_events") or [],
                code=str(updated_recommendation.get("code") or code),
                event_type=event_type,
                message=str(updated_recommendation.get("message") or ""),
                timestamp=current_timestamp,
                note=event_note,
            )
            # JSON-ready: the SQL store writes this dict straight into a JSONB column, where
            # the RecommendationEvent models raised TypeError (ack/resolve were a 500).
            maintenance_summary["recent_operational_recommendation_events"] = [
                event.model_dump(mode="json") for event in events
            ]
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

    def get_health_summary(self) -> dict:
        """Cheap readiness signal for load balancers / healthchecks: pings only."""
        db_ok = self.task_store.ping()
        redis_status = "disabled" if self.broker is None else ("ok" if self.broker.ping() else "down")
        llm_status = "ok" if self.llm_available else "down"
        dependencies = {
            "database": "ok" if db_ok else "down",
            "redis": redis_status,
            "llm": llm_status,
        }
        return {
            "status": "ok" if db_ok and redis_status != "down" and llm_status == "ok" else "degraded",
            "dependencies": dependencies,
        }

    def get_health_status(self) -> dict:
        # Probe dependencies first so /health is a real readiness signal (AUD-036).
        db_ok = self.task_store.ping()
        redis_status = "disabled" if self.broker is None else ("ok" if self.broker.ping() else "down")
        llm_status = "ok" if self.llm_available else "down"
        dependencies = {
            "database": "ok" if db_ok else "down",
            "redis": redis_status,
            "llm": llm_status,
        }
        if not db_ok:
            return {"status": "degraded", "dependencies": dependencies}
        graph_metrics = GraphMetrics.model_validate(get_graph_metrics_snapshot())
        step_events = self._filter_graph_step_events()
        queue_metrics = self.get_queue_metrics()
        return {
            "status": "ok" if redis_status != "down" and llm_status == "ok" else "degraded",
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

    def _maybe_enqueue_finalization(self, research_id: str) -> None:
        research = self.task_store.get_research(research_id)
        if research is None or research.status in (
            ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED, ResearchStatus.CANCELLED
        ):
            return
        tasks = self._report_tasks(self.task_store.get_tasks_by_research(research_id))
        if not tasks:
            return
        if all(self._search_settled(task) for task in tasks):
            logger.info("research_search_complete_auto_finalize research_id=%s task_count=%s", research_id, len(tasks))
            self.enqueue_research_finalization(research_id)  # idempotent via status guard

    _ACTIVE_SEARCH_JOB_STATUSES = (SearchJobStatus.PENDING, SearchJobStatus.RUNNING)

    def _search_settled(self, task: SearchTask) -> bool:
        """COMPLETED, or FAILED with no search job left to run. SearchAgent marks a task
        FAILED before its worker records the failure and schedules the retry; counting it
        settled in that window let a sibling finalize the research and the retry be drained.
        Every job settles through process_search_task_job, which re-checks afterwards, so
        a dead-lettered task still lets the research finalize. Only FAILED tasks cost a
        job lookup; replan/tie-break tasks have no job and are settled as they stand."""
        if task.status == TaskStatus.COMPLETED:
            return True
        if task.status != TaskStatus.FAILED:
            return False
        job = self.task_store.get_latest_search_task_job(task.id)
        return job is None or job.status not in self._ACTIVE_SEARCH_JOB_STATUSES

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
        task_id, research_id = task.id, task.research_id

        with bind_observability_context(job_id=job.id, task_id=task_id, research_id=research_id):
            # If the user cancelled (or the research otherwise ended) while this job sat in
            # the queue, don't spend search/extraction on it — drain the job and move on.
            # Once finalization has begun (ANALYZING) a late retry would only run alongside
            # it and its results would never be used, so it is drained the same way.
            research = self.task_store.get_research(research_id) if research_id else None
            if research is not None and (
                research.status in self._TERMINAL_STATUSES
                or research.status == ResearchStatus.ANALYZING
            ):
                logger.info("search_job_skipped_terminal status=%s", research.status.value)
                return self.task_store.update_search_task_job(
                    job_id, SearchJobStatus.COMPLETED, "Research no longer active — search skipped"
                )
            # Auto-finalize runs only after the retry decision (JOB-RETRY-ORDER): SearchAgent
            # records a failure as a FAILED task without raising, and finalizing on that
            # before the job is rescheduled would finish the research without the retry.
            # Nothing else triggers finalization, so every settled outcome below calls it.
            try:
                logger.info("search_job_processing depth=%s", job.depth.value)
                self.run_search_task(task_id, job.depth)
                task = self.task_store.get_task(task_id)
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
                        self._schedule_search_retry(task_id, failed_job)
                    if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                        logger.error("search_job_dead_letter")
                        # Out of retries: the FAILED task is settled and the research can
                        # finalize on what the other tasks found.
                        self._maybe_finalize_after_search(research_id)
                    return failed_job

                completed_job = self.task_store.update_search_task_job(job_id, SearchJobStatus.COMPLETED)
                logger.info("search_job_completed")
                self._maybe_finalize_after_search(research_id)
                return completed_job
            except Exception as exc:
                failed_job = self.task_store.record_search_task_job_failure(job_id, str(exc))
                logger.warning(
                    "search_job_exception error=%s next_status=%s",
                    str(exc),
                    failed_job.status.value if failed_job else "missing",
                )
                if failed_job and failed_job.status == SearchJobStatus.PENDING:
                    self._schedule_search_retry(task_id, failed_job)
                if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                    logger.error("search_job_dead_letter")
                    # The exception can leave the task RUNNING; settle it as FAILED first,
                    # or the research would wait on it in 'processing' forever.
                    self.task_store.update_task(
                        task_id,
                        TaskUpdate(
                            status=TaskStatus.FAILED,
                            log="Search job failed after all retries",
                        ),
                    )
                    self._maybe_finalize_after_search(research_id)
                return failed_job

    def _schedule_search_retry(self, task_id: str, job: SearchTaskJob) -> None:
        self.task_store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.PENDING,
                log="Search job scheduled for retry",
            ),
        )
        if self.broker:
            self.broker.push_search_job(job.id)
        logger.info("search_job_retry_scheduled")

    def _maybe_finalize_after_search(self, research_id: str | None) -> None:
        if research_id:
            self._maybe_enqueue_finalization(research_id)

    # ── Admin Panel Methods ───────────────────────────────────────────────────
    def get_admin_overview(self) -> AdminOverviewResponse:
        """Store counters plus the dependency probes /health uses (OPS-BOOT-HEALTH): the
        store sees neither the LLM nor the broker, so it must not decide overall health."""
        overview = self.task_store.get_admin_overview()
        health = self.get_health_summary()
        dependencies = health["dependencies"]
        healthy = health["status"] == "ok" and overview.failed_tasks_count == 0
        system_health = {
            "postgres": dependencies["database"],
            "redis": dependencies["redis"],
            "llm": dependencies["llm"],
            "overall": "healthy" if healthy else "degraded",
        }
        return overview.model_copy(update={"system_health": system_health})

    # The totals and breakdowns of the admin Tokens tab aggregate all of llm_usage_logs
    # (one row per LLM call), and the tab asks again on every page change: they are reused
    # for this long in a process, while the per-research page is always read fresh.
    TOKEN_ANALYTICS_CACHE_SECONDS = 30.0

    def get_admin_token_analytics(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> AdminTokenAnalyticsResponse:
        now = time.monotonic()
        cached = self._token_analytics_cache
        if cached is not None and now - cached[0] < self.TOKEN_ANALYTICS_CACHE_SECONDS:
            researches = self.task_store.get_admin_token_research_usage(page=page, page_size=page_size)
            return cached[1].model_copy(update={"researches": researches, "page": page, "page_size": page_size})
        analytics = self.task_store.get_admin_token_analytics(page=page, page_size=page_size)
        self._token_analytics_cache = (now, analytics)
        return analytics

    def get_admin_audit_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        actor_email: str | None = None,
    ) -> list[AdminAuditLogItem]:
        return self.task_store.get_admin_audit_logs(
            limit=limit,
            offset=offset,
            action=action,
            actor_email=actor_email,
        )

    # Defaults when an admin action omits its window: the job timeouts for stale recovery,
    # and the same age limits the admin panel offers for cleanup.
    _MAINTENANCE_DEFAULT_DAYS = {"cleanup_old_jobs": 7, "cleanup_search_cache": 3}

    def _resolve_maintenance_params(self, request: MaintenanceActionRequest) -> dict:
        """Fill in defaults and refuse a stale window below the configured job timeout:
        recovering a younger RUNNING job re-dispatches work that is still making progress
        (and fences a live finalize runner)."""
        params = request.params.model_dump(exclude_none=True)
        timeout = {
            "recover_stale_finalize_jobs": settings.finalize_job_timeout_seconds,
            "recover_stale_search_jobs": settings.search_job_timeout_seconds,
        }.get(request.action)
        if timeout is not None:
            params.setdefault("stale_seconds", timeout)
            if params["stale_seconds"] < timeout:
                raise UnprocessableError(
                    f"stale_seconds must be at least the job timeout ({timeout} seconds)"
                )
        if request.action in self._MAINTENANCE_DEFAULT_DAYS:
            params.setdefault("days", self._MAINTENANCE_DEFAULT_DAYS[request.action])
        return params

    def preview_maintenance_action(self, request: MaintenanceActionRequest) -> AdminDryRunResult:
        params = self._resolve_maintenance_params(request)
        return self.task_store.preview_maintenance_action(request.action, params)

    def execute_maintenance_action(
        self,
        request: MaintenanceActionRequest,
        actor_email: str,
        ip_address: str | None = None,
    ) -> AdminDryRunResult:
        """Run an admin maintenance action through the same service paths the workers use
        (ADMIN-MAINTENANCE): recovery and requeue re-dispatch to the broker, keep the
        dead-letter guard and reset task/research status; the store only counts and audits."""
        params = self._resolve_maintenance_params(request)
        action = request.action
        sample_ids: list[str] = []
        if action == "recover_stale_finalize_jobs":
            recovery = self.recover_stale_research_finalize_jobs(stale_seconds=params["stale_seconds"])
            affected, sample_ids = recovery.recovered_count, recovery.recovered_job_ids
            summary = f"Recovered {affected} stale finalize jobs"
        elif action == "recover_stale_search_jobs":
            recovery = self.recover_stale_search_task_jobs(stale_seconds=params["stale_seconds"])
            affected, sample_ids = recovery.recovered_count, recovery.recovered_job_ids
            summary = f"Recovered {affected} stale search jobs"
        elif action == "cleanup_old_jobs":
            cutoff = datetime.now(timezone.utc) - timedelta(days=params["days"])
            finalize = self.cleanup_old_research_finalize_jobs(older_than=cutoff)
            search = self.cleanup_old_search_task_jobs(older_than=cutoff)
            sample_ids = finalize.deleted_job_ids + search.deleted_job_ids
            affected = len(sample_ids)
            summary = f"Deleted {finalize.deleted_count} finalize and {search.deleted_count} search jobs"
        elif action == "cleanup_search_cache":
            cutoff = datetime.now(timezone.utc) - timedelta(days=params["days"])
            affected = self.cleanup_search_cache(older_than=cutoff)
            summary = f"Cleaned up {affected} search cache entries"
        elif action == "requeue_finalize_job":
            sample_ids = [self.requeue_research_finalize_job(params["target_id"]).id]
            affected = 1
            summary = f"Requeued finalize job {params['target_id']}"
        else:  # requeue_search_job
            sample_ids = [self.requeue_search_task_job(params["target_id"]).id]
            affected = 1
            summary = f"Requeued search job {params['target_id']}"

        self.task_store.record_admin_audit(
            actor_email=actor_email,
            action=action,
            target_type="maintenance",
            target_id=params.get("target_id"),
            details={"params": params, "affected_count": affected, "summary": summary},
            ip_address=ip_address,
        )
        return AdminDryRunResult(
            action=action,
            dry_run=False,
            affected_count=affected,
            sample_affected_ids=sample_ids[:10],
            summary=summary,
        )

    def get_agents_catalog(self) -> list[AgentMetadataItem]:
        return AGENTS_CATALOG
