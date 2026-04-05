import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.agents.claim_verifier import ClaimVerifierAgent
from src.agents.evidence_mapper import EvidenceMapperAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.replan import ReplanAgent
from src.agents.search import SearchAgent
from src.agents.source_critic import SourceCriticAgent
from src.api.schemas import (
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
    ResearchRecord,
    ResearchGraphResponse,
    ResearchRequest,
    ResearchResponse,
    ResearchReportResponse,
    ResearchSummary,
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
from src.graph import FinalizeGraphRunner
from src.graph.metrics import get_graph_metrics_snapshot, get_graph_step_events_snapshot
from src.observability import bind_observability_context
from src.providers.search import get_extraction_metrics_snapshot
from src.repositories.protocols import TaskStore
from src.search_depth_profiles import get_depth_profile

logger = logging.getLogger(__name__)


class ResearchService:
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
        replan_agent: ReplanAgent | None = None,
    ):
        self.task_store = task_store
        self.optimizer = optimizer
        self.orchestrator = orchestrator
        self.analyzer = analyzer
        self.source_critic = source_critic or SourceCriticAgent()
        self.evidence_mapper = evidence_mapper or EvidenceMapperAgent()
        self.claim_verifier = claim_verifier or ClaimVerifierAgent()
        self.replan_agent = replan_agent or ReplanAgent()
        self.finalize_graph_runner = FinalizeGraphRunner(self)

    def require_agent(self, agent, agent_name: str):
        if agent is None:
            raise HTTPException(
                status_code=503,
                detail=f"{agent_name} is unavailable. Check service configuration.",
            )
        return agent

    def optimize_prompt(self, prompt: str) -> str:
        optimizer = self.require_agent(self.optimizer, "Prompt optimizer")
        return optimizer.run(prompt)

    def list_tasks(self) -> list[SearchTask]:
        return self.task_store.get_all_tasks()

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
                self.task_store.add_search_task_job(task.id, depth.value, settings.job_max_attempts)

        return DecomposeResponse(tasks=registered_tasks, depth=depth)

    def start_research(
        self,
        request: ResearchRequest,
    ) -> ResearchResponse:
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        tasks_raw = orchestrator.run_decompose(request.prompt, request.depth)

        task_ids = []
        registered_tasks = []
        research = self.task_store.add_research(request, task_ids=[])

        with bind_observability_context(research_id=research.id):
            for task_dict in tasks_raw:
                task_dict["research_id"] = research.id
                task = self.task_store.add_task(task_dict)
                registered_tasks.append(task)
                task_ids.append(task.id)

            self.task_store.set_research_task_ids(research.id, task_ids)

            for task in registered_tasks:
                if task.status == TaskStatus.PENDING and task.queries:
                    self.task_store.add_search_task_job(task.id, request.depth.value, settings.job_max_attempts)

            logger.info(
                "research_started task_count=%s depth=%s",
                len(registered_tasks),
                request.depth.value,
            )

        return ResearchResponse(
            research_id=research.id,
            status="success",
            message=f"Research started with {len(registered_tasks)} tasks.",
        )

    def get_research_status(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        return research

    def get_task_summary(self, task_id: str) -> SearchTaskSummary:
        task = self.task_store.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return self._build_task_summary(task)

    def get_research_summary(self, research_id: str) -> ResearchSummary:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

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
        replan_recommendations = self.replan_agent.suggest_follow_up(
            research.prompt,
            research.depth,
            tasks,
            source_summary=source_critic_summary,
        )
        graph_execution_summary = self._build_graph_execution_summary(tasks)

        return ResearchSummary(
            id=research.id,
            prompt=research.prompt,
            depth=research.depth,
            status=research.status,
            task_ids=research.task_ids,
            created_at=research.created_at,
            updated_at=research.updated_at,
            has_final_report=bool(research.final_report),
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
        )

    def get_research_report(self, research_id: str) -> ResearchReportResponse:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")
        return ResearchReportResponse(
            research_id=research.id,
            status=research.status,
            final_report=research.final_report,
        )

    def get_research_graph(self, research_id: str) -> ResearchGraphResponse:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")
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
        aggregated_sources: list[dict] = []
        for task in tasks:
            for result in task.result or []:
                url = result.get("url")
                content = result.get("content")
                if not url or not content:
                    continue
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
            raise HTTPException(status_code=404, detail="Research not found")

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
        self.task_store.update_research_graph_state(research_id, graph_state)
        if event is not None:
            self.task_store.append_research_graph_event(research_id, event)

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
        for entry in research.graph_trail[-8:]:
            step = entry.get("step") or "unknown"
            detail = entry.get("detail") or ""
            lines.append(f"- {step_label}: {step}. {detail_label}: {detail}")
        return f"{report.rstrip()}\n\n" + "\n".join(lines)

    def _get_research_for_finalization(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research

        tasks = self.task_store.get_tasks_by_research(research_id)
        all_done = all(t.status in [TaskStatus.COMPLETED, TaskStatus.FAILED] for t in tasks)
        any_failed = any(t.status == TaskStatus.FAILED for t in tasks)

        if not tasks:
            raise HTTPException(status_code=409, detail="Research has no tasks to finalize")

        if not all_done:
            raise HTTPException(status_code=409, detail="Research tasks are still in progress")

        if any_failed and all(t.status == TaskStatus.FAILED for t in tasks):
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.FAILED,
                "All tasks failed.",
            )
            return self.task_store.get_research(research_id)

        return research

    def complete_research_finalization(self, research_id: str) -> ResearchRecord:
        with bind_observability_context(research_id=research_id):
            research = self.task_store.get_research(research_id)
            if not research:
                raise HTTPException(status_code=404, detail="Research not found")

            tasks = self.task_store.get_tasks_by_research(research_id)
            analyzer = self.require_agent(self.analyzer, "Analyzer")
            if settings.use_langgraph_finalize_graph:
                report = self.finalize_graph_runner.run(research_id, research.prompt, tasks, research.depth)
            else:
                report = analyzer.run_analysis(research.prompt, tasks, depth=research.depth)
            report = self._inject_graph_execution_trail(report, research_id)
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.COMPLETED,
                report,
            )
            logger.info("research_finalize_completed")

            return self.task_store.get_research(research_id)

    def enqueue_research_finalization(self, research_id: str) -> tuple[ResearchRecord, ResearchFinalizeJob | None]:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research, None

        with bind_observability_context(research_id=research_id):
            self.require_agent(self.analyzer, "Analyzer")
            self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
            job = self.task_store.add_research_finalize_job(research_id, settings.job_max_attempts)
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
                if failed_job and failed_job.status == FinalizeJobStatus.DEAD_LETTER:
                    self.task_store.update_research_status(
                        job.research_id,
                        ResearchStatus.FAILED,
                        f"Analysis failed: {str(exc)}",
                    )
                    logger.error("finalize_job_dead_letter")
                return failed_job

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        return self.task_store.get_research_finalize_job(job_id)

    def get_latest_research_finalize_job(self, research_id: str) -> ResearchFinalizeJob | None:
        return self.task_store.get_latest_research_finalize_job(research_id)

    def list_running_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        return self.task_store.get_running_research_finalize_jobs()

    def list_dead_letter_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        return self.task_store.get_dead_letter_research_finalize_jobs()

    def requeue_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob:
        job = self.task_store.get_research_finalize_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        if job.status != FinalizeJobStatus.DEAD_LETTER:
            raise HTTPException(status_code=409, detail="Only dead-letter finalize jobs can be requeued")

        self.require_agent(self.analyzer, "Analyzer")
        self.task_store.update_research_status(job.research_id, ResearchStatus.ANALYZING)
        requeued = self.task_store.requeue_research_finalize_job(job_id)
        if requeued is None:
            raise HTTPException(status_code=404, detail="Finalize job not found")
        logger.info("finalize_job_requeued job_id=%s research_id=%s", job.id, job.research_id)
        return requeued

    def recover_stale_research_finalize_jobs(self) -> JobRecoveryResponse:
        stale_before = datetime.now(timezone.utc) - timedelta(seconds=settings.finalize_job_timeout_seconds)
        recovered_jobs = self.task_store.recover_stale_research_finalize_jobs(stale_before)
        for job in recovered_jobs:
            self.task_store.update_research_status(job.research_id, ResearchStatus.ANALYZING)
            research = self.task_store.get_research(job.research_id)
            graph_state = (research.graph_state if research else None) or {}
            resume_step = graph_state.get("step") or "unknown"
            self.checkpoint_graph_state(
                job.research_id,
                {
                    **graph_state,
                    "resume_after_stale_recovery": True,
                },
                {
                    "step": "stale_recovered",
                    "detail": f"Finalize job {job.id} recovered after timeout; resume_from={resume_step}",
                },
            )
            logger.warning("finalize_job_recovered job_id=%s research_id=%s", job.id, job.research_id)
        return JobRecoveryResponse(
            recovered_job_ids=[job.id for job in recovered_jobs],
            recovered_count=len(recovered_jobs),
        )

    def cleanup_old_research_finalize_jobs(self) -> JobCleanupResponse:
        older_than = datetime.now(timezone.utc) - timedelta(seconds=settings.finalize_job_retention_seconds)
        deleted_ids = self.task_store.cleanup_old_research_finalize_jobs(older_than)
        if deleted_ids:
            logger.info("finalize_jobs_cleaned deleted_count=%s", len(deleted_ids))
        return JobCleanupResponse(
            deleted_job_ids=deleted_ids,
            deleted_count=len(deleted_ids),
        )

    def get_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        return self.task_store.get_search_task_job(job_id)

    def list_running_search_task_jobs(self) -> list[SearchTaskJob]:
        return self.task_store.get_running_search_task_jobs()

    def list_dead_letter_search_task_jobs(self) -> list[SearchTaskJob]:
        return self.task_store.get_dead_letter_search_task_jobs()

    def requeue_search_task_job(self, job_id: str) -> SearchTaskJob:
        job = self.task_store.get_search_task_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Search job not found")
        if job.status != SearchJobStatus.DEAD_LETTER:
            raise HTTPException(status_code=409, detail="Only dead-letter search jobs can be requeued")

        task = self.task_store.get_task(job.task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")

        self.task_store.update_task(
            task.id,
            TaskUpdate(status=TaskStatus.PENDING, log="Search job manually requeued"),
        )
        requeued = self.task_store.requeue_search_task_job(job_id)
        if requeued is None:
            raise HTTPException(status_code=404, detail="Search job not found")
        logger.info("search_job_requeued job_id=%s task_id=%s", job.id, task.id)
        return requeued

    def recover_stale_search_task_jobs(self) -> JobRecoveryResponse:
        stale_before = datetime.now(timezone.utc) - timedelta(seconds=settings.search_job_timeout_seconds)
        recovered_jobs = self.task_store.recover_stale_search_task_jobs(stale_before)
        for job in recovered_jobs:
            self.task_store.update_task(
                job.task_id,
                TaskUpdate(status=TaskStatus.PENDING, log="Recovered stale running search job"),
            )
            logger.warning("search_job_recovered job_id=%s task_id=%s", job.id, job.task_id)
        return JobRecoveryResponse(
            recovered_job_ids=[job.id for job in recovered_jobs],
            recovered_count=len(recovered_jobs),
        )

    def cleanup_old_search_task_jobs(self) -> JobCleanupResponse:
        older_than = datetime.now(timezone.utc) - timedelta(seconds=settings.search_job_retention_seconds)
        deleted_ids = self.task_store.cleanup_old_search_task_jobs(older_than)
        if deleted_ids:
            logger.info("search_jobs_cleaned deleted_count=%s", len(deleted_ids))
        return JobCleanupResponse(
            deleted_job_ids=deleted_ids,
            deleted_count=len(deleted_ids),
        )

    def compact_graph_operational_data(self) -> tuple[list[str], list[str]]:
        compacted_worker_names = self.task_store.compact_worker_graph_step_events()
        compacted_research_ids = self.task_store.compact_research_graph_trails()
        if compacted_worker_names or compacted_research_ids:
            logger.info(
                "graph_operational_data_compacted worker_count=%s research_count=%s",
                len(compacted_worker_names),
                len(compacted_research_ids),
            )
        return compacted_worker_names, compacted_research_ids

    def run_queue_maintenance(self) -> QueueMaintenanceResponse:
        search_recovery = self.recover_stale_search_task_jobs()
        finalize_recovery = self.recover_stale_research_finalize_jobs()
        search_cleanup = self.cleanup_old_search_task_jobs()
        finalize_cleanup = self.cleanup_old_research_finalize_jobs()
        compacted_worker_names, compacted_research_ids = self.compact_graph_operational_data()

        recovered_count = search_recovery.recovered_count + finalize_recovery.recovered_count
        deleted_count = search_cleanup.deleted_count + finalize_cleanup.deleted_count
        compacted_count = len(compacted_worker_names) + len(compacted_research_ids)

        return QueueMaintenanceResponse(
            recovered_search_job_ids=search_recovery.recovered_job_ids,
            recovered_finalize_job_ids=finalize_recovery.recovered_job_ids,
            deleted_search_job_ids=search_cleanup.deleted_job_ids,
            deleted_finalize_job_ids=finalize_cleanup.deleted_job_ids,
            compacted_graph_event_worker_names=compacted_worker_names,
            compacted_graph_trail_research_ids=compacted_research_ids,
            recovered_count=recovered_count,
            deleted_count=deleted_count,
            compacted_count=compacted_count,
            total_count=recovered_count + deleted_count + compacted_count,
        )

    def get_latest_search_task_job(self, task_id: str) -> SearchTaskJob | None:
        return self.task_store.get_latest_search_task_job(task_id)

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
        return metrics.model_copy(
            update={
                "graph_alerts": graph_alerts,
                "graph_alert_trend": self._build_graph_alert_trend(self._filter_graph_step_events()),
                "maintenance_summary": maintenance_summary,
                "operational_health": self._build_operational_health(metrics, graph_alerts, maintenance_summary),
            }
        )

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
            raise HTTPException(status_code=404, detail="Maintenance heartbeat not found")

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
            raise HTTPException(status_code=404, detail="Operational recommendation not found")

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
        graph_metrics = GraphMetrics.model_validate(get_graph_metrics_snapshot())
        step_events = self._filter_graph_step_events()
        queue_metrics = self.get_queue_metrics()
        return {
            "status": "ok",
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

    def _build_graph_alert_trend(self, step_events: list[dict]) -> GraphAlertTrend:
        step_windows: dict[str, list[float]] = {}
        repeated_alerts: dict[str, int] = {}
        research_counts: dict[str, int] = {}
        worker_counts: dict[str, int] = {}
        recent_alerts: list[GraphAlertHistoryEntry] = []

        for event in step_events:
            step_name = str(event.get("step") or "").strip()
            if not step_name:
                continue
            elapsed_ms = float(event.get("elapsed_ms") or 0.0)
            failed = bool(event.get("failed"))
            step_windows.setdefault(step_name, []).append(elapsed_ms)

            alert_code = None
            severity = "warning"
            threshold = 0.0
            if failed:
                alert_code = "step_failures"
                threshold = float(self.GRAPH_STEP_FAILURE_WARNING_COUNT)
            elif elapsed_ms >= self.GRAPH_STEP_WARNING_MS:
                alert_code = "high_avg_ms"
                severity = "critical" if elapsed_ms >= self.GRAPH_STEP_CRITICAL_MS else "warning"
                threshold = self.GRAPH_STEP_WARNING_MS if severity == "warning" else self.GRAPH_STEP_CRITICAL_MS

            if not alert_code:
                continue

            repeated_alerts[alert_code] = repeated_alerts.get(alert_code, 0) + 1
            if event.get("research_id"):
                research_id_value = str(event["research_id"])
                research_counts[research_id_value] = research_counts.get(research_id_value, 0) + 1
            if event.get("worker_name"):
                worker_name_value = str(event["worker_name"])
                worker_counts[worker_name_value] = worker_counts.get(worker_name_value, 0) + 1
            recent_alerts.append(
                GraphAlertHistoryEntry(
                    timestamp=datetime.fromisoformat(event["timestamp"]),
                    code=alert_code,
                    severity=severity,
                    step=step_name,
                    current_value=elapsed_ms if alert_code == "high_avg_ms" else 1.0,
                    threshold=threshold,
                    research_id=event.get("research_id"),
                    worker_name=event.get("worker_name"),
                )
            )

        worsening_steps: list[str] = []
        improving_steps: list[str] = []
        for step_name, values in step_windows.items():
            if len(values) < 4:
                continue
            window_size = min(5, len(values) // 2)
            if window_size <= 0:
                continue
            previous = values[-(window_size * 2):-window_size]
            recent = values[-window_size:]
            if not previous or not recent:
                continue
            previous_avg = sum(previous) / len(previous)
            recent_avg = sum(recent) / len(recent)
            if recent_avg > previous_avg * 1.25 and recent_avg - previous_avg >= 100:
                worsening_steps.append(step_name)
            elif previous_avg > 0 and recent_avg < previous_avg * 0.8 and previous_avg - recent_avg >= 100:
                improving_steps.append(step_name)

        top_research_ids = [
            research_id_value
            for research_id_value, _ in sorted(research_counts.items(), key=lambda item: item[1], reverse=True)[:3]
        ]
        top_worker_names = [
            worker_name_value
            for worker_name_value, _ in sorted(worker_counts.items(), key=lambda item: item[1], reverse=True)[:3]
        ]
        return GraphAlertTrend(
            worsening_steps=sorted(set(worsening_steps)),
            improving_steps=sorted(set(improving_steps)),
            repeated_alerts=dict(sorted(repeated_alerts.items(), key=lambda item: item[1], reverse=True)),
            top_research_ids=top_research_ids,
            top_worker_names=top_worker_names,
            recent_alerts=recent_alerts[-10:],
        )

    def _build_maintenance_summary(self, summary: MaintenanceSummary) -> MaintenanceSummary:
        recent_runs = list(summary.recent_runs or [])
        recommendation_events = list(summary.recent_operational_recommendation_events or [])
        recommendations = list(summary.recent_operational_recommendations or [])
        total_counts = [int(item.total_count or 0) for item in recent_runs[-8:]]
        compacted_counts = [int(item.compacted_count or 0) for item in recent_runs[-8:]]
        average_compacted = round(sum(compacted_counts) / len(compacted_counts), 2) if compacted_counts else 0.0
        direction = "stable"
        if len(total_counts) >= 4:
            half = len(total_counts) // 2
            previous = total_counts[:half]
            recent = total_counts[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg > previous_avg * 1.2 and recent_avg - previous_avg >= 1:
                direction = "growing"
            elif previous_avg > 0 and recent_avg < previous_avg * 0.8 and previous_avg - recent_avg >= 1:
                direction = "shrinking"
        trend = MaintenanceSummary.MaintenanceTrend(
            cleanup_volume_direction=direction,
            average_compacted_count=average_compacted,
            recent_total_counts=total_counts,
            recent_compacted_counts=compacted_counts,
        )
        alerts: list[MaintenanceSummary.MaintenanceAlert] = []
        recent_avg_total = round(sum(total_counts[-4:]) / len(total_counts[-4:]), 2) if total_counts[-4:] else 0.0
        if direction == "growing" and recent_avg_total >= self.MAINTENANCE_GROWING_CRITICAL_RECENT_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="cleanup_volume_growing",
                    severity="critical",
                    current_value=recent_avg_total,
                    threshold=self.MAINTENANCE_GROWING_CRITICAL_RECENT_AVG,
                    hint="Maintenance cleanup volume is rising; inspect backlog growth, retry churn, and graph operational noise.",
                )
            )
        elif direction == "growing" and recent_avg_total >= self.MAINTENANCE_GROWING_WARNING_RECENT_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="cleanup_volume_growing",
                    severity="warning",
                    current_value=recent_avg_total,
                    threshold=self.MAINTENANCE_GROWING_WARNING_RECENT_AVG,
                    hint="Cleanup volume is trending upward; check whether queue churn or graph retries are increasing.",
                )
            )

        if average_compacted >= self.MAINTENANCE_COMPACTED_CRITICAL_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="high_compacted_average",
                    severity="critical",
                    current_value=average_compacted,
                    threshold=self.MAINTENANCE_COMPACTED_CRITICAL_AVG,
                    hint="Persisted graph operational data is being compacted heavily; review event/trail volume and retention settings.",
                )
            )
        elif average_compacted >= self.MAINTENANCE_COMPACTED_WARNING_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="high_compacted_average",
                    severity="warning",
                    current_value=average_compacted,
                    threshold=self.MAINTENANCE_COMPACTED_WARNING_AVG,
                    hint="Compacted graph data per run is elevated; check whether operational history is growing too quickly.",
                )
            )

        if summary.last_run_at is not None:
            age_seconds = max((datetime.now(timezone.utc) - summary.last_run_at).total_seconds(), 0.0)
            if age_seconds >= self.MAINTENANCE_STALE_CRITICAL_SECONDS:
                alerts.append(
                    MaintenanceSummary.MaintenanceAlert(
                        code="maintenance_stale",
                        severity="critical",
                        current_value=round(age_seconds, 2),
                        threshold=float(self.MAINTENANCE_STALE_CRITICAL_SECONDS),
                        hint="Maintenance has not run recently; verify the maintenance worker heartbeat and queue maintenance path.",
                    )
                )
            elif age_seconds >= self.MAINTENANCE_STALE_WARNING_SECONDS:
                alerts.append(
                    MaintenanceSummary.MaintenanceAlert(
                        code="maintenance_stale",
                        severity="warning",
                        current_value=round(age_seconds, 2),
                        threshold=float(self.MAINTENANCE_STALE_WARNING_SECONDS),
                        hint="Maintenance cadence is getting stale; verify periodic cleanup is still running.",
                    )
                )

        recommendation_events_by_code: dict[str, list[MaintenanceSummary.RecommendationEvent]] = {}
        for event in recommendation_events:
            recommendation_events_by_code.setdefault(event.code, []).append(event)

        ack_durations_hours: list[float] = []
        resolve_durations_hours: list[float] = []
        reappeared_count = 0
        top_recurring_codes = sorted(
            (
                (item.code, max(int(item.shown_count or 1) - 1, 0))
                for item in recommendations
            ),
            key=lambda item: (-item[1], item[0]),
        )

        for code, events in recommendation_events_by_code.items():
            shown_timestamp: datetime | None = None
            for event in events:
                if event.event_type in {"shown", "reappeared"}:
                    if event.event_type == "reappeared":
                        reappeared_count += 1
                    shown_timestamp = event.timestamp
                elif event.event_type == "acknowledged" and shown_timestamp and event.timestamp:
                    ack_durations_hours.append(max((event.timestamp - shown_timestamp).total_seconds(), 0.0) / 3600.0)
                elif event.event_type == "resolved" and shown_timestamp and event.timestamp:
                    resolve_durations_hours.append(max((event.timestamp - shown_timestamp).total_seconds(), 0.0) / 3600.0)
                    shown_timestamp = None

        unresolved_items = [item for item in recommendations if not item.resolved]
        now_utc = datetime.now(timezone.utc)
        unresolved_ages_hours = [
            max((now_utc - (item.first_shown_at or item.last_shown_at)).total_seconds(), 0.0) / 3600.0
            for item in unresolved_items
            if item.first_shown_at or item.last_shown_at
        ]
        average_time_to_ack_hours = round(sum(ack_durations_hours) / len(ack_durations_hours), 2) if ack_durations_hours else 0.0
        average_time_to_resolve_hours = round(sum(resolve_durations_hours) / len(resolve_durations_hours), 2) if resolve_durations_hours else 0.0
        oldest_unresolved_hours = round(max(unresolved_ages_hours), 2) if unresolved_ages_hours else 0.0
        recommendation_analytics = MaintenanceSummary.RecommendationAnalytics(
            average_time_to_ack_hours=average_time_to_ack_hours,
            average_time_to_resolve_hours=average_time_to_resolve_hours,
            oldest_unresolved_hours=oldest_unresolved_hours,
            unresolved_count=len(unresolved_items),
            repeated_reappeared_count=reappeared_count,
            top_recurring_codes=[code for code, count in top_recurring_codes[:3] if count > 0],
        )

        if recommendation_analytics.unresolved_count >= self.RUNBOOK_UNRESOLVED_CRITICAL_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_unresolved_pressure",
                    severity="critical",
                    current_value=float(recommendation_analytics.unresolved_count),
                    threshold=float(self.RUNBOOK_UNRESOLVED_CRITICAL_COUNT),
                    hint="Too many unresolved runbook items remain active; clear the operator queue before retries and backlog compound.",
                )
            )
        elif recommendation_analytics.unresolved_count >= self.RUNBOOK_UNRESOLVED_WARNING_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_unresolved_pressure",
                    severity="warning",
                    current_value=float(recommendation_analytics.unresolved_count),
                    threshold=float(self.RUNBOOK_UNRESOLVED_WARNING_COUNT),
                    hint="Runbook unresolved items are accumulating; review whether recommendations are being acknowledged but not completed.",
                )
            )

        if recommendation_analytics.average_time_to_resolve_hours >= self.RUNBOOK_RESOLUTION_CRITICAL_HOURS:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_slow_resolution",
                    severity="critical",
                    current_value=recommendation_analytics.average_time_to_resolve_hours,
                    threshold=self.RUNBOOK_RESOLUTION_CRITICAL_HOURS,
                    hint="Recommendations are taking too long to close; prioritize recurring issues and reduce reappearing operational debt.",
                )
            )
        elif recommendation_analytics.average_time_to_resolve_hours >= self.RUNBOOK_RESOLUTION_WARNING_HOURS:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_slow_resolution",
                    severity="warning",
                    current_value=recommendation_analytics.average_time_to_resolve_hours,
                    threshold=self.RUNBOOK_RESOLUTION_WARNING_HOURS,
                    hint="Resolution time is drifting upward; confirm operators are not just acknowledging items without completing the fix.",
                )
            )

        if recommendation_analytics.repeated_reappeared_count >= self.RUNBOOK_REAPPEARED_CRITICAL_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_reappeared_items",
                    severity="critical",
                    current_value=float(recommendation_analytics.repeated_reappeared_count),
                    threshold=float(self.RUNBOOK_REAPPEARED_CRITICAL_COUNT),
                    hint="Runbook items are repeatedly reappearing; fixes are likely not addressing the root cause.",
                )
            )
        elif recommendation_analytics.repeated_reappeared_count >= self.RUNBOOK_REAPPEARED_WARNING_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_reappeared_items",
                    severity="warning",
                    current_value=float(recommendation_analytics.repeated_reappeared_count),
                    threshold=float(self.RUNBOOK_REAPPEARED_WARNING_COUNT),
                    hint="Some recommendations keep returning; audit the last fixes and operator notes for incomplete remediation.",
                )
            )

        return summary.model_copy(update={"trend": trend, "alerts": alerts, "recommendation_analytics": recommendation_analytics})

    def _build_operational_health(
        self,
        metrics: QueueMetrics,
        graph_alerts: list[GraphAlert],
        maintenance_summary: MaintenanceSummary,
    ) -> OperationalHealth:
        score = 100
        reasons: list[str] = []

        for alert in graph_alerts:
            if alert.severity == "critical":
                score -= 25
            else:
                score -= 10
            reasons.append(f"graph:{alert.code}")

        for alert in maintenance_summary.alerts:
            if alert.severity == "critical":
                score -= 20
            else:
                score -= 8
            reasons.append(f"maintenance:{alert.code}")

        backlog = (
            metrics.pending_search_jobs
            + metrics.running_search_jobs
            + metrics.dead_letter_search_jobs
            + metrics.pending_finalize_jobs
            + metrics.running_finalize_jobs
            + metrics.dead_letter_finalize_jobs
        )
        if backlog >= 20:
            score -= 20
            reasons.append("queue:high_backlog")
        elif backlog >= 8:
            score -= 10
            reasons.append("queue:elevated_backlog")

        score = max(score, 0)
        status = "healthy"
        if any(
            (alert.severity == "critical" for alert in graph_alerts)
        ) or any((alert.severity == "critical" for alert in maintenance_summary.alerts)) or score <= 50:
            status = "critical"
        elif graph_alerts or maintenance_summary.alerts or score < 90:
            status = "warning"

        deduped_reasons = list(dict.fromkeys(reasons))
        current_health = OperationalHealth(status=status, score=score, reasons=deduped_reasons[:8])
        history = list(maintenance_summary.recent_operational_health or [])
        current_timestamp = maintenance_summary.last_run_at
        history.append(
            OperationalHealth.OperationalHealthEntry(
                status=status,
                score=score,
                reasons=deduped_reasons[:8],
                timestamp=current_timestamp,
            )
        )
        history = history[-20:]
        score_values = [int(item.score or 100) for item in history[-8:]]
        statuses = [str(item.status or "healthy") for item in history[-8:]]
        average_score = round(sum(score_values) / len(score_values), 2) if score_values else 100.0
        score_direction = "stable"
        if len(score_values) >= 4:
            half = len(score_values) // 2
            previous = score_values[:half]
            recent = score_values[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg < previous_avg - 8:
                score_direction = "worsening"
            elif recent_avg > previous_avg + 8:
                score_direction = "improving"
        trend = OperationalHealth.OperationalHealthTrend(
            score_direction=score_direction,
            average_score=average_score,
            recent_scores=score_values,
            recent_statuses=statuses,
        )
        alerts: list[OperationalHealth.OperationalHealthAlert] = []
        if len(score_values) >= 4:
            half = len(score_values) // 2
            previous = score_values[:half]
            recent = score_values[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg <= previous_avg - self.OPERATIONAL_WORSENING_CRITICAL_DELTA:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_worsening",
                        severity="critical",
                        current_value=round(recent_avg, 2),
                        threshold=round(previous_avg, 2),
                        hint="Operational score is falling quickly; inspect graph and maintenance alerts before backlog compounds.",
                    )
                )
            elif recent_avg <= previous_avg - self.OPERATIONAL_WORSENING_WARNING_DELTA:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_worsening",
                        severity="warning",
                        current_value=round(recent_avg, 2),
                        threshold=round(previous_avg, 2),
                        hint="Operational score is trending downward; check recent alert growth and cleanup pressure.",
                    )
                )

        recent_critical_count = sum(1 for item in history[-5:] if str(item.status or "").lower() == "critical")
        if recent_critical_count >= self.OPERATIONAL_CRITICAL_STATE_CRITICAL_COUNT:
            alerts.append(
                OperationalHealth.OperationalHealthAlert(
                    code="repeated_critical_states",
                    severity="critical",
                    current_value=float(recent_critical_count),
                    threshold=float(self.OPERATIONAL_CRITICAL_STATE_CRITICAL_COUNT),
                    hint="Too many recent critical states; investigate persistent graph failures, backlog pressure, or stale maintenance.",
                )
            )
        elif recent_critical_count >= self.OPERATIONAL_CRITICAL_STATE_WARNING_COUNT:
            alerts.append(
                OperationalHealth.OperationalHealthAlert(
                    code="repeated_critical_states",
                    severity="warning",
                    current_value=float(recent_critical_count),
                    threshold=float(self.OPERATIONAL_CRITICAL_STATE_WARNING_COUNT),
                    hint="Critical states are recurring; confirm the system is actually recovering between maintenance cycles.",
                )
            )

        if len(history) >= 2:
            previous_status = str(history[-2].status or "healthy").lower()
            if previous_status in {"critical", "warning"} and status == "healthy" and score >= 90:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_recovered",
                        severity="warning",
                        current_value=float(score),
                        threshold=90.0,
                        hint="Operational health has recovered; verify the underlying cause was actually resolved and not just transient.",
                    )
                )

        recommendations = self._build_operational_recommendations(
            metrics=metrics,
            graph_alerts=graph_alerts,
            maintenance_summary=maintenance_summary,
            operational_alerts=alerts,
            reasons=deduped_reasons[:8],
        )
        return current_health.model_copy(
            update={
                "alerts": alerts,
                "recommendations": recommendations,
                "history": history,
                "trend": trend,
            }
        )

    def _build_operational_recommendations(
        self,
        metrics: QueueMetrics,
        graph_alerts: list[GraphAlert],
        maintenance_summary: MaintenanceSummary,
        operational_alerts: list[OperationalHealth.OperationalHealthAlert],
        reasons: list[str],
    ) -> list[OperationalHealth.RecommendationEntry]:
        recommendation_specs: list[tuple[str, str]] = []
        alert_codes = {alert.code for alert in operational_alerts}
        reason_set = set(reasons)
        current_timestamp = maintenance_summary.last_run_at or datetime.now(timezone.utc)

        if "repeated_critical_states" in alert_codes:
            recommendation_specs.append(
                (
                    "increase_worker_parallelism",
                    "Repeated critical states detected: consider increasing worker parallelism and checking whether one worker is saturating the queue.",
                )
            )
        if "score_worsening" in alert_codes:
            recommendation_specs.append(
                (
                    "inspect_backlog_latency_and_retries",
                    "Operational score is worsening: inspect queue backlog, extraction latency, and graph retries before the next maintenance cycle.",
                )
            )
        if "score_recovered" in alert_codes:
            recommendation_specs.append(
                (
                    "verify_recovered_score_root_cause",
                    "Score recovered after degradation: verify the underlying issue is resolved and not just temporarily masked.",
                )
            )

        maintenance_alert_codes = {alert.code for alert in maintenance_summary.alerts}
        if "maintenance_stale" in maintenance_alert_codes:
            recommendation_specs.append(
                (
                    "restart_or_verify_maintenance_path",
                    "Maintenance appears stale: verify the maintenance worker is running and trigger the maintenance path if needed.",
                )
            )
        if "high_compacted_average" in maintenance_alert_codes:
            recommendation_specs.append(
                (
                    "review_graph_retention_pressure",
                    "High graph compaction volume: review graph event/trail retention and whether operational data is growing too quickly.",
                )
            )

        graph_alert_codes = {alert.code for alert in graph_alerts}
        if "analyze_retries" in graph_alert_codes:
            recommendation_specs.append(
                (
                    "tighten_source_selection_and_claim_verification",
                    "Frequent analyze retries: tighten source selection or claim verification to reduce repeated finalize passes.",
                )
            )
        if "step_failures" in graph_alert_codes:
            recommendation_specs.append(
                (
                    "inspect_graph_failures_and_search_quality",
                    "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                )
            )

        if "queue:high_backlog" in reason_set or "queue:elevated_backlog" in reason_set:
            recommendation_specs.append(
                (
                    "reduce_queue_backlog",
                    "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                )
            )

        if metrics.extraction_metrics.avg_total_ms >= 3000:
            recommendation_specs.append(
                (
                    "reduce_extraction_latency",
                    "Extraction latency is elevated: review slow domains, timeout settings, and extraction concurrency.",
                )
            )

        previous_entries = {
            item.code: item
            for item in (maintenance_summary.recent_operational_recommendations or [])
        }
        recommendation_events = list(maintenance_summary.recent_operational_recommendation_events or [])
        active_codes: list[str] = []
        merged_entries: list[OperationalHealth.RecommendationEntry] = []
        seen_codes: set[str] = set()

        for code, message in recommendation_specs:
            if code in seen_codes:
                continue
            seen_codes.add(code)
            active_codes.append(code)
            previous = previous_entries.get(code)
            event_type = None
            if previous is None:
                event_type = "shown"
            elif not previous.active or previous.resolved:
                event_type = "reappeared"
            if event_type is not None:
                recommendation_events = self._append_operational_recommendation_event(
                    recommendation_events,
                    code=code,
                    event_type=event_type,
                    message=message,
                    timestamp=current_timestamp,
                )
            merged_entries.append(
                OperationalHealth.RecommendationEntry(
                    code=code,
                    message=message,
                    shown_count=(previous.shown_count + 1) if previous else 1,
                    active=True,
                    first_shown_at=(previous.first_shown_at if previous else current_timestamp),
                    last_shown_at=current_timestamp,
                    acknowledged=(previous.acknowledged if previous and not previous.resolved else False),
                    acknowledged_at=(previous.acknowledged_at if previous and not previous.resolved else None),
                    resolved=False,
                    resolved_at=None,
                    resolution_note=None,
                )
            )

        for code, previous in previous_entries.items():
            if code in active_codes:
                continue
            merged_entries.append(
                previous.model_copy(
                    update={
                        "active": False,
                    }
                )
            )

        merged_entries.sort(
            key=lambda item: (
                0 if item.active else 1,
                0 if not item.resolved else 1,
                0 if not item.acknowledged else 1,
                -(item.shown_count or 0),
                item.code,
            )
        )
        maintenance_summary.recent_operational_recommendation_events = recommendation_events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
        return merged_entries[:8]

    def _append_operational_recommendation_event(
        self,
        existing_events: list[dict] | list[MaintenanceSummary.RecommendationEvent],
        *,
        code: str,
        event_type: str,
        message: str,
        timestamp: str | datetime,
        note: str | None = None,
    ) -> list[MaintenanceSummary.RecommendationEvent]:
        normalized_timestamp = (
            timestamp.isoformat()
            if isinstance(timestamp, datetime)
            else str(timestamp)
        )
        events = [
            item.model_dump(mode="json") if isinstance(item, MaintenanceSummary.RecommendationEvent) else dict(item)
            for item in existing_events
        ]
        if events:
            latest = events[-1]
            if (
                str(latest.get("code") or "") == code
                and str(latest.get("event_type") or "") == event_type
                and str(latest.get("message") or "") == message
            ):
                return [
                    MaintenanceSummary.RecommendationEvent.model_validate(item)
                    for item in events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
                ]
        events.append(
            {
                "code": code,
                "event_type": event_type,
                "message": message,
                "timestamp": normalized_timestamp,
                "note": note,
            }
        )
        return [
            MaintenanceSummary.RecommendationEvent.model_validate(item)
            for item in events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
        ]

    def _graph_alert_hint(self, code: str, step: str | None) -> str:
        step_name = (step or "").strip().lower()
        if code == "high_avg_ms":
            if step_name == "analyze":
                return "Check LLM latency and consider reducing analyzer payload budget for large reports."
            if step_name in {"replan", "tie_break"}:
                return "Inspect follow-up query volume and search pass depth; trim weak branches before re-running."
            if step_name == "collect_context":
                return "Review source pool size and pre-filtering; excessive source aggregation is slowing graph preparation."
            if step_name == "verify":
                return "Check claim-verification heuristics and conflict volume; verification may be over-processing weak evidence."
            return "Review the slow graph step and reduce unnecessary work before the next finalize pass."
        if code == "step_failures":
            if step_name == "tie_break":
                return "Check search quality, blocked domains, and domain filters for tie-break follow-up queries."
            if step_name == "analyze":
                return "Inspect analyzer prompt size, model stability, and citation-repair loops."
            if step_name == "replan":
                return "Review replan recommendations and whether follow-up queries are too broad or malformed."
            if step_name == "collect_context":
                return "Inspect source critic/evidence mapping inputs; malformed source data may be breaking context collection."
            if step_name == "verify":
                return "Review claim-verifier assumptions and conflict payload quality before verification."
            return "Inspect logs for the failing graph step and tighten the corresponding inputs."
        if code == "analyze_retries":
            return "Strengthen source selection or claim verification so finalize produces a cleaner draft in fewer analyze passes."
        return "Inspect the related graph step and recent worker logs for the underlying cause."

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
                    logger.info("search_job_retry_scheduled")
                if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                    logger.error("search_job_dead_letter")
                return failed_job
