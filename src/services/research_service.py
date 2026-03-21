import logging
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.search import SearchAgent
from src.api.schemas import (
    JobCleanupResponse,
    DecomposeResponse,
    FinalizeJobStatus,
    JobRecoveryResponse,
    QueueMetrics,
    QueueMaintenanceResponse,
    ResearchRecord,
    ResearchRequest,
    ResearchResponse,
    ResearchStatus,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    SearchDepth,
    SearchTask,
    TaskUpdate,
    TaskStatus,
    WorkerHeartbeat,
)
from src.config import settings
from src.repositories.protocols import TaskStore
from src.search_depth_profiles import get_depth_profile

logger = logging.getLogger(__name__)


class ResearchService:
    def __init__(
        self,
        task_store: TaskStore,
        optimizer: PromptOptimizerAgent | None = None,
        orchestrator: OrchestratorAgent | None = None,
        analyzer: AnalyzerAgent | None = None,
    ):
        self.task_store = task_store
        self.optimizer = optimizer
        self.orchestrator = orchestrator
        self.analyzer = analyzer

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
            "research_started research_id=%s task_count=%s depth=%s",
            research.id,
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
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        tasks = self.task_store.get_tasks_by_research(research_id)
        analyzer = self.require_agent(self.analyzer, "Analyzer")
        report = analyzer.run_analysis(research.prompt, tasks)
        self.task_store.update_research_status(
            research_id,
            ResearchStatus.COMPLETED,
            report,
        )
        logger.info("research_finalize_completed research_id=%s", research_id)

        return self.task_store.get_research(research_id)

    def enqueue_research_finalization(self, research_id: str) -> tuple[ResearchRecord, ResearchFinalizeJob | None]:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research, None

        self.require_agent(self.analyzer, "Analyzer")
        self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
        job = self.task_store.add_research_finalize_job(research_id, settings.job_max_attempts)
        logger.info("research_finalize_enqueued research_id=%s finalize_job_id=%s", research_id, job.id)
        return self.task_store.get_research(research_id), job

    def process_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.task_store.get_research_finalize_job(job_id)
        if job is None:
            return None

        try:
            logger.info("finalize_job_processing job_id=%s research_id=%s", job.id, job.research_id)
            self.complete_research_finalization(job.research_id)
            completed_job = self.task_store.update_research_finalize_job(
                job_id,
                FinalizeJobStatus.COMPLETED,
            )
            logger.info("finalize_job_completed job_id=%s research_id=%s", job.id, job.research_id)
            return completed_job
        except Exception as exc:
            failed_job = self.task_store.record_research_finalize_job_failure(job_id, str(exc))
            logger.warning(
                "finalize_job_failed job_id=%s research_id=%s error=%s next_status=%s",
                job.id,
                job.research_id,
                str(exc),
                failed_job.status.value if failed_job else "missing",
            )
            if failed_job and failed_job.status == FinalizeJobStatus.DEAD_LETTER:
                self.task_store.update_research_status(
                    job.research_id,
                    ResearchStatus.FAILED,
                    f"Analysis failed: {str(exc)}",
                )
                logger.error("finalize_job_dead_letter job_id=%s research_id=%s", job.id, job.research_id)
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

    def run_queue_maintenance(self) -> QueueMaintenanceResponse:
        search_recovery = self.recover_stale_search_task_jobs()
        finalize_recovery = self.recover_stale_research_finalize_jobs()
        search_cleanup = self.cleanup_old_search_task_jobs()
        finalize_cleanup = self.cleanup_old_research_finalize_jobs()

        recovered_count = search_recovery.recovered_count + finalize_recovery.recovered_count
        deleted_count = search_cleanup.deleted_count + finalize_cleanup.deleted_count

        return QueueMaintenanceResponse(
            recovered_search_job_ids=search_recovery.recovered_job_ids,
            recovered_finalize_job_ids=finalize_recovery.recovered_job_ids,
            deleted_search_job_ids=search_cleanup.deleted_job_ids,
            deleted_finalize_job_ids=finalize_cleanup.deleted_job_ids,
            recovered_count=recovered_count,
            deleted_count=deleted_count,
            total_count=recovered_count + deleted_count,
        )

    def get_latest_search_task_job(self, task_id: str) -> SearchTaskJob | None:
        return self.task_store.get_latest_search_task_job(task_id)

    def get_worker_heartbeat(self, worker_name: str) -> WorkerHeartbeat | None:
        return self.task_store.get_worker_heartbeat(worker_name)

    def touch_worker_heartbeat(
        self,
        worker_name: str,
        processed_jobs: int,
        status: str,
        last_error: str | None = None,
    ) -> WorkerHeartbeat:
        return self.task_store.upsert_worker_heartbeat(
            worker_name,
            processed_jobs,
            status,
            last_error,
        )

    def get_queue_metrics(self) -> QueueMetrics:
        return self.task_store.get_queue_metrics()

    def finalize_research(self, research_id: str) -> ResearchRecord:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research

        self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
        return self.complete_research_finalization(research_id)

    def run_search_task(self, task_id: str, depth: SearchDepth):
        profile = get_depth_profile(depth)
        agent = SearchAgent(
            task_store=self.task_store,
            max_sources=profile["source_limit"],
            search_results_per_query=profile["search_results_per_query"],
            max_candidate_urls=profile["max_candidate_urls"],
            extraction_concurrency=settings.search_extraction_concurrency,
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

        try:
            logger.info("search_job_processing job_id=%s task_id=%s depth=%s", job.id, task.id, job.depth.value)
            self.run_search_task(task.id, job.depth)
            task = self.task_store.get_task(task.id)
            if task is not None and task.status == TaskStatus.FAILED:
                failed_job = self.task_store.record_search_task_job_failure(
                    job_id,
                    task.logs[-1] if task.logs else "Search task failed",
                )
                logger.warning(
                    "search_job_failed job_id=%s task_id=%s next_status=%s",
                    job.id,
                    task.id,
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
                    logger.info("search_job_retry_scheduled job_id=%s task_id=%s", job.id, task.id)
                if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                    logger.error("search_job_dead_letter job_id=%s task_id=%s", job.id, task.id)
                return failed_job

            completed_job = self.task_store.update_search_task_job(job_id, SearchJobStatus.COMPLETED)
            logger.info("search_job_completed job_id=%s task_id=%s", job.id, task.id)
            return completed_job
        except Exception as exc:
            failed_job = self.task_store.record_search_task_job_failure(job_id, str(exc))
            logger.warning(
                "search_job_exception job_id=%s task_id=%s error=%s next_status=%s",
                job.id,
                task.id,
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
                logger.info("search_job_retry_scheduled job_id=%s task_id=%s", job.id, task.id)
            if failed_job and failed_job.status == SearchJobStatus.DEAD_LETTER:
                logger.error("search_job_dead_letter job_id=%s task_id=%s", job.id, task.id)
            return failed_job
