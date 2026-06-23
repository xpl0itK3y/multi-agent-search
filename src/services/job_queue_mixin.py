"""Job & queue lifecycle concern of ResearchService (research-finalize + search-task job
accessors: get/list/requeue/recover/cleanup, run_queue_maintenance and the cache/graph
compaction it drives), extracted as a mixin (AUD-030). Composed into ResearchService; relies
on self.task_store and self.broker (set in ResearchService.__init__).
"""
import logging
from datetime import datetime, timedelta, timezone

from src.domain.errors import ConflictError, NotFoundError

from src.config import settings
from src.domain import *  # noqa: F401,F403

logger = logging.getLogger(__name__)


class JobQueueMixin:
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
            raise NotFoundError("Finalize job not found")
        if job.status != FinalizeJobStatus.DEAD_LETTER:
            raise ConflictError("Only dead-letter finalize jobs can be requeued")

        self.require_agent(self.analyzer, "Analyzer")
        self.task_store.update_research_status(job.research_id, ResearchStatus.ANALYZING)
        requeued = self.task_store.requeue_research_finalize_job(job_id)
        if requeued is None:
            raise NotFoundError("Finalize job not found")
        if self.broker:
            self.broker.push_finalize_job(requeued.id)
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
            # Re-dispatch to Redis (see search-job recovery note above).
            if self.broker:
                self.broker.push_finalize_job(job.id)
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
            raise NotFoundError("Search job not found")
        if job.status != SearchJobStatus.DEAD_LETTER:
            raise ConflictError("Only dead-letter search jobs can be requeued")

        task = self.task_store.get_task(job.task_id)
        if task is None:
            raise NotFoundError("Task not found")

        self.task_store.update_task(
            task.id,
            TaskUpdate(status=TaskStatus.PENDING, log="Search job manually requeued"),
        )
        requeued = self.task_store.requeue_search_task_job(job_id)
        if requeued is None:
            raise NotFoundError("Search job not found")
        if self.broker:
            self.broker.push_search_job(requeued.id)
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
            # Re-dispatch to Redis: in broker mode workers consume only from Redis (no Postgres
            # poll fallback), so a reset-to-PENDING job would otherwise never be claimed.
            if self.broker:
                self.broker.push_search_job(job.id)
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

    def cleanup_search_cache(self) -> int:
        older_than = datetime.now(timezone.utc) - timedelta(seconds=settings.search_cache_ttl_seconds)
        deleted = self.task_store.cleanup_search_cache(older_than)
        if deleted:
            logger.info("search_cache_cleaned deleted_count=%s", deleted)
        return deleted

    def run_queue_maintenance(self) -> QueueMaintenanceResponse:
        self.recover_pending_decompositions()
        self.run_due_watches()
        search_recovery = self.recover_stale_search_task_jobs()
        finalize_recovery = self.recover_stale_research_finalize_jobs()
        search_cleanup = self.cleanup_old_search_task_jobs()
        finalize_cleanup = self.cleanup_old_research_finalize_jobs()
        self.cleanup_search_cache()
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

