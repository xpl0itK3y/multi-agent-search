"""Job & queue lifecycle concern of ResearchService (research-finalize + search-task job
accessors: get/list/requeue/recover/cleanup, run_queue_maintenance and the cache/graph
compaction it drives), extracted as a mixin (AUD-030). Composed into ResearchService; relies
on self.task_store and self.broker (set in ResearchService.__init__).
"""
import logging
from datetime import datetime, timedelta, timezone

from src.domain.errors import ConflictError, NotFoundError

from src.agents.trail_text import trail_detail
from src.config import settings
from src.domain import (
    FinalizeJobStatus,
    JobCleanupResponse,
    JobRecoveryResponse,
    QueueMaintenanceResponse,
    ResearchFinalizeJob,
    ResearchStatus,
    SearchJobStatus,
    SearchTaskJob,
    TaskStatus,
    TaskUpdate,
)

logger = logging.getLogger(__name__)


class JobQueueMixin:
    @staticmethod
    def _stale_before(job_timeout_seconds: int, stale_seconds: int | None) -> datetime:
        """Cutoff for stale RUNNING jobs; a caller may widen it but never go below the job
        timeout, since recovery re-dispatches (and for finalize, fences) a live job."""
        seconds = max(stale_seconds or 0, job_timeout_seconds)
        return datetime.now(timezone.utc) - timedelta(seconds=seconds)

    def get_research_finalize_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        return self.task_store.get_research_finalize_job(job_id, user_id=user_id)

    def get_latest_research_finalize_job(
        self,
        research_id: str,
        user_id: str | None = None,
    ) -> ResearchFinalizeJob | None:
        return self.task_store.get_latest_research_finalize_job(research_id, user_id=user_id)

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
        if requeued is None:  # deleted, or requeued by a concurrent caller
            raise ConflictError("Only dead-letter finalize jobs can be requeued")
        if self.broker:
            self.broker.push_finalize_job(requeued.id)
        logger.info("finalize_job_requeued job_id=%s research_id=%s", job.id, job.research_id)
        return requeued

    def recover_stale_research_finalize_jobs(self, stale_seconds: int | None = None) -> JobRecoveryResponse:
        stale_before = self._stale_before(settings.finalize_job_timeout_seconds, stale_seconds)
        recovered_jobs = []
        for job in self.task_store.recover_stale_research_finalize_jobs(stale_before):
            if job.status != FinalizeJobStatus.PENDING:
                # The store closed it: the research was cancelled, completed or failed while
                # the job hung, and a new runner would have finished it anyway.
                logger.warning("finalize_job_closed_research_ended job_id=%s research_id=%s", job.id, job.research_id)
                continue
            recovered_jobs.append(job)
            # Never overwrite a status the runner does not own: this moves only a research
            # that is still in flight (normally already ANALYZING) and leaves a cancel that
            # landed meanwhile alone; the next runner's start-of-run check then closes the job.
            self.task_store.try_begin_finalization(job.research_id)
            research = self.task_store.get_research(job.research_id)
            if research is not None and research.status == ResearchStatus.ANALYZING:
                graph_state = research.graph_state or {}
                resume_step = graph_state.get("step") or "unknown"
                # Patch only the flag: merging the snapshot read above back would rewrite
                # every key with its pre-read value, racing the fenced runner that may still write.
                self.checkpoint_graph_state(
                    job.research_id,
                    {"resume_after_stale_recovery": True},
                    {
                        "step": "stale_recovered",
                        "detail": trail_detail(
                            "stale_recovered",
                            self._research_language(research),
                            job_id=job.id,
                            step=resume_step,
                        ),
                        "metrics": {"resume_from": resume_step},
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

    def cleanup_old_research_finalize_jobs(self, older_than: datetime | None = None) -> JobCleanupResponse:
        older_than = older_than or datetime.now(timezone.utc) - timedelta(
            seconds=settings.finalize_job_retention_seconds
        )
        deleted_ids = self.task_store.cleanup_old_research_finalize_jobs(older_than)
        if deleted_ids:
            logger.info("finalize_jobs_cleaned deleted_count=%s", len(deleted_ids))
        return JobCleanupResponse(
            deleted_job_ids=deleted_ids,
            deleted_count=len(deleted_ids),
        )

    def get_search_task_job(
        self,
        job_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        return self.task_store.get_search_task_job(job_id, user_id=user_id)

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
        if requeued is None:  # deleted, or requeued by a concurrent caller
            raise ConflictError("Only dead-letter search jobs can be requeued")
        if self.broker:
            self.broker.push_search_job(requeued.id)
        logger.info("search_job_requeued job_id=%s task_id=%s", job.id, task.id)
        return requeued

    def recover_stale_search_task_jobs(self, stale_seconds: int | None = None) -> JobRecoveryResponse:
        stale_before = self._stale_before(settings.search_job_timeout_seconds, stale_seconds)
        recovered_jobs = self.task_store.recover_stale_search_task_jobs(stale_before)
        for job in recovered_jobs:
            self.task_store.update_task(
                job.task_id,
                TaskUpdate(status=TaskStatus.PENDING, log="Recovered stale running search job"),
            )
            # Re-dispatch to Redis so a broker-mode worker takes it right away; without the push
            # it waits for a worker's lost-push DB claim, which runs only when a BLPOP is idle.
            if self.broker:
                self.broker.push_search_job(job.id)
            logger.warning("search_job_recovered job_id=%s task_id=%s", job.id, job.task_id)
        return JobRecoveryResponse(
            recovered_job_ids=[job.id for job in recovered_jobs],
            recovered_count=len(recovered_jobs),
        )

    def cleanup_old_search_task_jobs(self, older_than: datetime | None = None) -> JobCleanupResponse:
        older_than = older_than or datetime.now(timezone.utc) - timedelta(
            seconds=settings.search_job_retention_seconds
        )
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

    def cleanup_search_cache(self, older_than: datetime | None = None) -> int:
        older_than = older_than or datetime.now(timezone.utc) - timedelta(
            seconds=settings.search_cache_ttl_seconds
        )
        deleted = self.task_store.cleanup_search_cache(older_than)
        if deleted:
            logger.info("search_cache_cleaned deleted_count=%s", deleted)
        return deleted

    def cleanup_old_researches(self) -> list[str]:
        """Retention for finished researches (OPS-RETENTION). No-op by default:
        research_retention_seconds=0 keeps everything, matching existing deployments."""
        if settings.research_retention_seconds <= 0:
            return []
        older_than = datetime.now(timezone.utc) - timedelta(
            seconds=settings.research_retention_seconds
        )
        deleted_ids = self.task_store.cleanup_old_researches(older_than)
        if deleted_ids:
            logger.info("researches_cleaned deleted_count=%s", len(deleted_ids))
        return deleted_ids

    def cleanup_old_telemetry(self) -> dict[str, int]:
        """Retention for user_events, user_sessions and admin_audit_logs; a table whose
        *_retention_seconds is 0 is kept forever. Returns deleted counts per swept table."""
        now = datetime.now(timezone.utc)
        sweeps = (
            ("user_events", settings.user_events_retention_seconds, self.task_store.cleanup_old_user_events),
            ("user_sessions", settings.user_sessions_retention_seconds, self.task_store.cleanup_old_user_sessions),
            (
                "admin_audit_logs",
                settings.admin_audit_retention_seconds,
                self.task_store.cleanup_old_admin_audit_logs,
            ),
        )
        deleted: dict[str, int] = {}
        for table, retention_seconds, cleanup in sweeps:
            if retention_seconds <= 0:
                continue
            deleted[table] = cleanup(now - timedelta(seconds=retention_seconds))
            if deleted[table]:
                logger.info("telemetry_cleaned table=%s deleted_count=%s", table, deleted[table])
        return deleted

    def run_queue_maintenance(self) -> QueueMaintenanceResponse:
        self.recover_pending_decompositions()
        search_recovery = self.recover_stale_search_task_jobs()
        finalize_recovery = self.recover_stale_research_finalize_jobs()
        search_cleanup = self.cleanup_old_search_task_jobs()
        finalize_cleanup = self.cleanup_old_research_finalize_jobs()
        self.cleanup_search_cache()
        self.cleanup_old_researches()
        self.cleanup_old_telemetry()
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

    def get_latest_search_task_job(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        return self.task_store.get_latest_search_task_job(task_id, user_id=user_id)
