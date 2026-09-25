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
        # A dead-letter job outlives the research's next attempt (a retry that went back to
        # the searches), so requeueing it could rewind a research that has since completed,
        # been cancelled or is searching again. Only the latest job of a FAILED one qualifies.
        research = self.task_store.get_research(job.research_id)
        if research is None or research.status != ResearchStatus.FAILED:
            raise ConflictError("Only the finalize job of a failed research can be requeued")
        latest = self.task_store.get_latest_research_finalize_job(job.research_id)
        if latest is None or latest.id != job.id:
            raise ConflictError("A newer finalize job has superseded this one")

        self.require_agent(self.analyzer, "Analyzer")
        # One guarded transaction: requeueing first would let a polling worker claim the job
        # while the research is still FAILED; setting ANALYZING first could strand it there.
        requeued = self.task_store.requeue_failed_research_finalize_job(job_id)
        if requeued is None:  # deleted, requeued or superseded by a concurrent caller
            raise ConflictError("Finalize job state changed. Please retry.")
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
        # A dead-letter job outlives its research's attempt. Requeued on a research that
        # completed, was cancelled, failed or is finalizing, the worker only drained it and
        # left the task PENDING on the ended research; a failed one is retried instead.
        if task.research_id is not None:
            research = self.task_store.get_research(task.research_id)
            if research is None or research.status != ResearchStatus.PROCESSING:
                raise ConflictError(
                    "Only a search job of a research that is still searching can be requeued; "
                    "retry a failed research instead"
                )
        latest = self.task_store.get_latest_search_task_job(task.id)
        if latest is None or latest.id != job.id:
            raise ConflictError("A newer search job has superseded this one")

        # One guarded transaction: the task reset, the job requeue and a touch of the
        # research row, so a stalled sweep that listed the research loses its CAS instead
        # of failing it under the requeued search.
        requeued = self.task_store.requeue_search_task_job_of_active_research(
            job_id, "Search job manually requeued"
        )
        if requeued is None:  # deleted, requeued or superseded meanwhile, or the research moved on
            raise ConflictError("Search job state changed. Please retry.")
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

    # Researches the stalled sweep handles per maintenance pass (it runs every
    # queue_maintenance_interval_seconds, so a backlog drains over a few passes).
    STALLED_RESEARCH_SWEEP_LIMIT = 50
    STALLED_RESEARCH_REPORT = (
        "Research stopped making progress: no search or analysis was left queued for it. "
        "Please try again."
    )

    def _stalled_before(self) -> datetime:
        """Well past the admission stale cutoff and both job timeouts: a research with a
        live job is never a candidate, and every live step writes the research row."""
        seconds = max(
            settings.research_stale_active_seconds,
            settings.search_job_timeout_seconds,
            settings.finalize_job_timeout_seconds,
        )
        return datetime.now(timezone.utc) - timedelta(seconds=seconds)

    def sweep_stalled_researches(self) -> list[str]:
        """Safety net for work lost between steps: a crash between a status change and the
        job it should come with, a replan task orphaned RUNNING by a dead finalize worker,
        a decomposition whose marker is gone. A PROCESSING/ANALYZING research with no
        pending/running job, no decomposition in flight and no write since the cutoff
        cannot move. One whose searches all settled only lost its finalize enqueue and is
        finalized; any other is FAILED, which makes it retryable. Returns the ids handled."""
        stale_before = self._stalled_before()
        handled: list[str] = []
        for research_id in self.task_store.list_stalled_research_ids(
            stale_before, limit=self.STALLED_RESEARCH_SWEEP_LIMIT
        ):
            research = self.task_store.get_research(research_id)
            if research is None:
                continue
            if research.status == ResearchStatus.PROCESSING and self._finalize_stalled_search(research_id):
                handled.append(research_id)
                continue
            # Guarded on the status and on no write since the cutoff: anything that started
            # the research moving again in the meantime wrote the row and wins.
            failed = self.task_store.transition_research_status(
                research_id,
                [ResearchStatus.PROCESSING, ResearchStatus.ANALYZING],
                ResearchStatus.FAILED,
                self.STALLED_RESEARCH_REPORT,
                updated_before=stale_before,
            )
            if failed is not None:
                handled.append(research_id)
                logger.warning(
                    "research_stalled_failed research_id=%s previous_status=%s", research_id, research.status.value
                )
        return handled

    def _finalize_stalled_search(self, research_id: str) -> bool:
        # Chat follow-up tasks neither gate nor feed the report (see _report_tasks), as in
        # _maybe_enqueue_finalization: one a dead API process left RUNNING must not fail it.
        tasks = self._report_tasks(self.task_store.get_tasks_by_research(research_id))
        if not tasks or not all(self._search_settled(task) for task in tasks):
            return False
        try:
            _, job = self.enqueue_research_finalization(research_id)
        except Exception as exc:  # e.g. no analyzer in this process: fail it, retry can finalize
            logger.warning("research_stalled_finalize_failed research_id=%s error=%s", research_id, exc)
            return False
        if job is None:
            # Lost the CAS, or every task had failed and the research is now FAILED.
            current = self.task_store.get_research(research_id)
            return current is not None and current.status != ResearchStatus.PROCESSING
        logger.warning("research_stalled_finalize_enqueued research_id=%s finalize_job_id=%s", research_id, job.id)
        return True

    def run_queue_maintenance(self) -> QueueMaintenanceResponse:
        """Every step runs even when an earlier one fails, so one broken sweep (say research
        retention on a huge backlog) cannot starve recovery or the other sweeps pass after
        pass. The first failure is raised once all have run: the worker heartbeat and the
        admin route still report it."""
        failures: list[Exception] = []

        def step(name: str, run, fallback):
            try:
                return run()
            except Exception as exc:
                logger.exception("queue_maintenance_step_failed step=%s", name)
                failures.append(exc)
                return fallback

        no_recovery = JobRecoveryResponse(recovered_job_ids=[], recovered_count=0)
        no_cleanup = JobCleanupResponse(deleted_job_ids=[], deleted_count=0)
        step("recover_pending_decompositions", self.recover_pending_decompositions, 0)
        search_recovery = step("recover_stale_search_jobs", self.recover_stale_search_task_jobs, no_recovery)
        finalize_recovery = step(
            "recover_stale_finalize_jobs", self.recover_stale_research_finalize_jobs, no_recovery
        )
        # After the stale-job recovery, so a research whose job was just requeued is busy.
        stalled_research_ids = step("sweep_stalled_researches", self.sweep_stalled_researches, [])
        search_cleanup = step("cleanup_old_search_jobs", self.cleanup_old_search_task_jobs, no_cleanup)
        finalize_cleanup = step("cleanup_old_finalize_jobs", self.cleanup_old_research_finalize_jobs, no_cleanup)
        step("cleanup_search_cache", self.cleanup_search_cache, 0)
        step("cleanup_old_researches", self.cleanup_old_researches, [])
        step("cleanup_old_telemetry", self.cleanup_old_telemetry, {})
        compacted_worker_names, compacted_research_ids = step(
            "compact_graph_operational_data", self.compact_graph_operational_data, ([], [])
        )
        if failures:
            raise failures[0]

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
            stalled_research_ids=stalled_research_ids,
            recovered_count=recovered_count,
            deleted_count=deleted_count,
            compacted_count=compacted_count,
            total_count=recovered_count + deleted_count + compacted_count + len(stalled_research_ids),
        )

    def get_latest_search_task_job(
        self,
        task_id: str,
        user_id: str | None = None,
    ) -> SearchTaskJob | None:
        return self.task_store.get_latest_search_task_job(task_id, user_id=user_id)
