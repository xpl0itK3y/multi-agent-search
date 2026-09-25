import logging

from src.domain import FinalizeJobStatus, ResearchFinalizeJob
from src.observability import bind_observability_context, observe_worker_job
from src.services import ResearchService

logger = logging.getLogger(__name__)


def finalize_attempt_status(job: ResearchFinalizeJob | None, claimed_lease_epoch: int) -> str:
    """The mas_worker_jobs_total status of one processed attempt. process_finalize_job
    returns instead of raising when an attempt fails: an exception leaves the job PENDING
    (retry scheduled) or DEAD_LETTER, and a lost lease returns the job as its new holder
    left it. Only the job COMPLETED under the lease this attempt claimed is a success. A
    lease bumped mid-attempt (stale recovery, a requeue) means the attempt lost it, even
    when the recovery or the next runner then closed the job."""
    if (
        job is not None
        and job.status == FinalizeJobStatus.COMPLETED
        and job.lease_epoch == claimed_lease_epoch
    ):
        return "success"
    return "failure"


class FinalizeWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def _process_job(self, claimed: ResearchFinalizeJob, processed: int) -> int:
        # Read before processing: the in-memory store hands out its own mutable job.
        job_id, claimed_lease_epoch = claimed.id, claimed.lease_epoch
        with bind_observability_context(
            worker_name=self.worker_name,
            job_id=job_id,
        ):
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )
            logger.info("finalize_job_claimed job_id=%s", job_id)
            try:
                job = self.research_service.process_finalize_job(job_id)
            except Exception:
                observe_worker_job(self.worker_name, "finalize", "failure")
                raise
            processed += 1
            observe_worker_job(
                self.worker_name, "finalize", finalize_attempt_status(job, claimed_lease_epoch)
            )
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )
        return processed

    def run_once(self) -> int:
        processed = 0
        broker = getattr(self.research_service, "broker", None)

        if broker is not None:
            # Redis mode: BLPOP for one job_id, then claim it specifically in Postgres.
            task_store = self.research_service.task_store
            job_id = broker.pop_finalize_job()
            job = task_store.claim_research_finalize_job_by_id(job_id) if job_id is not None else None
            if job_id is not None and job is None:
                logger.debug("finalize_job_skip_already_claimed job_id=%s", job_id)
            if job is None:
                # Same lost-push fallback as the search worker: the SKIP LOCKED claim of
                # the oldest PENDING job.
                job = task_store.claim_next_research_finalize_job()
                if job is None:
                    return 0
                logger.info("finalize_job_claimed_without_push job_id=%s", job.id)
            return self._process_job(job, processed)

        # Postgres polling mode: drain all pending jobs in one pass.
        while True:
            job = self.research_service.task_store.claim_next_research_finalize_job()
            if job is None:
                break
            processed = self._process_job(job, processed)

        return processed
