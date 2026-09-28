import logging

from src.domain import SearchJobStatus, SearchTaskJob
from src.graph.metrics import get_graph_metrics_snapshot, get_graph_step_events_snapshot
from src.observability import bind_observability_context, observe_worker_job
from src.providers.search import get_extraction_metrics_snapshot
from src.services import ResearchService

logger = logging.getLogger(__name__)


def search_attempt_status(job: SearchTaskJob | None, claimed_lease_epoch: int | None = None) -> str:
    """The mas_worker_jobs_total status of one processed attempt. process_search_task_job
    returns instead of raising when an attempt fails: a FAILED task or an exception leaves
    the job PENDING (retry scheduled) or DEAD_LETTER, a missing task leaves it FAILED, a
    job deleted mid-attempt comes back None, and a lost lease returns the job as its new
    holder left it. Only COMPLETED (searched, or skipped because the research had ended)
    under the lease this attempt claimed is a success, as for finalize attempts."""
    if (
        job is not None
        and job.status == SearchJobStatus.COMPLETED
        and (claimed_lease_epoch is None or job.lease_epoch == claimed_lease_epoch)
    ):
        return "success"
    return "failure"


class SearchWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def _process_job(self, claimed: SearchTaskJob, processed: int) -> int:
        job_id = claimed.id
        with bind_observability_context(
            worker_name=self.worker_name,
            job_id=job_id,
        ):
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
                extraction_metrics=get_extraction_metrics_snapshot(),
                graph_metrics=get_graph_metrics_snapshot(),
                graph_step_events=get_graph_step_events_snapshot(),
            )
            logger.info("search_job_claimed job_id=%s", job_id)
            try:
                job = self.research_service.process_search_task_job(job_id, lease_epoch=claimed.lease_epoch)
            except Exception:
                observe_worker_job(self.worker_name, "search", "failure")
                raise
            processed += 1
            observe_worker_job(self.worker_name, "search", search_attempt_status(job, claimed.lease_epoch))
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
                extraction_metrics=get_extraction_metrics_snapshot(),
                graph_metrics=get_graph_metrics_snapshot(),
                graph_step_events=get_graph_step_events_snapshot(),
            )
        return processed

    def run_once(self) -> int:
        processed = 0
        broker = getattr(self.research_service, "broker", None)

        if broker is not None:
            # Redis mode: BLPOP for one job_id, then claim it specifically in Postgres.
            task_store = self.research_service.task_store
            job_id = broker.pop_search_job()
            job = task_store.claim_search_task_job_by_id(job_id) if job_id is not None else None
            if job_id is not None and job is None:
                logger.debug("search_job_skip_already_claimed job_id=%s", job_id)
            if job is None:
                # Nothing claimable was popped: also try the atomic SKIP LOCKED claim, so a
                # PENDING job whose push was lost (Redis restart, failed push) is not
                # stranded. Claims are exclusive; a later pop of its id finds it taken.
                job = task_store.claim_next_search_task_job()
                if job is None:
                    return 0
                logger.info("search_job_claimed_without_push job_id=%s", job.id)
            return self._process_job(job, processed)

        # Postgres polling mode: drain all pending jobs in one pass.
        while True:
            job = self.research_service.task_store.claim_next_search_task_job()
            if job is None:
                break
            processed = self._process_job(job, processed)

        return processed
