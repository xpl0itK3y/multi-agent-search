import logging

from src.services import ResearchService

logger = logging.getLogger(__name__)


class FinalizeWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def run_once(self) -> int:
        processed = 0

        while True:
            job = self.research_service.task_store.claim_next_research_finalize_job()
            if job is None:
                break
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )
            logger.info("finalize_job_claimed job_id=%s research_id=%s", job.id, job.research_id)
            self.research_service.process_finalize_job(job.id)
            processed += 1
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )

        return processed
