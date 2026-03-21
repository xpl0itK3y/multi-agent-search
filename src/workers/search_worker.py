import logging

from src.services import ResearchService

logger = logging.getLogger(__name__)


class SearchWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def run_once(self) -> int:
        processed = 0

        while True:
            job = self.research_service.task_store.claim_next_search_task_job()
            if job is None:
                break
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )
            logger.info("search_job_claimed job_id=%s task_id=%s depth=%s", job.id, job.task_id, job.depth.value)
            self.research_service.process_search_task_job(job.id)
            processed += 1
            self.research_service.touch_worker_heartbeat(
                self.worker_name,
                processed,
                "busy",
            )

        return processed
