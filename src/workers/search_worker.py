import logging

from src.observability import bind_observability_context
from src.graph.metrics import get_graph_metrics_snapshot, get_graph_step_events_snapshot
from src.providers.search import get_extraction_metrics_snapshot
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
            with bind_observability_context(
                worker_name=self.worker_name,
                job_id=job.id,
                task_id=job.task_id,
            ):
                self.research_service.touch_worker_heartbeat(
                    self.worker_name,
                    processed,
                    "busy",
                    extraction_metrics=get_extraction_metrics_snapshot(),
                    graph_metrics=get_graph_metrics_snapshot(),
                    graph_step_events=get_graph_step_events_snapshot(),
                )
                logger.info("search_job_claimed depth=%s", job.depth.value)
                self.research_service.process_search_task_job(job.id)
                processed += 1
                self.research_service.touch_worker_heartbeat(
                    self.worker_name,
                    processed,
                    "busy",
                    extraction_metrics=get_extraction_metrics_snapshot(),
                    graph_metrics=get_graph_metrics_snapshot(),
                    graph_step_events=get_graph_step_events_snapshot(),
                )

        return processed
