from src.services import ResearchService
from src.observability import bind_observability_context
from src.graph.metrics import get_graph_metrics_snapshot
from src.providers.search import get_extraction_metrics_snapshot
from src.workers.finalize_worker import FinalizeWorker
from src.workers.maintenance_worker import MaintenanceWorker
from src.workers.search_worker import SearchWorker


class JobWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def run_once(self) -> int:
        last_error = None
        with bind_observability_context(worker_name=self.worker_name):
            try:
                maintenance_recovered = MaintenanceWorker(self.research_service).run_once()
                search_processed = SearchWorker(self.research_service, worker_name=self.worker_name).run_once()
                finalize_processed = FinalizeWorker(self.research_service, worker_name=self.worker_name).run_once()
                processed = maintenance_recovered + search_processed + finalize_processed
                status = "busy" if processed else "idle"
            except Exception as exc:
                processed = 0
                status = "error"
                last_error = str(exc)
                self.research_service.task_store.upsert_worker_heartbeat(
                    self.worker_name,
                    processed,
                    status,
                    last_error,
                    get_extraction_metrics_snapshot(),
                    get_graph_metrics_snapshot(),
                )
                raise

            self.research_service.task_store.upsert_worker_heartbeat(
                self.worker_name,
                processed,
                status,
                last_error,
                get_extraction_metrics_snapshot(),
                get_graph_metrics_snapshot(),
            )
            return processed
