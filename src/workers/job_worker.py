from src.services import ResearchService
from src.workers.finalize_worker import FinalizeWorker
from src.workers.maintenance_worker import MaintenanceWorker
from src.workers.search_worker import SearchWorker


class JobWorker:
    def __init__(self, research_service: ResearchService, worker_name: str = "job-worker"):
        self.research_service = research_service
        self.worker_name = worker_name

    def run_once(self) -> int:
        last_error = None
        try:
            maintenance_recovered = MaintenanceWorker(self.research_service).run_once()
            search_processed = SearchWorker(self.research_service).run_once()
            finalize_processed = FinalizeWorker(self.research_service).run_once()
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
            )
            raise

        self.research_service.task_store.upsert_worker_heartbeat(
            self.worker_name,
            processed,
            status,
            last_error,
        )
        return processed
