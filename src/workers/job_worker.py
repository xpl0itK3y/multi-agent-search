import time

from src.config import settings
from src.services import ResearchService
from src.observability import bind_observability_context
from src.graph.metrics import get_graph_metrics_snapshot, get_graph_step_events_snapshot
from src.providers.search import get_extraction_metrics_snapshot
from src.workers.finalize_worker import FinalizeWorker
from src.workers.maintenance_worker import MaintenanceWorker
from src.workers.search_worker import SearchWorker


class JobWorker:
    def __init__(
        self,
        research_service: ResearchService,
        worker_name: str = "job-worker",
        maintenance_interval_seconds: float | None = None,
    ):
        self.research_service = research_service
        self.worker_name = worker_name
        self.maintenance_interval_seconds = max(
            0.0,
            settings.queue_maintenance_interval_seconds
            if maintenance_interval_seconds is None
            else maintenance_interval_seconds,
        )
        self.maintenance_worker = MaintenanceWorker(research_service)
        self._last_maintenance_at: float | None = None

    def _run_maintenance_if_due(self) -> int:
        now = time.monotonic()
        if (
            self._last_maintenance_at is not None
            and now - self._last_maintenance_at < self.maintenance_interval_seconds
        ):
            return 0
        self._last_maintenance_at = now
        return self.maintenance_worker.run_once()

    def run_once(self) -> int:
        last_error = None
        with bind_observability_context(worker_name=self.worker_name):
            try:
                maintenance_recovered = self._run_maintenance_if_due()
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
                    get_graph_step_events_snapshot(),
                )
                raise

            self.research_service.task_store.upsert_worker_heartbeat(
                self.worker_name,
                processed,
                status,
                last_error,
                get_extraction_metrics_snapshot(),
                get_graph_metrics_snapshot(),
                get_graph_step_events_snapshot(),
            )
            return processed
