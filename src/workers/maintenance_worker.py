import logging
from datetime import datetime, timezone

from src.observability import bind_observability_context
from src.services import ResearchService

logger = logging.getLogger(__name__)


class MaintenanceWorker:
    MAINTENANCE_HISTORY_LIMIT = 20

    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        with bind_observability_context(worker_name="maintenance"):
            result = self.research_service.run_queue_maintenance()
            current_timestamp = datetime.now(timezone.utc).isoformat()
            previous_heartbeat = self.research_service.task_store.get_worker_heartbeat("maintenance")
            previous_runs = []
            if previous_heartbeat is not None:
                previous_runs = [
                    item.model_dump(mode="json")
                    for item in previous_heartbeat.maintenance_summary.recent_runs
                ]
            previous_runs.append(
                {
                    "recovered_count": result.recovered_count,
                    "deleted_count": result.deleted_count,
                    "compacted_count": result.compacted_count,
                    "total_count": result.total_count,
                    "last_run_at": current_timestamp,
                }
            )
            self.research_service.touch_worker_heartbeat(
                "maintenance",
                result.total_count,
                "busy" if result.total_count else "idle",
                maintenance_summary={
                    **result.model_dump(),
                    "last_run_at": current_timestamp,
                    "recent_runs": previous_runs[-self.MAINTENANCE_HISTORY_LIMIT :],
                },
            )
            if result.total_count:
                logger.info(
                    "queue_maintenance_completed recovered_count=%s deleted_count=%s compacted_count=%s total_count=%s",
                    result.recovered_count,
                    result.deleted_count,
                    result.compacted_count,
                    result.total_count,
                )
            return result.total_count
