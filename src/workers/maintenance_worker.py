import logging
from datetime import datetime, timezone

from src.observability import bind_observability_context, observe_worker_job
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
            previous_operational_history = []
            previous_recommendation_history = []
            previous_recommendation_events = []
            if previous_heartbeat is not None:
                previous_runs = [
                    item.model_dump(mode="json")
                    for item in previous_heartbeat.maintenance_summary.recent_runs
                ]
                previous_operational_history = [
                    item.model_dump(mode="json")
                    for item in previous_heartbeat.maintenance_summary.recent_operational_health
                ]
                previous_recommendation_history = [
                    item.model_dump(mode="json")
                    for item in previous_heartbeat.maintenance_summary.recent_operational_recommendations
                ]
                previous_recommendation_events = [
                    item.model_dump(mode="json")
                    for item in previous_heartbeat.maintenance_summary.recent_operational_recommendation_events
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
                    "recent_operational_health": previous_operational_history[-self.MAINTENANCE_HISTORY_LIMIT :],
                    "recent_operational_recommendations": previous_recommendation_history[-self.MAINTENANCE_HISTORY_LIMIT :],
                    "recent_operational_recommendation_events": previous_recommendation_events[-self.MAINTENANCE_HISTORY_LIMIT :],
                },
            )
            queue_metrics = self.research_service.get_queue_metrics()
            maintenance_summary = queue_metrics.maintenance_summary.model_dump(mode="json")
            maintenance_summary["recent_operational_health"] = [
                item.model_dump(mode="json")
                for item in queue_metrics.operational_health.history[-self.MAINTENANCE_HISTORY_LIMIT :]
            ]
            maintenance_summary["recent_operational_recommendations"] = [
                item.model_dump(mode="json")
                for item in queue_metrics.operational_health.recommendations[-self.MAINTENANCE_HISTORY_LIMIT :]
            ]
            self.research_service.touch_worker_heartbeat(
                "maintenance",
                result.total_count,
                "busy" if result.total_count else "idle",
                maintenance_summary=maintenance_summary,
            )
            observe_worker_job("maintenance", "maintenance", "success")
            if result.total_count:
                logger.info(
                    "queue_maintenance_completed recovered_count=%s deleted_count=%s compacted_count=%s total_count=%s",
                    result.recovered_count,
                    result.deleted_count,
                    result.compacted_count,
                    result.total_count,
                )
            return result.total_count
