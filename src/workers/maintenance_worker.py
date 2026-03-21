import logging

from src.services import ResearchService

logger = logging.getLogger(__name__)


class MaintenanceWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        result = self.research_service.run_queue_maintenance()
        if result.total_count:
            logger.info(
                "queue_maintenance_completed recovered_count=%s deleted_count=%s total_count=%s",
                result.recovered_count,
                result.deleted_count,
                result.total_count,
            )
        return result.total_count
