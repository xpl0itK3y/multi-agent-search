from src.services import ResearchService


class MaintenanceWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        result = self.research_service.run_queue_maintenance()
        return result.total_count
