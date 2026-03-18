from src.services import ResearchService


class MaintenanceWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        search_recovery = self.research_service.recover_stale_search_task_jobs()
        finalize_recovery = self.research_service.recover_stale_research_finalize_jobs()
        search_cleanup = self.research_service.cleanup_old_search_task_jobs()
        finalize_cleanup = self.research_service.cleanup_old_research_finalize_jobs()
        return (
            search_recovery.recovered_count
            + finalize_recovery.recovered_count
            + search_cleanup.deleted_count
            + finalize_cleanup.deleted_count
        )
