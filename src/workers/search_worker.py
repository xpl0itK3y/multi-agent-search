from src.services import ResearchService


class SearchWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        jobs = self.research_service.task_store.get_pending_search_task_jobs()
        processed = 0

        for job in jobs:
            self.research_service.process_search_task_job(job.id)
            processed += 1

        return processed
