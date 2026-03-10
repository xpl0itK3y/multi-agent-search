from src.services import ResearchService


class SearchWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        processed = 0

        while True:
            job = self.research_service.task_store.claim_next_search_task_job()
            if job is None:
                break
            self.research_service.process_search_task_job(job.id)
            processed += 1

        return processed
