from src.services import ResearchService


class FinalizeWorker:
    def __init__(self, research_service: ResearchService):
        self.research_service = research_service

    def run_once(self) -> int:
        processed = 0

        while True:
            job = self.research_service.task_store.claim_next_research_finalize_job()
            if job is None:
                break
            self.research_service.process_finalize_job(job.id)
            processed += 1

        return processed
