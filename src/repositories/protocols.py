from typing import Protocol

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchFinalizeJob,
    ResearchRecord,
    ResearchRequest,
    ResearchStatus,
    SearchTask,
    TaskUpdate,
)


class TaskStore(Protocol):
    def add_research(self, request: ResearchRequest, task_ids: list[str]) -> ResearchRecord: ...

    def get_research(self, research_id: str) -> ResearchRecord | None: ...

    def update_research_status(
        self,
        research_id: str,
        status: ResearchStatus,
        report: str | None = None,
    ) -> ResearchRecord | None: ...

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None: ...

    def add_research_finalize_job(self, research_id: str) -> ResearchFinalizeJob: ...

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None: ...

    def get_pending_research_finalize_jobs(self) -> list[ResearchFinalizeJob]: ...

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
    ) -> ResearchFinalizeJob | None: ...

    def add_task(self, task_data: dict) -> SearchTask: ...

    def get_task(self, task_id: str) -> SearchTask | None: ...

    def get_all_tasks(self) -> list[SearchTask]: ...

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]: ...

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None: ...
