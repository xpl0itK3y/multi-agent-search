from datetime import datetime, timezone
import uuid

from src.api.schemas import ResearchRecord, ResearchRequest, ResearchStatus, SearchTask, TaskUpdate


class InMemoryTaskStore:
    def __init__(self):
        self.tasks: dict[str, SearchTask] = {}
        self.researches: dict[str, ResearchRecord] = {}

    def add_research(self, request: ResearchRequest, task_ids: list[str]) -> ResearchRecord:
        research_id = str(uuid.uuid4())
        record = ResearchRecord(
            id=research_id,
            prompt=request.prompt,
            depth=request.depth,
            task_ids=task_ids,
        )
        self.researches[research_id] = record
        return record

    def get_research(self, research_id: str) -> ResearchRecord | None:
        return self.researches.get(research_id)

    def update_research_status(
        self,
        research_id: str,
        status: ResearchStatus,
        report: str | None = None,
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research:
            research.status = status
            if report:
                research.final_report = report
            research.updated_at = datetime.now(timezone.utc)
        return research

    def add_task(self, task_data: dict) -> SearchTask:
        task = SearchTask(**task_data)
        self.tasks[task.id] = task
        return task

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None:
        research = self.researches.get(research_id)
        if research is None:
            return None

        research.task_ids = task_ids
        research.updated_at = datetime.now(timezone.utc)
        return research

    def get_task(self, task_id: str) -> SearchTask | None:
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> list[SearchTask]:
        return list(self.tasks.values())

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]:
        return [task for task in self.tasks.values() if task.research_id == research_id]

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        task = self.tasks.get(task_id)
        if not task:
            return None

        if update.status is not None:
            task.status = update.status
        if update.result is not None:
            task.result = update.result
        if update.log:
            task.logs.append(update.log)

        task.updated_at = datetime.now(timezone.utc)
        return task
