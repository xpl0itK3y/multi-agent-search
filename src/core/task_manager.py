from typing import Dict, List, Optional
from datetime import datetime, timezone
import uuid
from src.api.schemas import SearchTask, TaskUpdate, ResearchRecord, ResearchStatus

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, SearchTask] = {}
        self.researches: Dict[str, ResearchRecord] = {}

    def add_research(self, request, task_ids: List[str]) -> ResearchRecord:
        research_id = str(uuid.uuid4())
        record = ResearchRecord(
            id=research_id,
            prompt=request.prompt,
            depth=request.depth,
            task_ids=task_ids
        )
        self.researches[research_id] = record
        return record

    def get_research(self, research_id: str) -> Optional[ResearchRecord]:
        return self.researches.get(research_id)

    def update_research_status(self, research_id: str, status: ResearchStatus, report: Optional[str] = None) -> Optional[ResearchRecord]:
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

    def get_task(self, task_id: str) -> Optional[SearchTask]:
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> List[SearchTask]:
        return list(self.tasks.values())
        
    def get_tasks_by_research(self, research_id: str) -> List[SearchTask]:
        return [t for t in self.tasks.values() if t.research_id == research_id]

    def update_task(self, task_id: str, update: TaskUpdate) -> Optional[SearchTask]:
        task = self.tasks.get(task_id)
        if not task:
            return None
            
        if update.status:
            task.status = update.status
        if update.result is not None:
            task.result = update.result
        if update.log:
            task.logs.append(update.log)
            
        task.updated_at = datetime.now(timezone.utc)
        return task

task_manager = TaskManager()
