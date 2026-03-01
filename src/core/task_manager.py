from typing import Dict, Optional, List
from datetime import datetime
from src.api.schemas import SearchTask, TaskStatus, TaskUpdate

class TaskManager:
    def __init__(self):
        self._tasks: Dict[str, SearchTask] = {}

    def add_task(self, task_data: dict) -> SearchTask:
        task = SearchTask(**task_data)
        self._tasks[task.id] = task
        return task

    def get_task(self, task_id: str) -> Optional[SearchTask]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[SearchTask]:
        return list(self._tasks.values())

    def update_task(self, task_id: str, update: TaskUpdate) -> Optional[SearchTask]:
        task = self._tasks.get(task_id)
        if not task:
            return None
        
        if update.status:
            task.status = update.status
        if update.result is not None:
            task.result = update.result
        if update.log:
            task.logs.append(update.log)
        
        task.updated_at = datetime.utcnow()
        return task

# Singleton instance
task_manager = TaskManager()
