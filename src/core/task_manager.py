from src.repositories.in_memory_task_store import InMemoryTaskStore


class TaskManager(InMemoryTaskStore):
    pass

task_manager = TaskManager()
