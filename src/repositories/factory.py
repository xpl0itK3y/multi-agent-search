from src.config import settings
from src.db import create_session_factory
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.repositories.protocols import TaskStore
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore


def create_task_store() -> TaskStore:
    backend = settings.task_store_backend.lower()

    if backend == "memory":
        return InMemoryTaskStore()

    if backend == "postgres":
        return SQLAlchemyTaskStore(create_session_factory())

    raise ValueError(f"Unsupported task store backend: {settings.task_store_backend}")
