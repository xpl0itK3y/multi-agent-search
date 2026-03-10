from sqlalchemy.orm import sessionmaker

from src.config import settings
from src.db import create_engine_from_settings
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.repositories.protocols import TaskStore
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

task_store = InMemoryTaskStore()


def create_task_store() -> TaskStore:
    backend = settings.task_store_backend.lower()

    if backend == "memory":
        return task_store

    if backend == "postgres":
        engine = create_engine_from_settings()
        session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
        return SQLAlchemyTaskStore(session_factory)

    raise ValueError(f"Unsupported task store backend: {settings.task_store_backend}")
