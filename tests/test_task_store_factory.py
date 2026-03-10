from src.config import Settings
from src.core.task_manager import task_manager
from src.repositories.factory import create_task_store


def test_create_task_store_returns_memory_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="memory"),
    )

    store = create_task_store()

    assert store is task_manager


def test_create_task_store_returns_postgres_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="postgres"),
    )

    store = create_task_store()

    assert store.__class__.__name__ == "SQLAlchemyTaskStore"
