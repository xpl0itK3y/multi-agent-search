from src.config import Settings
from src.repositories.factory import create_task_store
from src.repositories.in_memory_task_store import InMemoryTaskStore


def test_create_task_store_returns_memory_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="memory"),
    )

    store = create_task_store()

    assert isinstance(store, InMemoryTaskStore)


def test_create_task_store_returns_postgres_store(monkeypatch):
    monkeypatch.setattr(
        "src.repositories.factory.settings",
        Settings(_env_file=None, task_store_backend="postgres"),
    )

    store = create_task_store()

    assert store.__class__.__name__ == "SQLAlchemyTaskStore"
