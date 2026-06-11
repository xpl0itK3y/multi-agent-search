import httpx
import pytest

from src.api.app import create_app
from src.core.llm import LLMProvider
from tests.postgres_helpers import (
    create_postgres_session_factory,
    require_postgres,
    truncate_runtime_tables,
)


class MockLLMProvider(LLMProvider):
    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        return f"Optimized: {user_prompt}"


@pytest.fixture
def mock_llm():
    return MockLLMProvider()


@pytest.fixture(autouse=True)
def _isolate_auth_settings(monkeypatch):
    """Pin auth settings to test defaults so a developer's local .env (which may set
    AUTH_DISABLED=false or Google OAuth creds) cannot change test behavior. Tests that
    need auth enabled / OAuth configured override these via mocker.patch."""
    from src.config import settings

    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "google_client_id", "", raising=False)
    monkeypatch.setattr(settings, "google_client_secret", "", raising=False)


@pytest.fixture
async def client():
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
            yield test_client


@pytest.fixture
def postgres_session_factory():
    engine, session_factory = create_postgres_session_factory()
    require_postgres(session_factory)
    truncate_runtime_tables(session_factory)
    try:
        yield session_factory
    finally:
        engine.dispose()
