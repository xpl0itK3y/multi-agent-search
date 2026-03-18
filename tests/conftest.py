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
