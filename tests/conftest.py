import httpx
import pytest

from src.api.app import create_app
from src.core.llm import LLMProvider
from tests.postgres_helpers import truncate_runtime_tables


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
    from src.auth.admin_rate_limit import reset_admin_rate_limiter
    from src.auth.login_rate_limit import reset_auth_rate_limiter
    from src.auth.telemetry_rate_limit import reset_telemetry_rate_limiter
    from src.config import settings

    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "", raising=False)
    monkeypatch.setattr(settings, "google_client_id", "", raising=False)
    monkeypatch.setattr(settings, "google_client_secret", "", raising=False)
    # The auth limiters are process-wide: without a reset, one test's login/register hits
    # (all from the same test client address) push a later test over the limit.
    reset_auth_rate_limiter()
    reset_admin_rate_limiter()
    reset_telemetry_rate_limiter()


@pytest.fixture
async def client():
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
            yield test_client


@pytest.fixture(scope="session")
def _postgres_test_db():
    """Session-scoped throwaway database, migrated to head (never the dev DB —
    it may sit on another branch's alembic revision)."""
    from tests.postgres_helpers import (
        POSTGRES_TEST_DATABASE,
        create_migrated_throwaway_database,
        drop_throwaway_database,
        server_base_url,
    )

    engine, session_factory = create_migrated_throwaway_database(POSTGRES_TEST_DATABASE)
    yield engine, session_factory
    engine.dispose()
    drop_throwaway_database(server_base_url(), POSTGRES_TEST_DATABASE)


@pytest.fixture
def postgres_session_factory(_postgres_test_db):
    engine, session_factory = _postgres_test_db
    truncate_runtime_tables(session_factory)
    yield session_factory
