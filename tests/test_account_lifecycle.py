"""DATA-LIFECYCLE: account deletion cascades owned data and revokes share tokens."""
import httpx
import pytest

from src.api.app import create_app
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.domain.errors import BadRequestError, ForbiddenError, UnauthorizedError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def _service_with_user(store=None):
    store = store or InMemoryTaskStore()
    service = ResearchService(task_store=store)
    user = service.register_user("owner@example.com", "secret123")
    return store, service, user


@pytest.fixture
async def auth_client(monkeypatch):
    from src.auth.login_rate_limit import reset_auth_rate_limiter
    from src.config import settings

    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    # The lifespan fails fast on an insecure secret when auth is on; CI has no
    # AUTH_SECRET_KEY, so pin a strong one explicitly (a dev .env masked this locally).
    monkeypatch.setattr(settings, "auth_secret_key", "ci-test-secret-" + "x" * 40, raising=False)
    # Don't leak this file's register hits into other tests' per-IP login window.
    reset_auth_rate_limiter()
    app = create_app()
    service = ResearchService(task_store=InMemoryTaskStore())
    async with app.router.lifespan_context(app):
        app.state.research_service = service
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as test_client:
            yield test_client
    reset_auth_rate_limiter()


def test_delete_user_cascades_researches_tasks_and_share_tokens():
    store, service, user = _service_with_user()
    research = store.add_research(
        ResearchRequest(prompt="user owned topic", depth=SearchDepth.EASY),
        task_ids=[],
        user_id=user.id,
    )
    store.add_task(
        {
            "id": "task-owned",
            "description": "search",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "research_id": research.id,
        }
    )
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "final")
    store.merge_research_graph_state(research.id, {"share_token": "t" * 32})
    # Another user's data must survive.
    other = store.add_research(
        ResearchRequest(prompt="other user topic", depth=SearchDepth.EASY),
        task_ids=[],
        user_id="someone-else",
    )

    service.delete_user_account(user.id, current_password="secret123", confirm=True)

    assert store.get_user_by_id(user.id) is None
    assert store.get_research(research.id) is None
    assert store.get_task("task-owned") is None
    assert store.get_research(other.id) is not None


def test_delete_account_requires_confirm_and_current_password():
    store, service, user = _service_with_user()

    with pytest.raises(BadRequestError):
        service.delete_user_account(user.id)
    with pytest.raises(UnauthorizedError):
        service.delete_user_account(user.id, current_password="wrong", confirm=True)
    assert store.get_user_by_id(user.id) is not None

    service.delete_user_account(user.id, current_password="secret123", confirm=True)
    assert store.get_user_by_id(user.id) is None


def test_oauth_only_account_deletes_after_a_fresh_google_sign_in():
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)
    user, _created = service.get_or_create_oauth_user(
        "google@example.com", google_subject="sub-1"
    )
    # OAuth accounts have no password: a recent Google sign-in is the proof (SEC2-3), a
    # session alone (possibly stolen) is not.
    with pytest.raises(ForbiddenError) as refused:
        service.delete_user_account(user.id, confirm=True)
    assert refused.value.detail.startswith("reauth_required")
    assert store.get_user_by_id(user.id) is not None

    service.delete_user_account(user.id, confirm=True, fresh_google_auth=True)
    assert store.get_user_by_id(user.id) is None


@pytest.mark.anyio
async def test_delete_account_endpoint_flow(auth_client):
    registered = await auth_client.post(
        "/v1/auth/register",
        json={"email": "deleteme@example.com", "password": "secret123"},
    )
    assert registered.status_code == 200
    token = registered.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Guards first: no confirm → 400, wrong password → 401.
    unconfirmed = await auth_client.request(
        "DELETE", "/v1/auth/account", json={"confirm": False}, headers=headers
    )
    assert unconfirmed.status_code == 400
    wrong_password = await auth_client.request(
        "DELETE",
        "/v1/auth/account",
        json={"confirm": True, "current_password": "wrong"},
        headers=headers,
    )
    assert wrong_password.status_code == 401

    deleted = await auth_client.request(
        "DELETE",
        "/v1/auth/account",
        json={"confirm": True, "current_password": "secret123"},
        headers=headers,
    )
    assert deleted.status_code == 200
    assert deleted.json() == {"status": "deleted"}

    # The token no longer resolves to an account.
    me = await auth_client.get("/v1/auth/me", headers=headers)
    assert me.status_code == 401


@pytest.mark.postgres
def test_postgres_delete_user_cascades_via_fk(postgres_session_factory):
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

    pytest.importorskip("sqlalchemy")
    store = SQLAlchemyTaskStore(postgres_session_factory)
    store.create_user("cascade-user", "cascade@example.com", None)
    research = store.add_research(
        ResearchRequest(prompt="postgres cascade topic", depth=SearchDepth.EASY),
        task_ids=[],
        user_id="cascade-user",
    )
    store.add_task(
        {
            "id": "task-pg-cascade",
            "research_id": research.id,
            "description": "search",
            "queries": ["q"],
            "status": TaskStatus.PENDING,
        }
    )

    assert store.delete_user("cascade-user") is True

    assert store.get_user_by_id("cascade-user") is None
    assert store.get_research(research.id) is None
    assert store.get_task("task-pg-cascade") is None
