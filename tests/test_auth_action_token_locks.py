"""BE-2: every write to an account's one-time links locks the users row before the
auth_action_tokens rows, so concurrent calls for one account wait instead of deadlocking.

A redeem (reset-password, verify-email) used to consume its token first, holding that
row, and then wait for the users row, while create_auth_action_token and delete_user hold
the users row and then wait for the account's tokens. PostgreSQL broke the cycle by
aborting one side: a 500 on the reset or verify page, or a mail silently not sent.

Postgres-only: the in-memory store runs each of these under one lock. Each test pauses
the first call right after its first statement on those tables (it then holds that
statement's row locks), starts the second call, waits until it blocks on a lock, and lets
the first go on. Run against the throwaway test database, never the live stack.
"""
import hashlib
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, text

from src.domain import AuthActionPurpose
from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore
from tests.postgres_helpers import truncate_runtime_tables

pytestmark = pytest.mark.postgres

_TABLES = ("users", "auth_action_tokens")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class _Call(threading.Thread):
    """One store call on its own thread (so on its own connection), result or error kept."""

    def __init__(self, name, work):
        super().__init__(name=name, daemon=True)
        self.work = work
        self.result = None
        self.error: Exception | None = None

    def run(self) -> None:
        try:
            self.result = self.work()
        except Exception as exc:  # the deadlock victim's OperationalError, among others
            self.error = exc


def _a_backend_waits_for_a_lock(engine) -> bool:
    with engine.connect() as conn:
        waiting = conn.execute(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = current_database() AND wait_event_type = 'Lock'"
            )
        ).scalar()
    return bool(waiting)


def _interleave(engine, first: _Call, second: _Call) -> None:
    holds = threading.Event()
    release = threading.Event()

    def pause(conn, cursor, statement, parameters, context, executemany):
        if threading.current_thread() is first and not holds.is_set() and any(t in statement for t in _TABLES):
            holds.set()
            release.wait(timeout=30)

    event.listen(engine, "after_cursor_execute", pause)
    try:
        first.start()
        assert holds.wait(timeout=10), "the first call never reached the database"
        second.start()
        deadline = time.monotonic() + 10
        while second.is_alive() and not _a_backend_waits_for_a_lock(engine) and time.monotonic() < deadline:
            time.sleep(0.05)
        release.set()
        first.join(timeout=30)
        second.join(timeout=30)
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", pause)
    assert not first.is_alive() and not second.is_alive()


@pytest.fixture
def account(_postgres_test_db):
    engine, session_factory = _postgres_test_db
    truncate_runtime_tables(session_factory)
    store = SQLAlchemyTaskStore(session_factory)
    tag = uuid.uuid4().hex[:8]
    user = store.create_user(f"lock-{tag}", f"lock-{tag}@example.com", "hash-1")
    return engine, store, user


def _issue(store, user, purpose, token: str):
    expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    return store.create_auth_action_token(user.id, purpose, _hash(token), user.email, expires_at)


def _redeem(store, user, purpose, token: str):
    if purpose == AuthActionPurpose.PASSWORD_RESET:
        return store.reset_password_with_token(_hash(token), "hash-2")
    return store.verify_email_with_token(_hash(token), user.id)


PURPOSES = [AuthActionPurpose.PASSWORD_RESET, AuthActionPurpose.EMAIL_VERIFICATION]


@pytest.mark.parametrize("purpose", PURPOSES)
def test_a_new_link_issued_while_one_is_redeemed_waits_for_it(account, purpose):
    engine, store, user = account
    _issue(store, user, purpose, "old")
    redeem = _Call("redeem", lambda: _redeem(store, user, purpose, "old"))
    issue = _Call("issue", lambda: _issue(store, user, purpose, "new"))

    _interleave(engine, redeem, issue)

    assert (redeem.error, issue.error) == (None, None)
    assert redeem.result is not None and redeem.result.id == user.id
    assert store.get_live_auth_action_token(_hash("new"), purpose).id == issue.result.id
    assert store.get_live_auth_action_token(_hash("old"), purpose) is None


@pytest.mark.parametrize("purpose", PURPOSES)
def test_a_link_redeemed_while_a_new_one_is_issued_waits_and_is_superseded(account, purpose):
    engine, store, user = account
    _issue(store, user, purpose, "old")
    issue = _Call("issue", lambda: _issue(store, user, purpose, "new"))
    redeem = _Call("redeem", lambda: _redeem(store, user, purpose, "old"))

    _interleave(engine, issue, redeem)

    assert (issue.error, redeem.error) == (None, None)
    assert redeem.result is None  # the newer link retired it before the redeem got its turn
    assert store.get_user_by_id(user.id).password_hash == "hash-1"
    assert store.get_live_auth_action_token(_hash("new"), purpose).id == issue.result.id


def test_deleting_an_account_while_its_link_is_redeemed_waits_for_it(account):
    engine, store, user = account
    _issue(store, user, AuthActionPurpose.PASSWORD_RESET, "old")
    redeem = _Call("redeem", lambda: _redeem(store, user, AuthActionPurpose.PASSWORD_RESET, "old"))
    delete = _Call("delete", lambda: store.delete_user(user.id))

    _interleave(engine, redeem, delete)

    assert (redeem.error, delete.error) == (None, None)
    assert redeem.result is not None and delete.result is True
    assert store.get_user_by_id(user.id) is None
