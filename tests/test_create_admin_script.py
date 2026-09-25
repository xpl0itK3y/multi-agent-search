"""scripts/create_admin.py: the operator path for ADMIN_EMAILS accounts."""
import importlib.util
import io
from pathlib import Path

import pytest

from src.config import settings
from src.domain.errors import ForbiddenError, UnauthorizedError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

ROOT = Path(__file__).resolve().parents[1]
ADMIN = "ops-admin@example.com"


def _load_script():
    spec = importlib.util.spec_from_file_location("create_admin", ROOT / "scripts" / "create_admin.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(settings, "admin_emails", ADMIN, raising=False)
    return ResearchService(task_store=InMemoryTaskStore())


def test_create_admin_creates_account_that_can_log_in(service):
    script = _load_script()

    user, created = script.create_admin("Ops-Admin@Example.com", "initial-pass1", service=service)

    assert created is True
    assert user.email == ADMIN and user.is_admin is True
    assert service.authenticate_user(ADMIN, "initial-pass1").id == user.id


def test_create_admin_sets_password_of_google_admin_and_revokes_sessions(service):
    script = _load_script()
    google_user, _ = service.get_or_create_oauth_user(ADMIN, google_subject="sub-ops")

    user, created = script.create_admin(ADMIN, "operator-pass1", service=service)

    assert created is False
    assert user.id == google_user.id
    assert user.token_version == google_user.token_version + 1
    assert service.authenticate_user(ADMIN, "operator-pass1").id == google_user.id


def test_create_admin_replacing_password_revokes_old_one(service):
    script = _load_script()
    first, _ = script.create_admin(ADMIN, "first-pass1", service=service)

    second, created = script.create_admin(ADMIN, "second-pass1", service=service)

    assert created is False
    assert second.token_version == first.token_version + 1
    with pytest.raises(UnauthorizedError):
        service.authenticate_user(ADMIN, "first-pass1")
    assert service.authenticate_user(ADMIN, "second-pass1").id == first.id


def test_create_admin_refuses_email_outside_admin_emails(service):
    script = _load_script()

    with pytest.raises(ForbiddenError):
        script.create_admin("someone@example.com", "whatever-pass1", service=service)

    assert service.task_store.get_user_by_email("someone@example.com") is None


def test_main_reads_password_from_stdin_not_argv(service, monkeypatch, capsys):
    script = _load_script()
    monkeypatch.setattr("sys.stdin", io.StringIO("stdin-pass-123\n"))

    assert script.main([ADMIN, "--password-stdin"], service=service) == 0

    assert "stdin-pass-123" not in capsys.readouterr().out
    assert service.authenticate_user(ADMIN, "stdin-pass-123").email == ADMIN


def test_main_prompts_with_getpass_and_rejects_mismatch(service, monkeypatch):
    script = _load_script()
    answers = iter(["typed-pass-1", "typed-pass-2"])
    monkeypatch.setattr(script.getpass, "getpass", lambda prompt="": next(answers))

    with pytest.raises(SystemExit):
        script.main([ADMIN], service=service)

    assert service.task_store.get_user_by_email(ADMIN) is None


def test_main_reports_refusal_with_nonzero_exit(service, monkeypatch, capsys):
    script = _load_script()
    monkeypatch.setattr("sys.stdin", io.StringIO("whatever-pass1\n"))

    assert script.main(["intruder@example.com", "--password-stdin"], service=service) == 1
    assert "ADMIN_EMAILS" in capsys.readouterr().err


def test_main_has_no_password_argument():
    script = _load_script()

    with pytest.raises(SystemExit):
        script.main([ADMIN, "--password", "on-the-command-line"])


def test_create_admin_refuses_memory_store(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)

    with pytest.raises(SystemExit):
        script.create_admin(ADMIN, "whatever-pass1")


def test_main_refuses_memory_store_before_prompting(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)
    prompted = []
    monkeypatch.setattr(script.getpass, "getpass", lambda prompt="": prompted.append(prompt) or "x")

    with pytest.raises(SystemExit):
        script.main([ADMIN])

    assert prompted == []
