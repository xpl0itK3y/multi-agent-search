"""scripts/issue_password_reset.py: the operator's out-of-band password reset link."""
import importlib.util
import logging
from pathlib import Path

import pytest

from src.auth.security import hash_password
from src.config import settings
from src.domain.errors import UnauthorizedError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

ROOT = Path(__file__).resolve().parents[1]


def _load_script():
    spec = importlib.util.spec_from_file_location("issue_password_reset", ROOT / "scripts" / "issue_password_reset.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def service():
    service = ResearchService(task_store=InMemoryTaskStore())  # EMAIL_BACKEND=disabled (conftest)
    service.task_store.create_user("u-locked", "locked@example.com", hash_password("forgotten-pass1"))
    return service


def test_main_prints_a_working_one_time_link_to_stdout_only(service, capsys, caplog):
    script = _load_script()

    with caplog.at_level(logging.DEBUG):
        assert script.main([" Locked@Example.com "], service=service) == 0

    out, err = capsys.readouterr()
    link = out.strip()
    assert link.startswith(f"{settings.public_app_url}/reset-password#token=")
    assert "\n" not in link
    token = link.rpartition("#token=")[2]
    assert token not in err and "locked@example.com" in err and "valid for 60 min" in err
    assert all(token not in record.getMessage() for record in caplog.records)
    stored = list(service.task_store.auth_action_tokens.values())
    assert len(stored) == 1 and token not in str(stored)  # only its hash is kept

    service.reset_password_with_token(token, "operator-set-pass1")
    assert service.authenticate_user("locked@example.com", "operator-set-pass1").email == "locked@example.com"
    with pytest.raises(UnauthorizedError):
        service.authenticate_user("locked@example.com", "forgotten-pass1")


def test_a_new_link_replaces_the_previous_one(service, capsys):
    script = _load_script()
    script.main(["locked@example.com"], service=service)
    first = capsys.readouterr().out.strip().rpartition("#token=")[2]
    script.main(["locked@example.com"], service=service)
    second = capsys.readouterr().out.strip().rpartition("#token=")[2]

    assert first != second
    with pytest.raises(Exception, match="reset_token_invalid"):
        service.reset_password_with_token(first, "operator-set-pass1")
    assert service.reset_password_with_token(second, "operator-set-pass1").email == "locked@example.com"


def test_an_unknown_email_exits_1_and_stores_nothing(service, capsys):
    script = _load_script()

    assert script.main(["nobody@example.com"], service=service) == 1

    out, err = capsys.readouterr()
    assert out == "" and "no account has the email nobody@example.com" in err
    assert service.task_store.auth_action_tokens == {}


def test_the_memory_store_is_refused(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(settings, "task_store_backend", "memory", raising=False)

    with pytest.raises(SystemExit, match="TASK_STORE_BACKEND=memory"):
        script.main(["locked@example.com"])
    with pytest.raises(SystemExit):
        script.issue_password_reset("locked@example.com")
