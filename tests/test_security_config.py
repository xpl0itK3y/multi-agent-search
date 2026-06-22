import pytest

from src.bootstrap import _INSECURE_SECRET_DEFAULT, _validate_security_config
from src.config import settings


def test_validate_security_config_skips_when_auth_disabled(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", _INSECURE_SECRET_DEFAULT, raising=False)
    _validate_security_config()  # must not raise in single-tenant / dev mode


def test_validate_security_config_rejects_default_secret(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", _INSECURE_SECRET_DEFAULT, raising=False)
    with pytest.raises(RuntimeError, match="AUTH_SECRET_KEY"):
        _validate_security_config()


def test_validate_security_config_rejects_short_secret(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "too-short", raising=False)
    with pytest.raises(RuntimeError):
        _validate_security_config()


def test_validate_security_config_accepts_strong_secret(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "auth_cookie_secure", True, raising=False)
    _validate_security_config()  # strong key + secure cookie must pass
