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


@pytest.mark.parametrize("secret", [_INSECURE_SECRET_DEFAULT, "too-short"])
def test_validate_security_config_rejects_weak_secret_when_admins_are_listed(monkeypatch, secret):
    """AUTH_DISABLED=true still enforces admin tokens once ADMIN_EMAILS is set, so a public
    default key would let anyone sign one."""
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", secret, raising=False)
    with pytest.raises(RuntimeError, match="ADMIN_EMAILS"):
        _validate_security_config()


def test_validate_security_config_accepts_strong_secret_with_admins_and_auth_disabled(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    _validate_security_config()


@pytest.mark.anyio
async def test_api_refuses_to_start_with_default_secret_when_admins_are_listed(monkeypatch):
    from src.api.app import create_app

    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", _INSECURE_SECRET_DEFAULT, raising=False)
    app = create_app()
    with pytest.raises(RuntimeError, match="AUTH_SECRET_KEY"):
        async with app.router.lifespan_context(app):
            pass
