"""The documented quick start boots, and its AUTH_SECRET_KEY placeholder fails closed.

Auth is on by default and bootstrap refuses the built-in or a short AUTH_SECRET_KEY.
The README once said auth was off by default and its minimal .env set no AUTH_*
value, so an operator who followed it got an API that never started. The key must
be in both example files with a value that is rejected as shipped, so that nobody
runs with a placeholder secret, and filling it in as the docs say must be enough.
"""
import re
import secrets
from pathlib import Path

import pytest

import src.bootstrap as bootstrap
from src.config import Settings

ROOT = Path(__file__).resolve().parents[1]
AUTH_ENV = ("AUTH_DISABLED", "AUTH_SECRET_KEY", "ADMIN_EMAILS", "AUTH_COOKIE_SECURE")


def _readme_minimal_env() -> str:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    match = re.search(r"Minimal `\.env`:\s*```env\n(.*?)```", readme, re.S)
    assert match, "README lost its 'Minimal `.env`' block"
    return match.group(1)


EXAMPLES = {
    "README minimal .env": _readme_minimal_env,
    ".env.example": lambda: (ROOT / ".env.example").read_text(encoding="utf-8"),
}


def _settings_from(text: str, tmp_path: Path) -> Settings:
    env_file = tmp_path / ".env"
    env_file.write_text(text, encoding="utf-8")
    return Settings(_env_file=env_file)


@pytest.fixture(autouse=True)
def _no_auth_env(monkeypatch):
    # CI and the local test command export AUTH_DISABLED=true; the files must stand alone.
    for name in AUTH_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_env_placeholder_secret_is_rejected(name, tmp_path, monkeypatch):
    text = EXAMPLES[name]()
    assert re.search(r"^AUTH_SECRET_KEY=", text, re.M), f"{name} must list AUTH_SECRET_KEY"
    settings = _settings_from(text, tmp_path)
    assert settings.auth_disabled is False
    monkeypatch.setattr(bootstrap, "settings", settings)
    with pytest.raises(RuntimeError, match="AUTH_SECRET_KEY"):
        bootstrap._validate_security_config()


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_env_starts_once_the_secret_is_filled_in(name, tmp_path, monkeypatch):
    text = re.sub(r"^AUTH_SECRET_KEY=.*$", f"AUTH_SECRET_KEY={secrets.token_hex(32)}", EXAMPLES[name](), flags=re.M)
    monkeypatch.setattr(bootstrap, "settings", _settings_from(text, tmp_path))
    bootstrap._validate_security_config()


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_env_offers_auth_disabled_for_local_use(name, tmp_path, monkeypatch):
    text = EXAMPLES[name]()
    assert re.search(r"^# ?AUTH_DISABLED=true", text, re.M), f"{name} must show the local opt-out"
    text = re.sub(r"^# ?AUTH_DISABLED=true.*$", "AUTH_DISABLED=true", text, flags=re.M)
    settings = _settings_from(text, tmp_path)
    assert settings.auth_disabled is True
    monkeypatch.setattr(bootstrap, "settings", settings)
    bootstrap._validate_security_config()


def test_docs_do_not_claim_auth_is_off_by_default():
    assert Settings.model_fields["auth_disabled"].default is False
    for path in ("README.md", ".env.example"):
        text = (ROOT / path).read_text(encoding="utf-8").lower()
        assert "off by default" not in text, path
