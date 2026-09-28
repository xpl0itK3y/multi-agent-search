"""AUTH-RECOVERY: the account email module (src/notifications) and its startup checks."""
import logging

import pytest

from src.bootstrap import _validate_email_config
from src.config import Settings, settings
from src.notifications import (
    AccountEmail,
    ConsoleMailSender,
    DisabledMailSender,
    OutgoingEmail,
    SmtpMailSender,
    create_mail_sender,
    describe_send_failure,
    email_config_errors,
    email_config_warnings,
    preferred_language,
    render_account_email,
)

MESSAGE = OutgoingEmail(to="owner@example.com", subject="Subject", body="Open https://app/reset-password#token=abc")


class FakeSMTP:
    """Records the smtplib calls a sender makes."""

    instances: list["FakeSMTP"] = []

    def __init__(self, host, port, timeout=None, context=None):
        self.calls = [("connect", host, port, timeout, context is not None)]
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.calls.append(("quit",))
        return False

    def starttls(self, context=None):
        self.calls.append(("starttls", context is not None))

    def login(self, username, password):
        self.calls.append(("login", username, password))

    def send_message(self, message):
        self.calls.append(("send", message))


@pytest.fixture(autouse=True)
def _fresh_fake_smtp():
    FakeSMTP.instances = []


def _sender(**overrides) -> SmtpMailSender:
    options = dict(
        host="smtp.example.com",
        port=587,
        sender="Veris <no-reply@veris.example>",
        username="mailer",
        password="smtp-secret",
        security="starttls",
        timeout_seconds=7.5,
        smtp_class=FakeSMTP,
        smtp_ssl_class=FakeSMTP,
    )
    options.update(overrides)
    return SmtpMailSender(**options)


def test_smtp_starttls_logs_in_sends_and_quits():
    _sender().send(MESSAGE)

    (client,) = FakeSMTP.instances
    assert [call[0] for call in client.calls] == ["connect", "starttls", "login", "send", "quit"]
    assert client.calls[0] == ("connect", "smtp.example.com", 587, 7.5, False)
    assert client.calls[1] == ("starttls", True)  # a verifying TLS context
    assert client.calls[2] == ("login", "mailer", "smtp-secret")
    sent = client.calls[3][1]
    assert (sent["From"], sent["To"], sent["Subject"]) == ("Veris <no-reply@veris.example>", MESSAGE.to, "Subject")
    assert sent["Auto-Submitted"] == "auto-generated"
    assert sent["Message-ID"].endswith("@veris.example>")
    assert sent["Date"]
    assert sent.get_content().strip() == MESSAGE.body


def test_smtp_ssl_uses_implicit_tls_and_none_skips_tls_and_login_without_a_user():
    ssl_classes = []

    class FakeSSL(FakeSMTP):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            ssl_classes.append(self)

    _sender(security="ssl", port=465, smtp_ssl_class=FakeSSL).send(MESSAGE)
    assert ssl_classes and ssl_classes[0].calls[0] == ("connect", "smtp.example.com", 465, 7.5, True)
    assert "starttls" not in [call[0] for call in ssl_classes[0].calls]

    FakeSMTP.instances = []
    _sender(security="none", username="").send(MESSAGE)
    assert [call[0] for call in FakeSMTP.instances[0].calls] == ["connect", "send", "quit"]


@pytest.mark.parametrize(
    "recipient",
    ["owner@example.com\nBcc: everyone@example.com", "a@example.com, b@example.com", "Owner <o@example.com>", ""],
)
def test_a_recipient_that_would_inject_headers_or_recipients_is_refused(recipient):
    with pytest.raises(ValueError):
        _sender().send(OutgoingEmail(to=recipient, subject="s", body="b"))
    with pytest.raises(ValueError):
        ConsoleMailSender().send(OutgoingEmail(to=recipient, subject="s", body="b"))
    assert all(call[0] != "send" for client in FakeSMTP.instances for call in client.calls)


def test_the_console_backend_logs_the_whole_message_at_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="src.notifications.mail"):
        ConsoleMailSender().send(MESSAGE)
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert "#token=abc" in record.getMessage() and "development only" in record.getMessage()


def test_the_backend_follows_email_backend():
    def sender_for(**values):
        return create_mail_sender(Settings(_env_file=None, **values))

    assert isinstance(sender_for(), DisabledMailSender) and sender_for().enabled is False
    assert isinstance(sender_for(email_backend=" Console "), ConsoleMailSender)
    smtp = sender_for(
        email_backend="smtp",
        smtp_host="mail.example.com",
        smtp_from="no-reply@example.com",
        smtp_port=2525,
        smtp_security="SSL",
        smtp_timeout_seconds=3,
    )
    assert isinstance(smtp, SmtpMailSender) and smtp.enabled is True
    assert (smtp.host, smtp.port, smtp.security, smtp.timeout_seconds) == ("mail.example.com", 2525, "ssl", 3)
    defaults = Settings(_env_file=None)
    assert (defaults.smtp_port, defaults.smtp_security, defaults.smtp_timeout_seconds) == (587, "starttls", 10)
    assert (defaults.password_reset_ttl_seconds, defaults.email_verification_ttl_seconds) == (3600, 86400)
    assert defaults.public_app_url == "http://localhost:8502"
    assert defaults.email_delivery_enabled is False


def test_a_send_failure_is_described_without_its_text():
    import smtplib

    refused = smtplib.SMTPRecipientsRefused({"owner@example.com": (550, b"secret reply")})
    auth = smtplib.SMTPAuthenticationError(535, b"password smtp-secret rejected")
    assert describe_send_failure(refused) == "SMTPRecipientsRefused"
    assert describe_send_failure(auth) == "SMTPAuthenticationError smtp_code=535"
    assert describe_send_failure(TimeoutError("timed out")) == "TimeoutError"


# ── configuration checks ──────────────────────────────────────────────────────


def _errors(**values) -> list[str]:
    return email_config_errors(Settings(_env_file=None, **values))


def test_email_configuration_errors():
    assert _errors() == []
    assert _errors(email_backend="console") == []
    assert _errors(email_backend="smtp", smtp_host="h", smtp_from="f@example.com") == []
    assert _errors(email_backend="smtp") == ["EMAIL_BACKEND=smtp needs SMTP_HOST", "EMAIL_BACKEND=smtp needs SMTP_FROM"]
    assert "SMTP_SECURITY" in _errors(email_backend="smtp", smtp_host="h", smtp_from="f", smtp_security="tls")[0]
    assert "EMAIL_BACKEND must be one of" in _errors(email_backend="sendgrid")[0]
    assert "PUBLIC_APP_URL" in _errors(email_backend="console", public_app_url="veris.example")[0]
    assert _errors(public_app_url="veris.example") == []  # no links go out while disabled


def test_bootstrap_refuses_smtp_without_host_and_from(monkeypatch):
    monkeypatch.setattr(settings, "email_backend", "smtp", raising=False)
    monkeypatch.setattr(settings, "smtp_host", "", raising=False)
    monkeypatch.setattr(settings, "smtp_from", "", raising=False)
    with pytest.raises(RuntimeError, match="SMTP_HOST.*SMTP_FROM"):
        _validate_email_config()
    monkeypatch.setattr(settings, "smtp_host", "mail.example.com", raising=False)
    monkeypatch.setattr(settings, "smtp_from", "no-reply@example.com", raising=False)
    _validate_email_config()


def test_bootstrap_warns_that_the_console_backend_is_for_development(monkeypatch, caplog):
    monkeypatch.setattr(settings, "email_backend", "console", raising=False)
    with caplog.at_level(logging.WARNING, logger="src.bootstrap"):
        _validate_email_config()
    assert any("development only" in record.getMessage() for record in caplog.records)


def _warnings(**values) -> list[str]:
    return email_config_warnings(Settings(_env_file=None, **values))


SMTP = {"email_backend": "smtp", "smtp_host": "mail.example.com", "smtp_from": "no-reply@example.com"}


def test_cleartext_smtp_to_a_relay_off_this_machine_is_warned_about():
    """SEC-REC-5: SMTP_SECURITY=none puts working reset links (and the SMTP login) on the
    network unencrypted; a loopback relay keeps them on the machine."""
    assert _warnings(**SMTP) == []
    assert _warnings(**SMTP, smtp_security="ssl") == []
    [warning] = _warnings(**SMTP, smtp_security="none")
    assert warning.startswith("SMTP_SECURITY=none sends account email")
    assert "SMTP_HOST=mail.example.com" in warning and "SMTP_USERNAME" not in warning
    [with_login] = _warnings(**SMTP, smtp_security="None", smtp_username="mailer", smtp_password="secret")
    assert "SMTP_USERNAME / SMTP_PASSWORD login" in with_login and "secret" not in with_login
    for local in ("localhost", "127.0.0.1", "127.0.0.2", "::1", "[::1]", " LOCALHOST "):
        assert _warnings(**{**SMTP, "smtp_host": local}, smtp_security="none") == [], local
    assert _warnings(**{**SMTP, "smtp_host": "127.example.com"}, smtp_security="none")


def test_a_plain_http_public_app_url_off_localhost_is_warned_about():
    for url in ("http://localhost:8502", "http://127.0.0.1", "http://[::1]:8080/", "https://veris.example"):
        assert _warnings(email_backend="console", public_app_url=url) == [], url
    [warning] = _warnings(email_backend="console", public_app_url="http://veris.example")
    assert warning.startswith("PUBLIC_APP_URL=http://veris.example is plain http")
    assert _warnings(**SMTP, public_app_url="HTTP://10.0.0.5:8502")
    assert _warnings(**SMTP, public_app_url="http://localhost.veris.example")
    # No links go out while email is disabled.
    assert _warnings(public_app_url="http://veris.example", smtp_security="none") == []


def test_bootstrap_starts_with_a_warning_for_cleartext_links(monkeypatch, caplog):
    for name, value in {**SMTP, "smtp_security": "none", "public_app_url": "http://veris.example"}.items():
        monkeypatch.setattr(settings, name, value, raising=False)

    with caplog.at_level(logging.WARNING, logger="src.bootstrap"):
        _validate_email_config()  # a warning, not an error

    messages = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert [message.split("=", 1)[0] for message in messages] == ["SMTP_SECURITY", "PUBLIC_APP_URL"]


@pytest.mark.anyio
async def test_the_api_refuses_to_start_with_an_unusable_smtp_configuration(monkeypatch):
    from src.api.app import create_app

    monkeypatch.setattr(settings, "email_backend", "smtp", raising=False)
    monkeypatch.setattr(settings, "smtp_host", "", raising=False)
    app = create_app()
    with pytest.raises(RuntimeError, match="SMTP_HOST"):
        async with app.router.lifespan_context(app):
            pass


# ── messages ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("header", "language"),
    [
        (None, "en"),
        ("", "en"),
        ("ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "ru"),
        ("de-DE,de;q=0.9,es;q=0.8,en;q=0.5", "es"),
        ("en;q=0.2, ru;q=0.8", "ru"),
        ("fr, de", "en"),
        ("ES", "es"),
        ("ru;q=0, es;q=0.1", "es"),
        ("ru;q=abc, en", "en"),
        ("*", "en"),
    ],
)
def test_the_language_comes_from_accept_language(header, language):
    assert preferred_language(header) == language


@pytest.mark.parametrize("kind", list(AccountEmail))
@pytest.mark.parametrize("language", ["en", "ru", "es"])
def test_every_message_renders_in_every_language(kind, language):
    link = "https://veris.example/reset-password#token=tok" if kind in (
        AccountEmail.PASSWORD_RESET,
        AccountEmail.EMAIL_VERIFICATION,
    ) else ""
    message = render_account_email(
        kind, language, to="owner@example.com", app_url="https://veris.example/", link=link, valid_for_seconds=3600
    )

    assert message.to == "owner@example.com"
    assert message.subject and "\n" not in message.subject
    assert "owner@example.com" in message.body and "Veris" in message.body
    assert "{" not in message.body and "}" not in message.body  # every placeholder filled
    assert "//forgot" not in message.body and "//settings" not in message.body
    assert not link or link in message.body
    assert ("#token=" in message.body) is bool(link)


def test_link_lifetimes_read_naturally():
    def body(language, seconds):
        return render_account_email(
            AccountEmail.EMAIL_VERIFICATION, language, to="o@example.com", app_url="https://a", link="L",
            valid_for_seconds=seconds,
        ).body

    assert "24 hours" in body("en", 86400) and "60 minutes" in body("en", 3600) and "1 minute" in body("en", 60)
    assert "24 часа" in body("ru", 86400) and "60 минут" in body("ru", 3600) and "21 минуту" in body("ru", 1260)
    assert "5 часов" in body("ru", 18000) and "24 horas" in body("es", 86400) and "1 minuto" in body("es", 60)
    assert render_account_email(AccountEmail.PASSWORD_RESET, "de", to="o@e.com", app_url="https://a").subject == (
        "Reset your Veris password"
    )
