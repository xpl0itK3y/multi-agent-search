"""Outgoing account email (AUTH-RECOVERY): one small sender interface, three backends.

EMAIL_BACKEND picks the backend:

- ``disabled`` (default): nothing is sent. The recovery endpoints still answer as usual,
  so their responses do not depend on the configuration.
- ``console``: the whole message, link included, goes to the application log at WARNING.
  For development only: anyone who reads the log can use the links.
- ``smtp``: stdlib smtplib with STARTTLS, implicit TLS (``ssl``) or no transport security
  (``none``), and a connect/IO timeout.

Callers send off the request path (FastAPI BackgroundTasks) and treat a failure as
"not sent": it is logged, never raised to the client. Messages carry one-time links, so
nothing here logs a message body or a link except the console backend, whose whole
purpose that is. A failure is logged by exception class and SMTP code only: the text of
an SMTP error can echo the recipient or the server's reply.
"""
from __future__ import annotations

import ipaddress
import logging
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid, parseaddr
from typing import Protocol
from urllib.parse import urlsplit

from src.config import Settings, settings as default_settings

logger = logging.getLogger(__name__)

EMAIL_BACKENDS = ("disabled", "console", "smtp")
SMTP_SECURITY_MODES = ("starttls", "ssl", "none")


@dataclass(frozen=True)
class OutgoingEmail:
    """A plain-text message to one recipient."""

    to: str
    subject: str
    body: str


class MailSender(Protocol):
    # False for the disabled backend: callers skip the work of preparing a message.
    enabled: bool

    def send(self, message: OutgoingEmail) -> None:
        """Deliver the message or raise; never retries on its own."""
        ...


def _check_address(address: str) -> str:
    """The recipient as given, refused when it cannot be one header value and one SMTP
    recipient (a line break or a second address would inject headers or recipients)."""
    if not address or any(ch in address for ch in "\r\n\t ,;<>") or parseaddr(address)[1] != address:
        raise ValueError("invalid recipient address")
    return address


class DisabledMailSender:
    enabled = False

    def send(self, message: OutgoingEmail) -> None:
        return None


class ConsoleMailSender:
    """Development backend: the message goes to the application log instead of a mailbox."""

    enabled = True

    def send(self, message: OutgoingEmail) -> None:
        _check_address(message.to)
        logger.warning(
            "console_email (EMAIL_BACKEND=console, development only) to=%s subject=%s\n%s",
            message.to,
            message.subject,
            message.body,
        )


class SmtpMailSender:
    enabled = True

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender: str,
        username: str = "",
        password: str = "",
        security: str = "starttls",
        timeout_seconds: float = 10.0,
        smtp_class=smtplib.SMTP,
        smtp_ssl_class=smtplib.SMTP_SSL,
    ) -> None:
        if security not in SMTP_SECURITY_MODES:
            raise ValueError(f"SMTP_SECURITY must be one of {', '.join(SMTP_SECURITY_MODES)}")
        self.host = host
        self.port = port
        self.sender = sender
        self.username = username
        self.password = password
        self.security = security
        self.timeout_seconds = timeout_seconds
        self._smtp_class = smtp_class
        self._smtp_ssl_class = smtp_ssl_class

    def build_message(self, message: OutgoingEmail) -> EmailMessage:
        email = EmailMessage()  # policy.default: a header value with a line break raises
        email["From"] = self.sender
        email["To"] = _check_address(message.to)
        email["Subject"] = message.subject
        email["Date"] = formatdate(usegmt=True)
        # The sender's domain: make_msgid() would otherwise ask DNS for this host's FQDN.
        domain = parseaddr(self.sender)[1].rpartition("@")[2] or "localhost"
        email["Message-ID"] = make_msgid(domain=domain)
        # RFC 3834: an automated message; well-behaved autoresponders do not answer it.
        email["Auto-Submitted"] = "auto-generated"
        email.set_content(message.body)
        return email

    def send(self, message: OutgoingEmail) -> None:
        email = self.build_message(message)
        if self.security == "ssl":
            client = self._smtp_ssl_class(
                self.host, self.port, timeout=self.timeout_seconds, context=ssl.create_default_context()
            )
        else:
            client = self._smtp_class(self.host, self.port, timeout=self.timeout_seconds)
        with client:
            if self.security == "starttls":
                client.starttls(context=ssl.create_default_context())
            if self.username:
                client.login(self.username, self.password)
            client.send_message(email)


def email_backend_name(config: Settings | None = None) -> str:
    config = config or default_settings
    return (config.email_backend or "disabled").strip().lower() or "disabled"


def create_mail_sender(config: Settings | None = None) -> MailSender:
    """The sender EMAIL_BACKEND names. Configuration errors are bootstrap's to report
    (src/bootstrap.py refuses them at startup); here an unknown backend sends nothing."""
    config = config or default_settings
    backend = email_backend_name(config)
    if backend == "console":
        return ConsoleMailSender()
    if backend == "smtp":
        return SmtpMailSender(
            host=config.smtp_host,
            port=config.smtp_port,
            sender=config.smtp_from,
            username=config.smtp_username,
            password=config.smtp_password,
            security=(config.smtp_security or "starttls").strip().lower(),
            timeout_seconds=config.smtp_timeout_seconds,
        )
    return DisabledMailSender()


def email_config_errors(config: Settings | None = None) -> list[str]:
    """What makes the EMAIL_* / SMTP_* configuration unusable; empty when it is fine."""
    config = config or default_settings
    backend = email_backend_name(config)
    if backend not in EMAIL_BACKENDS:
        return [f"EMAIL_BACKEND must be one of {', '.join(EMAIL_BACKENDS)} (got {backend!r})"]
    errors: list[str] = []
    if backend == "smtp":
        if not (config.smtp_host or "").strip():
            errors.append("EMAIL_BACKEND=smtp needs SMTP_HOST")
        if not (config.smtp_from or "").strip():
            errors.append("EMAIL_BACKEND=smtp needs SMTP_FROM")
        security = (config.smtp_security or "").strip().lower()
        if security not in SMTP_SECURITY_MODES:
            errors.append(f"SMTP_SECURITY must be one of {', '.join(SMTP_SECURITY_MODES)} (got {security!r})")
    if backend != "disabled" and not (config.public_app_url or "").strip().lower().startswith(
        ("http://", "https://")
    ):
        errors.append("PUBLIC_APP_URL must be an http(s) URL: account email links are built from it")
    return errors


def _is_loopback_host(host: str | None) -> bool:
    """localhost or a loopback address (127.0.0.0/8, ::1): traffic that never leaves the
    machine."""
    host = (host or "").strip().lower().strip("[]")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def email_config_warnings(config: Settings | None = None) -> list[str]:
    """What a usable EMAIL_* / SMTP_* configuration exposes on the network: the links in
    account email are working credentials (a reset link sets the password). Warnings
    only, since a trusted relay or a local-only deployment may be deliberate."""
    config = config or default_settings
    backend = email_backend_name(config)
    if backend not in ("console", "smtp"):
        return []
    warnings: list[str] = []
    security = (config.smtp_security or "").strip().lower()
    if backend == "smtp" and security == "none" and not _is_loopback_host(config.smtp_host):
        exposed = "account email, working password reset links included"
        if (config.smtp_username or "").strip():
            exposed += ", and the SMTP_USERNAME / SMTP_PASSWORD login"
        warnings.append(
            f"SMTP_SECURITY=none sends {exposed} to SMTP_HOST={config.smtp_host.strip()} unencrypted: "
            "use starttls or ssl unless that relay sits on a trusted private network"
        )
    url = (config.public_app_url or "").strip()
    if url.lower().startswith("http://") and not _is_loopback_host(urlsplit(url).hostname):
        warnings.append(
            f"PUBLIC_APP_URL={url} is plain http on a host other than localhost: the password reset "
            "and verification links in account email, and the pages that redeem them, travel "
            "unencrypted. Serve the app over HTTPS and set PUBLIC_APP_URL to its https:// address"
        )
    return warnings


def describe_send_failure(exc: BaseException) -> str:
    """A log-safe description of a failed send: the exception class and, for an SMTP
    reply, its code. Never the message text, which can echo addresses or server replies."""
    code = getattr(exc, "smtp_code", None)
    return f"{type(exc).__name__}" + (f" smtp_code={code}" if isinstance(code, int) else "")
