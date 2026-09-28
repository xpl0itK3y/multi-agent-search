"""Account email: the sender backends (mail.py) and the localized messages (messages.py)."""
from src.notifications.mail import (
    EMAIL_BACKENDS,
    SMTP_SECURITY_MODES,
    ConsoleMailSender,
    DisabledMailSender,
    MailSender,
    OutgoingEmail,
    SmtpMailSender,
    create_mail_sender,
    describe_send_failure,
    email_backend_name,
    email_config_errors,
    email_config_warnings,
    mailbox_key,
)
from src.notifications.messages import (
    SUPPORTED_LANGUAGES,
    AccountEmail,
    preferred_language,
    render_account_email,
)

__all__ = [
    "EMAIL_BACKENDS",
    "SMTP_SECURITY_MODES",
    "SUPPORTED_LANGUAGES",
    "AccountEmail",
    "ConsoleMailSender",
    "DisabledMailSender",
    "MailSender",
    "OutgoingEmail",
    "SmtpMailSender",
    "create_mail_sender",
    "describe_send_failure",
    "email_backend_name",
    "email_config_errors",
    "email_config_warnings",
    "mailbox_key",
    "preferred_language",
    "render_account_email",
]
