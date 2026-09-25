"""Who holds admin rights (SEC-ADMIN-IDENTITY): the single rule every check uses.

ADMIN_EMAILS names the admin addresses, but sign-up verifies no email. Membership alone
would therefore make whoever registered an address first its admin, including a local
account created before the operator added the address to ADMIN_EMAILS. An account holds
admin rights only when its email is listed AND the address is verified:

- it is linked to a Google identity (``google_subject`` is set: Google verified the email
  when the account was created), or
- the operator provisioned it with scripts/create_admin.py (``admin_provisioned_at`` is
  set), which also replaced its password and revoked every earlier session.
"""
from __future__ import annotations

from datetime import datetime

from src.config import settings


def admin_emails() -> set[str]:
    """ADMIN_EMAILS, lower-cased (a comma-separated string or a list)."""
    raw = getattr(settings, "admin_emails", "")
    if not raw:
        return set()
    if isinstance(raw, str):
        return {e.strip().lower() for e in raw.split(",") if e.strip()}
    return {str(e).strip().lower() for e in raw if str(e).strip()}


def is_admin_email(email: str | None) -> bool:
    """Whether the address is listed in ADMIN_EMAILS (case-insensitive). Not by itself an
    admin check: use has_admin_rights."""
    return bool(email) and email.strip().lower() in admin_emails()


def has_admin_rights(
    email: str | None,
    google_subject: str | None,
    admin_provisioned_at: datetime | None,
) -> bool:
    """ADMIN_EMAILS membership plus a verified identity (see the module docstring)."""
    return is_admin_email(email) and (bool(google_subject) or admin_provisioned_at is not None)
