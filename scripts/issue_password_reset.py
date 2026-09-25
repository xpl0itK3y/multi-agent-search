"""Operator CLI: print a one-time password reset link for an account.

For out-of-band delivery when the API sends no email (EMAIL_BACKEND=disabled), or when a
user cannot receive it:

    python scripts/issue_password_reset.py user@example.com

The link is the one a forgot-password email carries, {PUBLIC_APP_URL}/reset-password#token=...:
it works once, for PASSWORD_RESET_TTL_SECONDS, and only while the account keeps that
email; issuing it invalidates the account's earlier reset links. Opening it sets a new
password, marks the email verified and signs the account out everywhere. Hand it only to
the owner of the address, over a channel you trust: whoever holds it can take the account.

The link is printed to stdout only; nothing secret is logged. An unknown email exits 1
and stores nothing.
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _create_service():
    from src.config import settings
    from src.repositories import create_task_store
    from src.services import ResearchService

    if settings.task_store_backend.lower() == "memory":
        # The in-memory store is gone when this process exits: the link would never work.
        raise SystemExit("issue_password_reset: TASK_STORE_BACKEND=memory persists nothing; point it at Postgres")
    return ResearchService(task_store=create_task_store())


def issue_password_reset(email: str, service=None):
    """(AuthUser, link) for the account with this email, or None when there is none."""
    service = service if service is not None else _create_service()
    return service.issue_password_reset_link(email)


def main(argv: list[str] | None = None, service=None) -> int:
    from src.config import settings

    parser = argparse.ArgumentParser(description="Print a one-time password reset link for an account")
    parser.add_argument("email", help="the account's email")
    args = parser.parse_args(argv)
    if service is None:
        service = _create_service()
    issued = issue_password_reset(args.email, service=service)
    if issued is None:
        print(f"issue_password_reset: no account has the email {args.email.strip().lower()}", file=sys.stderr)
        return 1
    user, link = issued
    minutes = max(settings.password_reset_ttl_seconds // 60, 1)
    print(
        f"issue_password_reset: one-time link for {user.email} (id={user.id}), valid for {minutes} min; "
        "earlier reset links no longer work:",
        file=sys.stderr,
    )
    print(link)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
