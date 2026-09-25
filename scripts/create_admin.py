"""Operator CLI: create an ADMIN_EMAILS account or set its password.

Admin rights need an ADMIN_EMAILS address AND a verified identity: a linked Google account,
or provisioning by this script (users.admin_provisioned_at, see src/auth/admin_identity.py).
The API refuses to register ADMIN_EMAILS addresses and never sets a first password from the
login form. This is the out-of-band path:

    python scripts/create_admin.py ops@example.com                  # prompts twice
    printf '%s\\n' "$PW" | python scripts/create_admin.py ops@example.com --password-stdin

The password is never accepted on the command line (it would end up in shell history
and `ps` output). Replacing an existing password bumps token_version, which revokes
every session minted before it. That is also how a self-registered account whose
address was added to ADMIN_EMAILS later becomes an admin: whoever registered it loses
access, and only the operator's new password works. Password-only admins from before
users.admin_provisioned_at existed run this once after upgrading.
"""
import argparse
import getpass
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
        # The in-memory store is gone when this process exits: refuse instead of
        # reporting an account that was never persisted.
        raise SystemExit("create_admin: TASK_STORE_BACKEND=memory persists nothing; point it at Postgres")
    return ResearchService(task_store=create_task_store())


def create_admin(email: str, password: str, service=None):
    """Create the admin account or replace its password. Returns (AuthUser, created)."""
    service = service if service is not None else _create_service()
    return service.provision_admin_account(email, password)


def _read_password(from_stdin: bool) -> str:
    if from_stdin:
        return sys.stdin.readline().rstrip("\r\n")
    password = getpass.getpass("Password: ")
    if getpass.getpass("Repeat password: ") != password:
        raise SystemExit("create_admin: passwords do not match")
    return password


def main(argv: list[str] | None = None, service=None) -> int:
    from src.domain.errors import ServiceError

    parser = argparse.ArgumentParser(description="Create an ADMIN_EMAILS account or set its password")
    parser.add_argument("email", help="account email; must be listed in ADMIN_EMAILS")
    parser.add_argument(
        "--password-stdin",
        action="store_true",
        help="read the password from the first line of stdin instead of prompting",
    )
    args = parser.parse_args(argv)
    if service is None:
        service = _create_service()  # refuse an unusable backend before prompting
    password = _read_password(args.password_stdin)
    try:
        user, created = create_admin(args.email, password, service=service)
    except ServiceError as exc:
        print(f"create_admin: {exc.detail}", file=sys.stderr)
        return 1
    action = "created admin account" if created else "password replaced, existing sessions revoked, for"
    print(f"create_admin: {action} {user.email} (id={user.id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
