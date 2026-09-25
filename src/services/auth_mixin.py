"""Authentication concern of ResearchService, extracted as a mixin (AUD-030).

Composed into ResearchService; relies on self.task_store (set in ResearchService.__init__).
"""
import uuid

from src.domain.errors import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    UnauthorizedError,
    UnprocessableError,
)

from src.auth.admin_identity import admin_emails, has_admin_rights
from src.domain import AuthUser

# Detail prefix of the 403 that asks for a fresh Google sign-in. The web UI keys on it
# (isReauthRequired: a 403 whose detail starts with it) to offer signing in again.
REAUTH_REQUIRED = "reauth_required"


def _reauth_required(action: str) -> str:
    from src.auth.security import FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS

    minutes = FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS // 60
    return f"{REAUTH_REQUIRED}: {action} needs a Google sign-in from the last {minutes} minutes"


class AuthMixin:
    def register_user(self, email: str, password: str) -> AuthUser:
        from src.auth.security import hash_password

        normalized = email.strip().lower()
        if "@" not in normalized or "." not in normalized.split("@")[-1]:
            raise UnprocessableError("Invalid email address")
        # Admin rights follow the email and sign-up verifies nothing, so a local account for
        # an ADMIN_EMAILS address would make whoever registers it first an admin.
        if normalized in admin_emails():
            raise ForbiddenError(
                "This email is reserved for an administrator: sign in with Google, or ask the "
                "operator to provision it with scripts/create_admin.py"
            )
        if self.task_store.get_user_by_email(normalized) is not None:
            raise ConflictError("Email already registered")
        user = self.task_store.create_user(str(uuid.uuid4()), normalized, hash_password(password))
        return self._to_auth_user(user)

    def authenticate_user(self, email: str, password: str) -> AuthUser:
        from src.auth.security import verify_password

        normalized = email.strip().lower()
        user = self.task_store.get_user_by_email(normalized)
        if user is None:
            raise UnauthorizedError("Invalid email or password")
        # A passwordless (Google-created) account never takes a password from the login form,
        # admin or not: the first one is set from an authenticated session (set-password) or
        # by the operator (scripts/create_admin.py). Accepting it here let anyone claim it.
        if user.password_hash is None:
            raise UnauthorizedError("This account was registered via Google Sign-In. Please sign in with Google.")

        if not verify_password(password, user.password_hash):
            raise UnauthorizedError("Invalid email or password")
        return self._to_auth_user(user)

    def get_or_create_oauth_user(
        self,
        email: str,
        google_subject: str,
        name: str | None = None,
        avatar_url: str | None = None,
    ) -> tuple[AuthUser, bool]:
        """Resolve (or create) an account for a verified OAuth identity.

        Stores/refreshes the provider's name + avatar. Returns (user, created):
        ``created`` is True for a brand-new account, so the caller can offer to set a
        password for future email/password login.
        """
        normalized = (email or "").strip().lower()
        subject = (google_subject or "").strip()
        if "@" not in normalized or not subject:
            raise BadRequestError("OAuth provider returned an incomplete identity")

        linked = self.task_store.get_user_by_google_subject(subject)
        if linked is not None:
            self.task_store.update_user_profile(linked.id, name, avatar_url)  # keep fresh
            return self._to_auth_user(linked), False

        # Never silently attach a verified OAuth identity to an existing local account — admin
        # emails included: sign-up verifies no email, so that row may belong to whoever
        # claimed the address first.
        if self.task_store.get_user_by_email(normalized) is not None:
            raise ConflictError("An account with this email already exists")

        # New OAuth accounts are explicitly passwordless until the user sets one.
        user = self.task_store.create_user(
            str(uuid.uuid4()),
            normalized,
            None,
            google_subject=subject,
        )
        self.task_store.update_user_profile(user.id, name, avatar_url)
        return self._to_auth_user(user), True

    def set_user_password(
        self,
        user_id: str,
        password: str,
        current_password: str | None = None,
        *,
        fresh_google_auth: bool = False,
    ) -> AuthUser:
        """Set/replace a user's password (e.g. after first Google sign-in).

        A password that exists is replaced with the current one, as before. Two changes
        need ``fresh_google_auth`` (a session from a Google sign-in of the last few minutes,
        see security.is_fresh_google_auth) instead, else ForbiddenError(reauth_required):
        the first password of a passwordless account, and a reset without the current
        password on a Google-linked account (recovery). A stolen session alone must not add
        a password login that the owner then cannot rotate or remove (SEC2-3)."""
        from src.auth.security import hash_password, verify_password

        if len(password or "") < 6:
            raise UnprocessableError("Password must be at least 6 characters")
        user = self.task_store.get_user_by_id(user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if user.password_hash is None:
            if not fresh_google_auth:
                raise ForbiddenError(_reauth_required("setting a first password"))
        elif current_password:
            if not verify_password(current_password, user.password_hash):
                raise UnauthorizedError("Current password is incorrect")
        elif user.google_subject:
            if not fresh_google_auth:
                raise ForbiddenError(_reauth_required("resetting the password without the current one"))
        else:
            raise BadRequestError("Current password is required")
        updated = self.task_store.update_user_password(user_id, hash_password(password))
        if updated is None:
            raise UnauthorizedError("User not found")
        return self._to_auth_user(updated)

    def revoke_user_sessions(self, user_id: str) -> bool:
        """Revoke every session token of the account (sign out on all devices) by bumping
        its token_version. False when the account does not exist.

        The store has no token_version-only write, so this writes the stored password hash
        back unchanged (None for a passwordless account) through update_user_password,
        which bumps token_version in the same statement. A password change committed
        between the read and this write would be overwritten by the old hash (its sessions
        end either way); an atomic token_version-only store write would close that gap."""
        user = self.task_store.get_user_by_id(user_id)
        if user is None:
            return False
        return self.task_store.update_user_password(user_id, user.password_hash) is not None

    def provision_admin_account(self, email: str, password: str) -> tuple[AuthUser, bool]:
        """Operator path (scripts/create_admin.py): create an ADMIN_EMAILS account or replace
        its password. Returns (user, created).

        Every password write bumps token_version, so replacing one revokes all sessions
        minted before it — including any held by whoever registered the address first.
        The same write stamps admin_provisioned_at: the operator's vouching is what makes a
        password-only ADMIN_EMAILS account an admin (src/auth/admin_identity.py).
        """
        from src.auth.security import hash_password

        normalized = (email or "").strip().lower()
        if normalized not in admin_emails():
            raise ForbiddenError(f"{normalized or 'email'} is not listed in ADMIN_EMAILS")
        if len(password or "") < 6:
            raise UnprocessableError("Password must be at least 6 characters")
        existing = self.task_store.get_user_by_email(normalized)
        if existing is None:
            user = self.task_store.create_user(
                str(uuid.uuid4()), normalized, hash_password(password), admin_provisioned=True
            )
            return self._to_auth_user(user), True
        updated = self.task_store.update_user_password(
            existing.id, hash_password(password), admin_provisioned=True
        )
        if updated is None:
            raise NotFoundError("User not found")
        return self._to_auth_user(updated), False

    def get_auth_user(self, user_id: str) -> AuthUser | None:
        user = self.task_store.get_user_by_id(user_id)
        return self._to_auth_user(user) if user else None

    def delete_user_account(
        self,
        user_id: str,
        *,
        current_password: str | None = None,
        confirm: bool = False,
        fresh_google_auth: bool = False,
    ) -> None:
        """Delete the account and everything it owns (DATA-LIFECYCLE).

        The DB cascade removes researches, tasks, results and jobs — which also revokes
        every public share token the user had minted. The current password (when the
        account has one) plus an explicit confirm flag guard against accidents and
        CSRF-style abuse. A passwordless account needs ``fresh_google_auth`` instead (a
        Google sign-in of the last few minutes), else ForbiddenError(reauth_required).
        """
        from src.auth.security import verify_password

        if not confirm:
            raise BadRequestError("Pass confirm=true to delete the account")
        user = self.task_store.get_user_by_id(user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if user.password_hash is None:
            if not fresh_google_auth:
                raise ForbiddenError(_reauth_required("deleting the account"))
        else:
            if not current_password:
                raise BadRequestError("Current password is required")
            if not verify_password(current_password, user.password_hash):
                raise UnauthorizedError("Current password is incorrect")
        if not self.task_store.delete_user(user_id):
            raise UnauthorizedError("User not found")

    def update_profile(self, user_id: str, name: str | None = None, avatar_url: str | None = None) -> AuthUser:
        user = self.task_store.update_user_profile(user_id, name, avatar_url)
        if not user:
            raise UnauthorizedError("User not found")
        return self._to_auth_user(user)

    @staticmethod
    def _to_auth_user(user) -> AuthUser:
        # The one place an AuthUser's admin flag is computed: the guards only read it.
        return AuthUser(
            id=user.id,
            email=user.email,
            name=user.name,
            avatar_url=user.avatar_url,
            is_admin=has_admin_rights(user.email, user.google_subject, user.admin_provisioned_at),
            token_version=user.token_version,
        )
