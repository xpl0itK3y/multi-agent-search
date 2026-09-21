"""Authentication concern of ResearchService, extracted as a mixin (AUD-030).

Composed into ResearchService; relies on self.task_store (set in ResearchService.__init__).
"""
import uuid

from src.domain.errors import BadRequestError, ConflictError, UnauthorizedError, UnprocessableError

from src.domain import AuthUser


class AuthMixin:
    def register_user(self, email: str, password: str) -> AuthUser:
        from src.auth.security import hash_password

        normalized = email.strip().lower()
        if "@" not in normalized or "." not in normalized.split("@")[-1]:
            raise UnprocessableError("Invalid email address")
        if self.task_store.get_user_by_email(normalized) is not None:
            raise ConflictError("Email already registered")
        user = self.task_store.create_user(str(uuid.uuid4()), normalized, hash_password(password))
        return self._to_auth_user(user)

    def authenticate_user(self, email: str, password: str) -> AuthUser:
        from src.auth.security import hash_password, verify_password
        from src.config import settings

        normalized = email.strip().lower()
        user = self.task_store.get_user_by_email(normalized)
        if user is None:
            raise UnauthorizedError("Invalid email or password")

        allowed = {e.strip().lower() for e in settings.admin_emails.split(",") if e.strip()}
        if user.password_hash is None:
            if normalized in allowed and len(password or "") >= 6:
                updated = self.task_store.update_user_password(user.id, hash_password(password))
                if updated:
                    return self._to_auth_user(updated)
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

        # Never silently attach a verified OAuth identity to an existing local account,
        # unless it is a designated admin account being linked.
        existing = self.task_store.get_user_by_email(normalized)
        if existing is not None:
            from src.config import settings
            allowed = {e.strip().lower() for e in settings.admin_emails.split(",") if e.strip()}
            if normalized in allowed:
                self.task_store.update_user_profile(existing.id, name, avatar_url)
                return self._to_auth_user(existing), False
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
    ) -> AuthUser:
        """Set/replace a user's password (e.g. after first Google sign-in)."""
        from src.auth.security import hash_password, verify_password

        if len(password or "") < 6:
            raise UnprocessableError("Password must be at least 6 characters")
        user = self.task_store.get_user_by_id(user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if user.password_hash is not None:
            if not current_password:
                raise BadRequestError("Current password is required")
            if not verify_password(current_password, user.password_hash):
                raise UnauthorizedError("Current password is incorrect")
        updated = self.task_store.update_user_password(user_id, hash_password(password))
        if updated is None:
            raise UnauthorizedError("User not found")
        return self._to_auth_user(updated)

    def get_auth_user(self, user_id: str) -> AuthUser | None:
        user = self.task_store.get_user_by_id(user_id)
        return self._to_auth_user(user) if user else None

    def delete_user_account(
        self,
        user_id: str,
        *,
        current_password: str | None = None,
        confirm: bool = False,
    ) -> None:
        """Delete the account and everything it owns (DATA-LIFECYCLE).

        The DB cascade removes researches, tasks, results and jobs — which also revokes
        every public share token the user had minted. Password (when the account has
        one) plus an explicit confirm flag guard against accidents and CSRF-style abuse.
        """
        from src.auth.security import verify_password

        if not confirm:
            raise BadRequestError("Pass confirm=true to delete the account")
        user = self.task_store.get_user_by_id(user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if user.password_hash is not None:
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
        from src.config import settings

        allowed = {e.strip().lower() for e in settings.admin_emails.split(",") if e.strip()}
        is_admin = bool(user.email and user.email.lower() in allowed)
        return AuthUser(
            id=user.id,
            email=user.email,
            name=user.name,
            avatar_url=user.avatar_url,
            is_admin=is_admin,
            token_version=user.token_version,
        )
