"""Authentication concern of ResearchService, extracted as a mixin (AUD-030).

Composed into ResearchService; relies on self.task_store (set in ResearchService.__init__).
"""
import secrets
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
        from src.auth.security import verify_password

        user = self.task_store.get_user_by_email(email.strip().lower())
        if user is None or not verify_password(password, user.password_hash):
            raise UnauthorizedError("Invalid email or password")
        return self._to_auth_user(user)

    def get_or_create_oauth_user(
        self, email: str, name: str | None = None, avatar_url: str | None = None
    ) -> tuple[AuthUser, bool]:
        """Resolve (or create) an account for a verified OAuth identity.

        Stores/refreshes the provider's name + avatar. Returns (user, created):
        ``created`` is True for a brand-new account, so the caller can offer to set a
        password for future email/password login.
        """
        from src.auth.security import hash_password

        normalized = (email or "").strip().lower()
        if "@" not in normalized:
            raise BadRequestError("OAuth provider returned no email")
        existing = self.task_store.get_user_by_email(normalized)
        if existing is not None:
            self.task_store.update_user_profile(existing.id, name, avatar_url)  # keep fresh
            return AuthUser(
                id=existing.id, email=existing.email,
                name=name or existing.name, avatar_url=avatar_url or existing.avatar_url,
            ), False
        # New account: start passwordless (a random unusable hash) until the user sets one.
        user = self.task_store.create_user(
            str(uuid.uuid4()), normalized, hash_password(secrets.token_urlsafe(32))
        )
        self.task_store.update_user_profile(user.id, name, avatar_url)
        return AuthUser(id=user.id, email=user.email, name=name, avatar_url=avatar_url), True

    def set_user_password(self, user_id: str, password: str) -> None:
        """Set/replace a user's password (e.g. after first Google sign-in)."""
        from src.auth.security import hash_password

        if len(password or "") < 6:
            raise UnprocessableError("Password must be at least 6 characters")
        self.task_store.update_user_password(user_id, hash_password(password))

    def get_auth_user(self, user_id: str) -> AuthUser | None:
        user = self.task_store.get_user_by_id(user_id)
        return self._to_auth_user(user) if user else None

    @staticmethod
    def _to_auth_user(user) -> AuthUser:
        return AuthUser(id=user.id, email=user.email, name=user.name, avatar_url=user.avatar_url)
