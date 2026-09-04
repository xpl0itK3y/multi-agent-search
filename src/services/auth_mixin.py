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
        from src.auth.security import verify_password

        user = self.task_store.get_user_by_email(email.strip().lower())
        if user is None or user.password_hash is None or not verify_password(password, user.password_hash):
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
            return AuthUser(
                id=linked.id,
                email=linked.email,
                name=name or linked.name,
                avatar_url=avatar_url or linked.avatar_url,
                token_version=linked.token_version,
            ), False

        # Never silently attach a verified OAuth identity to an existing local account.
        # Account linking needs a separate flow that proves control of both identities.
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
        return AuthUser(
            id=user.id,
            email=user.email,
            name=name,
            avatar_url=avatar_url,
            token_version=user.token_version,
        ), True

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

    @staticmethod
    def _to_auth_user(user) -> AuthUser:
        return AuthUser(
            id=user.id,
            email=user.email,
            name=user.name,
            avatar_url=user.avatar_url,
            token_version=user.token_version,
        )
