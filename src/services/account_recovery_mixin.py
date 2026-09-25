"""Account recovery and email verification (AUTH-RECOVERY), extracted as a mixin.

Composed into ResearchService; relies on self.task_store and self.mail_sender (set in
ResearchService.__init__) and on AuthMixin._to_auth_user.

One-time links. A link carries secrets.token_urlsafe(32) (256 random bits) in its URL
fragment, {PUBLIC_APP_URL}/reset-password#token=... or /verify-email#token=..., so the
token never reaches a server log or a Referer header. Only the token's sha256 hex is
stored and a redeem finds it through the unique index on that hash: a copy of the table
holds nothing that redeems. A token works once, until it expires, and only while the
account's email is still the address it was sent to; issuing one invalidates the
account's earlier unused ones of the same purpose.

Email goes out off the request path: the routes hand the methods documented as
"background" to FastAPI BackgroundTasks. Those never raise: a failure is logged by kind,
account id and exception class (never a token, a link or the error text) and changes no
response. POST /v1/auth/password/forgot does all of its work there, so its response is
the same, and as quick, for a known and an unknown address.
"""
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Callable

from src.config import settings
from src.domain import AuthActionPurpose, AuthUser, UserRecord
from src.domain.errors import BadRequestError, UnprocessableError
from src.notifications import (
    AccountEmail,
    MailSender,
    create_mail_sender,
    describe_send_failure,
    render_account_email,
)

logger = logging.getLogger(__name__)

# The 400 details of a link that does not redeem (unknown, used, expired, or sent to an
# address the account no longer has). The web UI keys on the prefix before the colon.
RESET_TOKEN_INVALID_DETAIL = "reset_token_invalid: this password reset link is invalid or has expired"
VERIFICATION_TOKEN_INVALID_DETAIL = "verification_token_invalid: this verification link is invalid or has expired"

# The SPA routes the links open.
PASSWORD_RESET_PATH = "/reset-password"
EMAIL_VERIFICATION_PATH = "/verify-email"

# The notices send_account_notice sends: none of them carries a link to redeem.
ACCOUNT_NOTICES = frozenset(
    {
        AccountEmail.PASSWORD_RESET_DONE,
        AccountEmail.GOOGLE_LINKED,
        AccountEmail.GOOGLE_LINKED_PASSWORD_REMOVED,
        AccountEmail.PASSWORD_CHANGED,
    }
)


def hash_link_token(token: str) -> str:
    """What auth_action_tokens stores for a link token: its sha256, hex."""
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


def build_link(path: str, token: str) -> str:
    """{PUBLIC_APP_URL}{path}#token=<token>. token_urlsafe output needs no escaping."""
    return f"{settings.public_app_url.strip().rstrip('/')}{path}#token={token}"


class AccountRecoveryMixin:
    # ResearchService sets it; None means "the backend EMAIL_BACKEND names, right now".
    mail_sender: MailSender | None = None

    def _mail(self) -> MailSender:
        return self.mail_sender if self.mail_sender is not None else create_mail_sender()

    def email_delivery_enabled(self) -> bool:
        """Whether account email goes anywhere (EMAIL_BACKEND is not "disabled")."""
        return self._mail().enabled

    # ── issuing links ─────────────────────────────────────────────────────────

    def _issue_link(
        self,
        user: UserRecord,
        purpose: AuthActionPurpose,
        path: str,
        ttl_seconds: int,
        requested_ip: str | None = None,
    ) -> str | None:
        """A fresh one-time link for the account (its earlier ones of this purpose stop
        working), or None when the account is gone."""
        token = secrets.token_urlsafe(32)
        record = self.task_store.create_auth_action_token(
            user.id,
            purpose,
            hash_link_token(token),
            user.email,
            datetime.now(timezone.utc) + timedelta(seconds=max(int(ttl_seconds), 60)),
            requested_ip=requested_ip,
        )
        return build_link(path, token) if record is not None else None

    def issue_password_reset_link(self, email: str) -> tuple[AuthUser, str] | None:
        """Operator path (scripts/issue_password_reset.py): a one-time reset link for the
        account with this email, whatever EMAIL_BACKEND says, for out-of-band delivery.
        None, and nothing stored, when no account has the address."""
        user = self.task_store.get_user_by_email((email or "").strip().lower())
        if user is None:
            return None
        link = self._issue_link(
            user, AuthActionPurpose.PASSWORD_RESET, PASSWORD_RESET_PATH, settings.password_reset_ttl_seconds
        )
        if link is None:
            return None
        logger.info("password_reset_link_issued_by_operator user_id=%s", user.id)
        return self._to_auth_user(user), link

    # ── sending (background) ──────────────────────────────────────────────────

    def _deliver(
        self,
        sender: MailSender,
        kind: AccountEmail,
        user: UserRecord,
        language: str,
        *,
        link: str = "",
        valid_for_seconds: int = 0,
    ) -> bool:
        sender.send(
            render_account_email(
                kind,
                language,
                to=user.email,
                app_url=settings.public_app_url,
                link=link,
                valid_for_seconds=valid_for_seconds,
            )
        )
        logger.info("account_email_sent kind=%s user_id=%s", kind.value, user.id)
        return True

    @staticmethod
    def _background(kind: AccountEmail, user_id: str | None, work: Callable[[], bool]) -> bool:
        """Run a background send: True when a message went out; a failure is logged and
        returns False, never raises."""
        try:
            return work()
        except Exception as exc:
            logger.warning(
                "account_email_failed kind=%s user_id=%s error=%s",
                kind.value,
                user_id or "-",
                describe_send_failure(exc),
            )
            return False

    def request_password_reset(
        self, email: str, *, language: str = "en", requested_ip: str | None = None
    ) -> bool:
        """Background half of POST /v1/auth/password/forgot: email a reset link to the
        account with this address. Nothing happens for an unknown address or with email
        disabled, and nothing tells the two apart from a sent link."""

        def work() -> bool:
            sender = self._mail()
            if not sender.enabled:
                return False
            user = self.task_store.get_user_by_email((email or "").strip().lower())
            if user is None:
                return False
            ttl = settings.password_reset_ttl_seconds
            link = self._issue_link(
                user, AuthActionPurpose.PASSWORD_RESET, PASSWORD_RESET_PATH, ttl, requested_ip
            )
            if link is None:
                return False
            return self._deliver(sender, AccountEmail.PASSWORD_RESET, user, language, link=link, valid_for_seconds=ttl)

        return self._background(AccountEmail.PASSWORD_RESET, None, work)

    def send_email_verification(self, user_id: str, *, language: str = "en") -> bool:
        """Background: email the account a verification link, unless its address is
        verified already (or email is disabled, or the account is gone)."""

        def work() -> bool:
            sender = self._mail()
            if not sender.enabled:
                return False
            user = self.task_store.get_user_by_id(user_id)
            if user is None or user.email_verified_at is not None:
                return False
            ttl = settings.email_verification_ttl_seconds
            link = self._issue_link(user, AuthActionPurpose.EMAIL_VERIFICATION, EMAIL_VERIFICATION_PATH, ttl)
            if link is None:
                return False
            return self._deliver(
                sender, AccountEmail.EMAIL_VERIFICATION, user, language, link=link, valid_for_seconds=ttl
            )

        return self._background(AccountEmail.EMAIL_VERIFICATION, user_id, work)

    def send_account_notice(self, user_id: str, kind: AccountEmail, *, language: str = "en") -> bool:
        """Background: a security notice to the account's address (password reset, Google
        sign-in linked, password changed). Never a message that carries a link."""
        kind = AccountEmail(kind)
        if kind not in ACCOUNT_NOTICES:
            raise ValueError(f"{kind.value} is not an account notice")

        def work() -> bool:
            sender = self._mail()
            if not sender.enabled:
                return False
            user = self.task_store.get_user_by_id(user_id)
            if user is None:
                return False
            return self._deliver(sender, kind, user, language)

        return self._background(kind, user_id, work)

    # ── redeeming links (request path) ────────────────────────────────────────

    def reset_password_with_token(self, token: str, password: str) -> AuthUser:
        """POST /v1/auth/password/reset. In one store transaction: the token is used up,
        the password set, the email marked verified (the link proved the mailbox), every
        session revoked and the account's other reset links invalidated. Signs nobody in.

        BadRequestError(RESET_TOKEN_INVALID_DETAIL) when the link does not redeem."""
        from src.auth.security import hash_password

        if len(password or "") < 6:  # the set-password minimum
            raise UnprocessableError("Password must be at least 6 characters")
        if not token:
            raise BadRequestError(RESET_TOKEN_INVALID_DETAIL)
        user = self.task_store.reset_password_with_token(hash_link_token(token), hash_password(password))
        if user is None:
            raise BadRequestError(RESET_TOKEN_INVALID_DETAIL)
        logger.info("password_reset_completed user_id=%s", user.id)
        return self._to_auth_user(user)

    def verify_email_with_token(self, token: str) -> AuthUser:
        """POST /v1/auth/email/verify: use up the token and mark the email verified.

        BadRequestError(VERIFICATION_TOKEN_INVALID_DETAIL) when the link does not redeem."""
        if not token:
            raise BadRequestError(VERIFICATION_TOKEN_INVALID_DETAIL)
        user = self.task_store.verify_email_with_token(hash_link_token(token))
        if user is None:
            raise BadRequestError(VERIFICATION_TOKEN_INVALID_DETAIL)
        logger.info("email_verified user_id=%s", user.id)
        return self._to_auth_user(user)

    # ── retention ─────────────────────────────────────────────────────────────

    def cleanup_expired_auth_action_tokens(self) -> int:
        """Maintenance: delete the link tokens that expired or were used."""
        deleted = self.task_store.cleanup_auth_action_tokens(datetime.now(timezone.utc))
        if deleted:
            logger.info("auth_action_tokens_cleaned deleted_count=%s", deleted)
        return deleted
