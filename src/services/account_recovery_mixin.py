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

Who may redeem. A reset link is anonymous: it replaces the password and revokes every
session, so it proves inbox control for the only credential left. A verification link
redeems only in a session of the account it was sent to. Verification is what lets a
password survive a later Google link (AuthMixin._link_google_identity), so it has to show
that whoever holds the password also controls the inbox. An anonymous click shows the
inbox alone: a stranger's sign-up with someone's address would turn verified at its
owner's click and keep the stranger's password and sessions (and admin rights, for an
ADMIN_EMAILS address) once the owner signs in with Google.

Email goes out off the request path: the routes hand the methods documented as
"background" to FastAPI BackgroundTasks. Those never raise: a failure is logged by kind,
account id and exception class (never a token, a link or the error text) and changes no
response. POST /v1/auth/password/forgot does all of its work there, so its response is
the same, and as quick, for a known and an unknown address.

How much. Every recipient has one hourly budget for all of this mail
(ACCOUNT_EMAIL_PER_RECIPIENT_PER_HOUR), checked in the background job before a link is
issued; past it the message is skipped without a word to the caller.
"""
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Callable

from src.auth.sliding_window import SlidingWindowLimiter
from src.config import settings
from src.domain import AuthActionPurpose, AuthUser, UserRecord
from src.domain.errors import BadRequestError, ForbiddenError, UnprocessableError
from src.notifications import (
    AccountEmail,
    MailSender,
    create_mail_sender,
    describe_send_failure,
    mailbox_key,
    render_account_email,
)

logger = logging.getLogger(__name__)

# The 400 details of a link that does not redeem (unknown, used, expired, or sent to an
# address the account no longer has). The web UI keys on the prefix before the colon.
RESET_TOKEN_INVALID_DETAIL = "reset_token_invalid: this password reset link is invalid or has expired"
VERIFICATION_TOKEN_INVALID_DETAIL = "verification_token_invalid: this verification link is invalid or has expired"
# The 403 of a live verification link redeemed in another account's session: it is left
# unused, for its own account to redeem. The web UI keys on the prefix too.
VERIFICATION_WRONG_ACCOUNT_DETAIL = "verification_wrong_account: sign in to the account this link was sent to"

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

# One hourly budget per recipient address for every account email, whatever asked for it:
# a sign-up, a resend, a reset request, a notice (SEC-REC2-2). The routes bound their
# callers per client address or per account only, and a register/delete loop gets a fresh
# account each time, so an address nobody has proven could otherwise be sent hundreds of
# messages an hour. The security notices to a verified address take none of it
# (_within_mail_budget). Per API process like every throttle, and without lockout
# protection like the recovery ones (login_rate_limit, SEC-REC-4): with 10,000 addresses
# over budget the oldest is forgotten, rather than every new address refused.
ACCOUNT_EMAIL_PER_RECIPIENT_PER_HOUR = 10
account_email_limiter = SlidingWindowLimiter(3600.0, protect_lockouts=False)


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
    def _within_mail_budget(kind: AccountEmail, user: UserRecord) -> bool:
        """Take one of the account address's hourly sends (account_email_limiter), or
        False past the budget: the caller then sends nothing and issues no link, since a
        new link would retire the one already in the inbox. A security notice to a
        verified address is exempt and takes nothing: a flood must not hide a real
        password change from the owner. Logged by kind and account id, never the address."""
        if kind in ACCOUNT_NOTICES and user.email_verified_at is not None:
            return True
        if account_email_limiter.allow(mailbox_key(user.email), ACCOUNT_EMAIL_PER_RECIPIENT_PER_HOUR):
            return True
        logger.info("account_email_over_budget kind=%s user_id=%s", kind.value, user.id)
        return False

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
        account with this address. Nothing happens for an unknown address, with email
        disabled or past the address's mail budget, and nothing tells those apart from a
        sent link."""

        def work() -> bool:
            sender = self._mail()
            if not sender.enabled:
                return False
            user = self.task_store.get_user_by_email((email or "").strip().lower())
            if user is None or not self._within_mail_budget(AccountEmail.PASSWORD_RESET, user):
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
        verified already (or email is disabled, the account is gone, or the address is past
        its mail budget: the link already sent then stays the one that works)."""

        def work() -> bool:
            sender = self._mail()
            if not sender.enabled:
                return False
            user = self.task_store.get_user_by_id(user_id)
            if user is None or user.email_verified_at is not None:
                return False
            if not self._within_mail_budget(AccountEmail.EMAIL_VERIFICATION, user):
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
            if user is None or not self._within_mail_budget(kind, user):
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

    def verify_email_with_token(self, token: str, user_id: str) -> AuthUser:
        """POST /v1/auth/email/verify for the signed-in account ``user_id``: use up the
        token and mark the email verified, when the link was sent to this account.

        ForbiddenError(VERIFICATION_WRONG_ACCOUNT_DETAIL) for a live link of another
        account, which stays unused; BadRequestError(VERIFICATION_TOKEN_INVALID_DETAIL)
        when the link does not redeem at all."""
        if not token:
            raise BadRequestError(VERIFICATION_TOKEN_INVALID_DETAIL)
        token_hash = hash_link_token(token)
        user = self.task_store.verify_email_with_token(token_hash, user_id)
        if user is None:
            live = self.task_store.get_live_auth_action_token(token_hash, AuthActionPurpose.EMAIL_VERIFICATION)
            if live is not None and live.user_id != user_id:
                logger.info("email_verification_refused_other_account user_id=%s", user_id)
                raise ForbiddenError(VERIFICATION_WRONG_ACCOUNT_DETAIL)
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
