"""Brute-force protection for the auth endpoints (AUD-027).

Small in-process sliding-window limiters (per API worker), keyed three ways:

- per client IP on login/register (``enforce_auth_rate_limit``);
- per normalized account email on login (``enforce_login_account_rate_limit``), so
  password guesses spread over many addresses still hit one budget per account;
- per user on the routes that verify ``current_password`` (set-password, delete
  account), so a stolen session cannot be used to guess the password.

They are intentionally a no-op when auth is disabled — the app is fully open in that
mode, so throttling the login form would add nothing. The account-recovery budgets at
the end (forgot-password, reset, email verification) are hourly and always on: they
bound outbound email and hashing work rather than password guessing. For a cross-worker global limit,
back this with Redis (the same pattern as providers/rate_limit.py); per-worker is enough
to blunt a brute force against 2 API workers.
"""
from __future__ import annotations

from fastapi import HTTPException, Request

from src.api.dependencies import get_current_user
from src.api.schemas import AuthUser
from src.auth.sliding_window import SlidingWindowLimiter
from src.config import settings
from src.services.account_recovery_mixin import account_email_limiter


_auth_limiter = SlidingWindowLimiter(protect_lockouts=True)
_account_limiter = SlidingWindowLimiter(protect_lockouts=True)
_password_check_limiter = SlidingWindowLimiter(protect_lockouts=True)


def reset_auth_rate_limiter() -> None:
    """Test hook: drop all recorded hits so suites don't share the auth windows."""
    for limiter in (_auth_limiter, _account_limiter, _password_check_limiter, *_RECOVERY_LIMITERS):
        limiter.reset()


def _auth_limit() -> int:
    """Allowed attempts per minute, or 0 when throttling is off."""
    if settings.auth_disabled:
        return 0
    return max(settings.auth_rate_limit_per_minute, 0)


def enforce_auth_rate_limit(request: Request) -> None:
    limit = _auth_limit()
    if not limit:
        return
    client = request.client
    key = client.host if client else "unknown"
    if not _auth_limiter.allow(key, limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")


def enforce_login_account_rate_limit(email: str) -> None:
    """Per-account login budget. Keyed on the submitted email whether or not it exists,
    so the 429 reveals nothing about which accounts are registered."""
    limit = _auth_limit()
    if not limit:
        return
    if not _account_limiter.allow((email or "").strip().lower(), limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")


def enforce_password_check_rate_limit(request: Request) -> AuthUser:
    """Require an identity and consume one per-user current-password check allowance."""
    user = get_current_user(request)
    limit = _auth_limit()
    if limit and not _password_check_limiter.allow(user.id, limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")
    return user


# ── account recovery (AUTH-RECOVERY) ──────────────────────────────────────────
# Hourly budgets. Unlike the limiters above these stay on with AUTH_DISABLED=true: they
# bound the email the API sends at a stranger's request (forgot-password, verification
# links) and the password hashing a reset costs, whatever the auth mode. Keys never
# depend on whether an account exists, so a 429 reveals nothing about which do.
RECOVERY_WINDOW_SECONDS = 3600.0
FORGOT_PASSWORD_PER_IP_PER_HOUR = 20
FORGOT_PASSWORD_PER_EMAIL_PER_HOUR = 5
# Redeeming a link (reset-password, verify-email), per client IP and per route. A token
# has 256 random bits, so this bounds work, not guessing.
LINK_REDEEM_PER_IP_PER_HOUR = 30
VERIFICATION_EMAIL_PER_USER_PER_HOUR = 5

# Without lockout protection (SEC-REC-4): with it, a table full of keys at their limit
# (10,000 addresses or clients) refused every new one for the hour, turning recovery off
# for everyone. What these limit is harmless to repeat, since a link token has 256 random
# bits and every reset mail goes to the account's own address, so forgetting the oldest
# lockout at the cap only hands one address or client a fresh budget.
_forgot_password_ip_limiter = SlidingWindowLimiter(RECOVERY_WINDOW_SECONDS, protect_lockouts=False)
_forgot_password_email_limiter = SlidingWindowLimiter(RECOVERY_WINDOW_SECONDS, protect_lockouts=False)
_password_reset_ip_limiter = SlidingWindowLimiter(RECOVERY_WINDOW_SECONDS, protect_lockouts=False)
_email_verify_ip_limiter = SlidingWindowLimiter(RECOVERY_WINDOW_SECONDS, protect_lockouts=False)
_verification_email_limiter = SlidingWindowLimiter(RECOVERY_WINDOW_SECONDS, protect_lockouts=False)
_RECOVERY_LIMITERS = (
    _forgot_password_ip_limiter,
    _forgot_password_email_limiter,
    _password_reset_ip_limiter,
    _email_verify_ip_limiter,
    _verification_email_limiter,
    # The per-recipient account email budget, which the background senders check
    # (src/services/account_recovery_mixin.py): listed here for the reset hook.
    account_email_limiter,
)


def _client_key(request: Request) -> str:
    client = request.client
    return client.host if client and client.host else "unknown"


def _refuse_unless(allowed: bool) -> None:
    if not allowed:
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")


def enforce_forgot_password_ip_rate_limit(request: Request) -> None:
    _refuse_unless(_forgot_password_ip_limiter.allow(_client_key(request), FORGOT_PASSWORD_PER_IP_PER_HOUR))


def enforce_forgot_password_email_rate_limit(email: str) -> None:
    """Per-address budget for reset links, keyed on the normalized email whether or not
    an account has it: one address cannot be flooded with mail from many clients."""
    key = (email or "").strip().lower()
    _refuse_unless(_forgot_password_email_limiter.allow(key, FORGOT_PASSWORD_PER_EMAIL_PER_HOUR))


def enforce_password_reset_rate_limit(request: Request) -> None:
    _refuse_unless(_password_reset_ip_limiter.allow(_client_key(request), LINK_REDEEM_PER_IP_PER_HOUR))


def enforce_email_verify_rate_limit(request: Request) -> None:
    _refuse_unless(_email_verify_ip_limiter.allow(_client_key(request), LINK_REDEEM_PER_IP_PER_HOUR))


def enforce_verification_email_rate_limit(request: Request) -> AuthUser:
    """Require an identity and consume one of its hourly verification-email sends."""
    user = get_current_user(request)
    _refuse_unless(_verification_email_limiter.allow(user.id, VERIFICATION_EMAIL_PER_USER_PER_HOUR))
    return user
