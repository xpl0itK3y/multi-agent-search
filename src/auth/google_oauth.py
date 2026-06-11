"""Google OAuth 2.0 (Authorization Code flow) — stdlib + httpx, no new deps.

We exchange the authorization code for a Google access token (using our client
secret), then read the verified identity from Google's userinfo endpoint. Google
validates the access token, so we don't verify an id_token signature locally.
"""

from __future__ import annotations

from urllib.parse import urlencode

import httpx

from src.config import settings

AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
USERINFO_ENDPOINT = "https://www.googleapis.com/oauth2/v3/userinfo"


def build_authorization_url(state: str) -> str:
    """The Google consent-screen URL to redirect the browser to."""
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"{AUTH_ENDPOINT}?{urlencode(params)}"


def fetch_userinfo(code: str) -> dict:
    """Exchange the auth code for a token and return the verified user profile.

    Returns Google's userinfo dict (``sub``, ``email``, ``email_verified``, ...).
    Raises on any HTTP error or a missing access token.
    """
    token_response = httpx.post(
        TOKEN_ENDPOINT,
        data={
            "code": code,
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "redirect_uri": settings.google_redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=15.0,
    )
    token_response.raise_for_status()
    access_token = token_response.json().get("access_token")
    if not access_token:
        raise ValueError("Google token exchange returned no access_token")

    userinfo_response = httpx.get(
        USERINFO_ENDPOINT,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15.0,
    )
    userinfo_response.raise_for_status()
    return userinfo_response.json()
