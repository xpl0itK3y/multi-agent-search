"""Helpers to guard outbound requests against SSRF.

Used before firing user-supplied webhooks and before fetching web-page content: a user
(or a page we scrape) must not be able to make the server reach internal/loopback/
link-local/metadata addresses.
"""
from __future__ import annotations

import ipaddress
import logging
import socket
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


def is_safe_public_url(url: str) -> tuple[bool, str]:
    """Return (ok, reason). ``ok`` is True only for http(s) URLs whose host
    resolves exclusively to public, routable IP addresses.

    Rejects loopback, private, link-local (incl. cloud metadata 169.254.169.254),
    reserved, multicast and unspecified addresses across IPv4 and IPv6.
    """
    if not url or not isinstance(url, str):
        return False, "empty URL"
    try:
        parsed = urlparse(url)
    except Exception:
        return False, "unparseable URL"

    if parsed.scheme not in ("http", "https"):
        return False, f"unsupported scheme: {parsed.scheme or 'none'}"

    host = parsed.hostname
    if not host:
        return False, "missing host"

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except Exception as exc:
        return False, f"DNS resolution failed: {exc}"

    if not infos:
        return False, "host did not resolve"

    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            return False, f"invalid resolved address: {ip_str}"
        # Normalise IPv4-mapped IPv6 (e.g. ::ffff:127.0.0.1) before classifying.
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return False, f"resolves to non-public address {ip}"

    return True, "ok"


_FETCH_USER_AGENT = "Mozilla/5.0 (compatible; MultiAgentSearch/1.0; +https://example.com/bot)"


def safe_fetch_html(
    url: str,
    *,
    timeout: float = 15.0,
    max_redirects: int = 1,
    max_bytes: int = 5_000_000,
) -> str | None:
    """Fetch a page's HTML while keeping every hop inside public address space.

    Redirects are followed manually and each hop is re-validated with is_safe_public_url,
    so a page cannot 30x-redirect the server into an internal/metadata target (which a
    plain ``follow_redirects=True`` would). Returns the decoded body, or None if any hop is
    unsafe, the fetch fails, a non-2xx is returned, or the body exceeds ``max_bytes``.

    Residual: a host could DNS-rebind between this validation and httpx's own connect
    resolution. Close that fully at the infra layer (egress allowlist / proxy).
    """
    import httpx

    current = url
    try:
        with httpx.Client(
            follow_redirects=False,
            timeout=timeout,
            headers={"User-Agent": _FETCH_USER_AGENT},
        ) as client:
            for _ in range(max_redirects + 1):
                ok, reason = is_safe_public_url(current)
                if not ok:
                    logger.warning("safe_fetch_blocked url=%s reason=%s", current, reason)
                    return None
                response = client.get(current)
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        return None
                    current = urljoin(current, location)
                    continue
                if response.status_code >= 400:
                    return None
                if len(response.content) > max_bytes:
                    logger.warning("safe_fetch_too_large url=%s bytes=%s", current, len(response.content))
                    return None
                return response.text
    except Exception as exc:
        logger.info("safe_fetch_failed url=%s error=%s", url, exc)
        return None
    return None  # exceeded max_redirects
