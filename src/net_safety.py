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


def _classify_ip(ip_str: str) -> str | None:
    """Return a rejection reason if ``ip_str`` is a non-public/routable address, else None."""
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return f"invalid resolved address: {ip_str}"
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
        return f"resolves to non-public address {ip}"
    return None


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
        reason = _classify_ip(info[4][0])
        if reason:
            return False, reason

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


def resolve_validated_ip(url: str) -> tuple[str | None, str]:
    """Resolve ``url``'s host ONCE and return (validated_ip, reason).

    Unlike is_safe_public_url (which validates then lets the caller re-resolve), this returns
    the concrete public IP to *connect to*, so the caller can pin it and close the DNS-rebind
    TOCTOU window. validated_ip is None (with a reason) if the host is missing, fails to
    resolve, or any resolved address is non-public.
    """
    if not url or not isinstance(url, str):
        return None, "empty URL"
    try:
        parsed = urlparse(url)
    except Exception:
        return None, "unparseable URL"
    if parsed.scheme not in ("http", "https"):
        return None, f"unsupported scheme: {parsed.scheme or 'none'}"
    host = parsed.hostname
    if not host:
        return None, "missing host"

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except Exception as exc:
        return None, f"DNS resolution failed: {exc}"
    if not infos:
        return None, "host did not resolve"

    chosen: str | None = None
    for info in infos:
        ip_str = info[4][0]
        reason = _classify_ip(ip_str)
        if reason:
            return None, reason  # reject if ANY resolved address is non-public
        if chosen is None:
            chosen = ip_str
    if chosen is None:
        return None, "host did not resolve"
    return chosen, "ok"


def safe_post_json(url: str, payload: object, *, timeout: float = 10.0) -> bool:
    """POST ``payload`` as JSON to a user-supplied webhook with the connection pinned to a
    validated public IP (SEC-007).

    Resolves + validates the host once, then connects to that exact IP while keeping the
    original host for the Host header and TLS SNI/cert verification — so a host that DNS-rebinds
    to a private/metadata address between validation and connect cannot be reached. Best-effort:
    returns True if the request was sent, False if blocked or it failed.
    """
    ip, reason = resolve_validated_ip(url)
    if ip is None:
        logger.warning("webhook_blocked_unsafe_url url=%s reason=%s", url, reason)
        return False

    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    ip_netloc = f"[{ip}]:{port}" if ":" in ip else f"{ip}:{port}"
    pinned_url = parsed._replace(netloc=ip_netloc).geturl()
    try:
        import httpx

        # url host is the pinned IP (connect target); Host header + sni_hostname keep the original
        # hostname so HTTP routing and TLS cert verification still target the intended server.
        httpx.post(
            pinned_url,
            json=payload,
            timeout=timeout,
            headers={"Host": host},
            extensions={"sni_hostname": host},
        )
        return True
    except Exception as exc:
        logger.warning("webhook_failed url=%s error=%s", url, exc)
        return False
