"""Helpers to guard outbound requests against SSRF.

Used before firing user-supplied webhooks: a user must not be able to make the
server reach internal/loopback/link-local/metadata addresses.
"""
from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse


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
