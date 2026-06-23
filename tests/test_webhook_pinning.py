"""SEC-007: webhook delivery pins the connection to a validated public IP, closing the
DNS-rebind TOCTOU window between validation and httpx's own resolution."""
import httpx

from src import net_safety


def _addrinfo(ip, port=443):
    return [(2, 1, 6, "", (ip, port))]


def test_resolve_validated_ip_returns_public_ip(monkeypatch):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("8.8.8.8"))
    ip, reason = net_safety.resolve_validated_ip("https://dns.google/")
    assert ip == "8.8.8.8" and reason == "ok"


def test_resolve_validated_ip_rejects_private(monkeypatch):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("10.0.0.5"))
    ip, reason = net_safety.resolve_validated_ip("https://internal.example/")
    assert ip is None and "non-public" in reason


def test_safe_post_json_pins_ip_and_keeps_host(monkeypatch):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("93.184.216.34"))
    captured = {}

    def fake_post(url, **kw):
        captured["url"] = url
        captured["kw"] = kw

    monkeypatch.setattr(httpx, "post", fake_post)
    assert net_safety.safe_post_json("https://example.com/hook", {"x": 1}) is True
    assert captured["url"] == "https://93.184.216.34:443/hook"  # connects to the pinned IP
    assert captured["kw"]["headers"]["Host"] == "example.com"
    assert captured["kw"]["extensions"]["sni_hostname"] == "example.com"


def test_safe_post_json_blocks_rebind_to_metadata(monkeypatch):
    monkeypatch.setattr(
        net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo("169.254.169.254", 80)
    )
    called = []
    monkeypatch.setattr(httpx, "post", lambda *a, **k: called.append(1))
    assert net_safety.safe_post_json("http://rebind.evil/hook", {"x": 1}) is False
    assert called == []  # never connected to the rebound private address
