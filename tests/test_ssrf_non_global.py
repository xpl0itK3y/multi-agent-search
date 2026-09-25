"""SEC2-5: the SSRF guard rejects every address that is not globally routable, the shared
address space 100.64.0.0/10 (carrier-grade NAT, Tailscale) included, on both the webhook
path (resolve_validated_ip / safe_post_json) and the page-fetch path (is_safe_public_url /
safe_fetch_document)."""
import httpx
import pytest

from src import net_safety

NON_GLOBAL = [
    "100.64.0.1",  # shared address space (RFC 6598): neither private nor reserved
    "100.100.100.200",  # Tailscale's MagicDNS / some providers' metadata endpoints
    "100.127.255.254",
    "::ffff:100.64.0.1",  # IPv4-mapped form of the same range
    "fec0::1",  # deprecated IPv6 site-local, which is_global calls global
    "192.0.0.8",  # IETF protocol assignments
    "198.18.0.1",  # benchmarking
    "192.0.2.1",  # documentation
    "2001:db8::1",
]
# Still rejected by the named checks that is_global alone would pass.
GLOBAL_BUT_UNSAFE = ["224.0.0.1", "239.1.1.1", "ff0e::1", "64:ff9b::a9fe:a9fe"]
PUBLIC = ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"]


def _addrinfo(ip, port=443):
    family = 23 if ":" in ip else 2
    return [(family, 1, 6, "", (ip, port))]


@pytest.mark.parametrize("ip", NON_GLOBAL + GLOBAL_BUT_UNSAFE)
def test_classifier_rejects_non_global_addresses(ip):
    assert net_safety._classify_ip(ip) is not None


@pytest.mark.parametrize("ip", PUBLIC)
def test_classifier_allows_public_unicast(ip):
    assert net_safety._classify_ip(ip) is None


@pytest.mark.parametrize("ip", ["100.64.0.1", "100.100.100.200"])
def test_webhook_to_shared_address_space_is_never_sent(monkeypatch, ip):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo(ip))
    sent = []
    monkeypatch.setattr(httpx, "post", lambda *a, **k: sent.append(a))

    assert net_safety.resolve_validated_ip("https://hooks.example/x")[0] is None
    assert net_safety.safe_post_json("https://hooks.example/x", {"ok": True}) is False
    assert sent == []


@pytest.mark.parametrize("ip", ["100.64.0.1", "100.100.100.200"])
def test_page_fetch_to_shared_address_space_is_never_made(monkeypatch, ip):
    monkeypatch.setattr(net_safety.socket, "getaddrinfo", lambda *a, **k: _addrinfo(ip, 80))
    fetched = []

    class _Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get(self, url):
            fetched.append(url)
            raise AssertionError(f"fetched a non-global target: {url}")

    monkeypatch.setattr(httpx, "Client", _Client)

    assert net_safety.is_safe_public_url("http://cgnat.example/page")[0] is False
    assert net_safety.safe_fetch_document("http://cgnat.example/page") is None
    assert fetched == []


def test_one_non_global_answer_among_public_ones_blocks_the_host(monkeypatch):
    monkeypatch.setattr(
        net_safety.socket,
        "getaddrinfo",
        lambda *a, **k: _addrinfo("93.184.216.34") + _addrinfo("100.64.0.9"),
    )

    assert net_safety.resolve_validated_ip("https://mixed.example/")[0] is None
    assert net_safety.is_safe_public_url("https://mixed.example/")[0] is False
