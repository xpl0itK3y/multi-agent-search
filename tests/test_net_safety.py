from src.net_safety import is_safe_public_url


def test_blocks_loopback():
    assert is_safe_public_url("http://127.0.0.1/hook")[0] is False
    assert is_safe_public_url("http://localhost:8000/hook")[0] is False


def test_blocks_private_ranges():
    assert is_safe_public_url("http://10.0.0.5/hook")[0] is False
    assert is_safe_public_url("http://192.168.1.10/hook")[0] is False
    assert is_safe_public_url("http://172.16.0.1/hook")[0] is False


def test_blocks_cloud_metadata():
    assert is_safe_public_url("http://169.254.169.254/latest/meta-data/")[0] is False


def test_blocks_ipv6_loopback():
    assert is_safe_public_url("http://[::1]/hook")[0] is False


def test_blocks_non_http_scheme():
    assert is_safe_public_url("ftp://example.com/x")[0] is False
    assert is_safe_public_url("file:///etc/passwd")[0] is False


def test_blocks_missing_or_garbage():
    assert is_safe_public_url("http://")[0] is False
    assert is_safe_public_url("not a url")[0] is False
    assert is_safe_public_url("")[0] is False


def test_allows_public_ip():
    ok, reason = is_safe_public_url("https://8.8.8.8/hook")
    assert ok is True, reason
