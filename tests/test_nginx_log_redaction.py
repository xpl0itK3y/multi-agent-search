"""The nginx access-log maps (web/nginx.conf) that redact share tokens.

nginx matches them with PCRE, a backtracking engine like Python's re, and none of them
uses a construct the two treat differently, so they are run here with re: each map
chain must redact the spellings the API's filter redacts (tests/test_access_log_redaction.py),
and stay linear in the line's length. A backtracking pattern stalls the nginx worker
on every such line and, past PCRE's match limit, makes nginx log the map's default,
which is the raw line with its token.
"""
import pathlib
import re
import time

import pytest

NGINX_CONF = pathlib.Path(__file__).resolve().parents[1] / "web" / "nginx.conf"
TOKEN = "SECRETSHARETOKEN1234567890abcdef"

_MAP_RE = re.compile(r"^map \$(\w+) \$(\w+) \{\n(.*?)^\}", re.MULTILINE | re.DOTALL)
_ENTRY_RE = re.compile(r'^\s*(default|"[^"]*")\s+("[^"]*"|\S+);', re.MULTILINE)
_VARIABLE_RE = re.compile(r"\$\{(\w+)\}|\$(\w+)")


def _load_maps() -> dict:
    """{target: (source, default, exact entries, [(compiled regex, value)])}."""
    maps = {}
    for source, target, body in _MAP_RE.findall(NGINX_CONF.read_text(encoding="utf-8")):
        default, exact, regexes = f"${source}", {}, []
        for key, value in _ENTRY_RE.findall(body):
            value = value.strip('"')
            if key == "default":
                default = value
            elif key.startswith('"~'):
                pattern = key[1:-1]
                flags = re.IGNORECASE if pattern.startswith("~*") else 0
                pattern = pattern[2:] if flags else pattern[1:]
                # PCRE's (?<name>...) is (?P<name>...) in re.
                regexes.append((re.compile(re.sub(r"\(\?<(?=[A-Za-z_])", "(?P<", pattern), flags), value))
            else:
                exact[key[1:-1]] = value
        maps[target] = (source, default, exact, regexes)
    return maps


MAPS = _load_maps()


def _evaluate(name: str, variables: dict[str, str]) -> str:
    """The value of $name as nginx computes it: exact entries first, then the regexes in
    order, then the default; a regex's named captures fill its ${...} references."""
    if name not in MAPS:
        return variables[name]
    source, default, exact, regexes = MAPS[name]
    value = _evaluate(source, variables)
    if value in exact:
        return _expand(exact[value], variables, {})
    for regex, result in regexes:
        match = regex.search(value)
        if match:
            return _expand(result, variables, match.groupdict())
    return _expand(default, variables, {})


def _expand(template: str, variables: dict[str, str], captures: dict[str, str]) -> str:
    def lookup(match: re.Match) -> str:
        name = match.group(1) or match.group(2)
        return captures[name] if name in captures else _evaluate(name, variables)

    return _VARIABLE_RE.sub(lookup, template)


def test_the_log_format_uses_the_redacted_variables():
    conf = NGINX_CONF.read_text(encoding="utf-8")
    log_format = conf[conf.index("log_format redacted") : conf.index(";", conf.index("log_format redacted"))]
    assert "$redacted_request_uri" in log_format and "$redacted_http_referer" in log_format
    assert "$request_uri" not in log_format.replace("$redacted_request_uri", "")
    assert "$http_referer" not in log_format.replace("$redacted_http_referer", "")
    assert "access_log /var/log/nginx/access.log redacted;" in conf


@pytest.mark.parametrize(
    ("request_uri", "expected"),
    [
        (f"/v1/public/research/{TOKEN}", "/v1/public/research/[redacted]"),
        (f"/v1/public/research/{TOKEN}?format=md", "/v1/public/research/[redacted]?format=md"),
        (f"//v1/public/research/{TOKEN}", "//v1/public/research/[redacted]"),
        (f"///v1//Public/RESEARCH%2F{TOKEN}", "///v1//Public/RESEARCH%2F[redacted]"),
        (f"/v1%252fpublic%252Fresearch%252f{TOKEN}", "/v1%252fpublic%252Fresearch%252f[redacted]"),
        (f"/prefix/v1/public/research/{TOKEN}/export", "/prefix/v1/public/research/[redacted]/export"),
        (f"/r/{TOKEN}", "/r/[redacted]"),
        (f"//R/{TOKEN}?x=1", "//R/[redacted]?x=1"),
        (f"/login?redirect=/r/{TOKEN}", "/login?redirect=/r/[redacted]"),
        (f"/login?redirect=%2Fr%2F{TOKEN}", "/login?redirect=%2Fr%2F[redacted]"),
        ("/v1/research/abc/r/keep", "/v1/research/abc/r/keep"),
        ("/assets/index.js", "/assets/index.js"),
    ],
)
def test_request_line_is_redacted(request_uri, expected):
    assert _evaluate("redacted_request_uri", {"request_uri": request_uri}) == expected


@pytest.mark.parametrize(
    ("referer", "expected"),
    [
        (f"https://h.example/r/{TOKEN}", "https://h.example/r/[redacted]"),
        (f"HTTPS://Example.com//r/{TOKEN}", "HTTPS://Example.com//r/[redacted]"),
        (f"https://h.example%2fr%2f{TOKEN}", "https://h.example%2fr%2f[redacted]"),
        (f"https://h.example/v1/public/research/{TOKEN}", "https://h.example/v1/public/research/[redacted]"),
        (f"https://h.example/login?redirect=/r/{TOKEN}", "https://h.example/login?redirect=/r/[redacted]"),
        ("https://h.example/research/abc/r/keep", "https://h.example/research/abc/r/keep"),
        ("", "-"),
    ],
)
def test_referer_is_redacted(referer, expected):
    assert _evaluate("redacted_http_referer", {"http_referer": referer}) == expected


# nginx takes a request line or a header of at most 8 KB by default
# (large_client_header_buffers): the longest line any of these maps sees.
LINE_LENGTH = 8 * 1024
MATCH_BUDGET_SECONDS = 0.05


@pytest.mark.parametrize("separator", ["/", "%2f", "%2F", "%252f", "=", "=/"])
@pytest.mark.parametrize(
    "head",
    ["", "/v1", "/v1/public", "/v1/public/research", "/x?a=", "/x?redirect=", "https://h.example", "/r"],
)
def test_every_map_regex_stays_linear_on_a_run_of_separators(head, separator):
    line = head + separator * ((LINE_LENGTH - len(head)) // len(separator))
    regexes = [regex for _source, _default, _exact, entries in MAPS.values() for regex, _value in entries]
    assert len(regexes) >= 5

    for regex in regexes:
        started = time.perf_counter()
        regex.search(line)
        elapsed = time.perf_counter() - started
        assert elapsed < MATCH_BUDGET_SECONDS, (regex.pattern, elapsed)
