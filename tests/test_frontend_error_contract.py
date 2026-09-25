"""FE-CONTRACT: the web UI keys some messages on the server's English `detail` text.

The API sends no error codes. web/src/lib/api.ts maps known details of a status to a
specific i18n key (API_DETAIL_KEYS) and matches detail prefixes (isReauthRequired), and
web/src/lib/stream.ts maps known stream_error details (SERVER_STREAM_ERRORS). Each of those
patterns must still match a detail the backend can raise with that status: rewording a
backend message then fails here, instead of the UI silently falling back to its generic text.

The backend side is read statically from src/: the message of every ServiceError subclass
and HTTPException raise, JSONResponse {"detail": ...} bodies and stream_error payloads.
String literals, f-strings (a placeholder becomes a sample word, or the value of a
module-level string constant) and helper functions that return one are resolved.

The check runs one way only, frontend pattern -> backend source: a backend detail with no
frontend mapping is fine, so this passes before and after the web side adds a mapping.
"""
from __future__ import annotations

import ast
import itertools
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

from src.domain import errors as domain_errors

ROOT = Path(__file__).resolve().parent.parent
API_TS = ROOT / "web" / "src" / "lib" / "api.ts"
STREAM_TS = ROOT / "web" / "src" / "lib" / "stream.ts"
SRC = ROOT / "src"
SAMPLE = "sample"  # stands in for an f-string placeholder; matches \w+ and .+

SERVICE_ERROR_STATUS = {
    name: value.status_code
    for name, value in vars(domain_errors).items()
    if isinstance(value, type) and issubclass(value, domain_errors.ServiceError)
}


# ── frontend patterns ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FrontendPattern:
    source: str  # where the web UI matches it, for the failure message
    status: int | None  # None: any status (stream errors carry none)
    regex: re.Pattern

    @property
    def id(self) -> str:
        return f"{self.source}:{self.status}:{self.regex.pattern}"


def _object_literal(source: str, name: str, path: Path) -> str:
    match = re.search(rf"\bconst {name}\b[^=]*=\s*\{{(.*?)\n\}};", source, re.S)
    assert match, f"const {name} not found in {path.name}: update {Path(__file__).name}'s parser"
    return match.group(1)


def _js_regex(body: str, flags: str) -> re.Pattern:
    # The JS syntax the mappings use is Python syntax too, bar named groups.
    python_body = re.sub(r"\(\?<(?![=!])", "(?P<", body)
    return re.compile(python_body, re.IGNORECASE if "i" in flags else 0)


_REGEX_ENTRY = re.compile(r'\[\s*/((?:\\.|[^/\\\n])+)/([dgimsuy]*)\s*,\s*"([\w.]+)"\s*\]')


def _api_detail_key_patterns(source: str) -> list[FrontendPattern]:
    block = _object_literal(source, "API_DETAIL_KEYS", API_TS)
    statuses = list(re.finditer(r"(?m)^\s*(\d{3})\s*:\s*\[", block))
    assert statuses, "API_DETAIL_KEYS has no status sections: update the parser"
    patterns: list[FrontendPattern] = []
    for current, following in itertools.zip_longest(statuses, statuses[1:]):
        section = block[current.end() : following.start() if following else len(block)]
        entries = _REGEX_ENTRY.findall(section)
        # Every `"key"]` in the section must have been read as a /regex/ entry.
        assert len(entries) == len(re.findall(r'"[\w.]+"\s*\]', section)), section
        patterns += [
            FrontendPattern(f"API_DETAIL_KEYS.{key}", int(current.group(1)), _js_regex(body, flags))
            for body, flags, key in entries
        ]
    return patterns


_DETAIL_TEST = re.compile(r'(?<!typeof )\bdetail\s*(?:\.\s*(startsWith|includes)\s*\(\s*"([^"]+)"\s*\)|===\s*"([^"]+)")')


def _detail_string_tests(source: str) -> list[FrontendPattern]:
    """`detail.startsWith("...")`, `.includes("...")` and `detail === "..."` checks, with the
    status compared on the same line (isReauthRequired: 403 and the reauth_required prefix)."""
    patterns: list[FrontendPattern] = []
    for line in source.splitlines():
        for method, needle, exact in _DETAIL_TEST.findall(line):
            status = re.search(r"\bstatus\s*===\s*(\d{3})", line)
            if exact:
                regex = re.compile(rf"^{re.escape(exact)}$")
            elif method == "startsWith":
                regex = re.compile(rf"^{re.escape(needle)}")
            else:
                regex = re.compile(re.escape(needle))
            patterns.append(
                FrontendPattern(f"{API_TS.name} detail test", int(status.group(1)) if status else None, regex)
            )
    return patterns


def _stream_error_patterns(source: str) -> list[FrontendPattern]:
    block = _object_literal(source, "SERVER_STREAM_ERRORS", STREAM_TS)
    keys = re.findall(r'(?m)^\s*(?:"([^"]+)"|(\w+))\s*:\s*"[\w.]+"', block)
    assert len(keys) == block.count(":"), block
    return [
        FrontendPattern(f"SERVER_STREAM_ERRORS.{quoted or bare}", None, re.compile(rf"^{re.escape(quoted or bare)}$"))
        for quoted, bare in keys
    ]


def _frontend_patterns() -> list[FrontendPattern]:
    api_source = API_TS.read_text(encoding="utf-8")
    return (
        _api_detail_key_patterns(api_source)
        + _detail_string_tests(api_source)
        + _stream_error_patterns(STREAM_TS.read_text(encoding="utf-8"))
    )


FRONTEND_PATTERNS = _frontend_patterns()


# ── backend details ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BackendDetail:
    status: int | None  # None: a stream_error payload
    text: str
    where: str


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    return {
        node.targets[0].id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }


class _Module:
    def __init__(self, path: Path, tree: ast.Module, global_constants: dict[str, str]):
        self.path = path
        local = _module_string_constants(tree)
        imported = {
            alias.asname or alias.name: global_constants[alias.name]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
            if alias.name in global_constants
        }
        self.constants = {**imported, **local}
        # Top-level functions and methods, by name: helpers that build a detail.
        self.functions = {
            node.name: node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

    def resolve(self, expr: ast.expr | None, depth: int = 0) -> list[str]:
        """The texts an expression can evaluate to, as far as a static read can tell."""
        if expr is None or depth > 3:
            return []
        if isinstance(expr, ast.Constant):
            return [expr.value] if isinstance(expr.value, str) else []
        if isinstance(expr, ast.JoinedStr):
            parts = []
            for value in expr.values:
                if isinstance(value, ast.Constant):
                    parts.append(str(value.value))
                elif isinstance(value.value, ast.Name) and value.value.id in self.constants:
                    parts.append(self.constants[value.value.id])
                else:
                    parts.append(SAMPLE)
            return ["".join(parts)]
        if isinstance(expr, ast.Name):
            return [self.constants[expr.id]] if expr.id in self.constants else []
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
            lefts, rights = self.resolve(expr.left, depth + 1), self.resolve(expr.right, depth + 1)
            return [left + right for left in lefts for right in rights]
        if isinstance(expr, ast.IfExp):
            return self.resolve(expr.body, depth + 1) + self.resolve(expr.orelse, depth + 1)
        if isinstance(expr, ast.Call):
            func = expr.func
            if isinstance(func, ast.Attribute) and func.attr == "format":  # "...".format(...)
                return self.resolve(func.value, depth + 1)
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            helper = self.functions.get(name or "")
            if helper is not None:
                return [
                    text
                    for node in ast.walk(helper)
                    if isinstance(node, ast.Return)
                    for text in self.resolve(node.value, depth + 1)
                ]
        return []


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((kw.value for kw in call.keywords if kw.arg == name), None)


def _status(expr: ast.expr | None) -> int | None:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, int):
        return expr.value
    if isinstance(expr, ast.Attribute):  # status.HTTP_409_CONFLICT
        match = re.match(r"HTTP_(\d{3})_", expr.attr)
        return int(match.group(1)) if match else None
    return None


def _detail_of(expr: ast.expr | None) -> ast.expr | None:
    if isinstance(expr, ast.Dict):
        for key, value in zip(expr.keys, expr.values):
            if isinstance(key, ast.Constant) and key.value == "detail":
                return value
    return None


def _details_raised(module: _Module, tree: ast.Module) -> list[BackendDetail]:
    found: list[tuple[int | None, ast.expr | None, int]] = []
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        name = _call_name(call)
        if name in SERVICE_ERROR_STATUS:
            message = call.args[0] if call.args else _keyword(call, "detail")
            found.append((SERVICE_ERROR_STATUS[name], message, call.lineno))
        elif name == "HTTPException":
            status = _keyword(call, "status_code") or (call.args[0] if call.args else None)
            message = _keyword(call, "detail") or (call.args[1] if len(call.args) > 1 else None)
            found.append((_status(status), message, call.lineno))
        elif name == "JSONResponse":
            found.append((_status(_keyword(call, "status_code")), _detail_of(_keyword(call, "content")), call.lineno))
        elif (
            name == "sse"
            and len(call.args) == 2
            and isinstance(call.args[0], ast.Constant)
            and call.args[0].value == "stream_error"
        ):
            found.append((None, _detail_of(call.args[1]), call.lineno))
    where = module.path.relative_to(ROOT).as_posix()
    return [
        BackendDetail(status, text, f"{where}:{line}")
        for status, message, line in found
        for text in module.resolve(message)
    ]


def _backend_details() -> list[BackendDetail]:
    parsed = [(path, ast.parse(path.read_text(encoding="utf-8"))) for path in sorted(SRC.rglob("*.py"))]
    global_constants: dict[str, str] = {}
    for _, tree in parsed:
        global_constants.update(_module_string_constants(tree))
    details: list[BackendDetail] = []
    for path, tree in parsed:
        details += _details_raised(_Module(path, tree, global_constants), tree)
    return details


BACKEND_DETAILS = _backend_details()


# ── the contract ──────────────────────────────────────────────────────────────


def test_the_parsers_find_the_mappings_and_the_backend_details():
    """Guards the guard: a parser that stops finding anything would pass vacuously."""
    sources = {pattern.source.split(".")[0] for pattern in FRONTEND_PATTERNS}
    assert {"API_DETAIL_KEYS", "SERVER_STREAM_ERRORS"} <= sources
    assert any(pattern.status == 403 and pattern.source.endswith("detail test") for pattern in FRONTEND_PATTERNS)
    assert len(BACKEND_DETAILS) > 50
    # A detail built by a helper from a module constant (auth_mixin._reauth_required).
    assert any(d.status == 403 and d.text.startswith("reauth_required: ") for d in BACKEND_DETAILS)


@pytest.mark.parametrize("pattern", FRONTEND_PATTERNS, ids=lambda pattern: pattern.id)
def test_every_frontend_detail_pattern_matches_a_detail_the_backend_raises(pattern):
    candidates = [d for d in BACKEND_DETAILS if pattern.status is None or d.status == pattern.status]
    matches = [d for d in candidates if pattern.regex.search(d.text)]
    assert matches, (
        f"{pattern.source} expects a {pattern.status or 'stream'} detail matching /{pattern.regex.pattern}/, "
        "but no raise in src/ produces one: the backend wording changed, so update the web "
        "mapping (web/src/lib/api.ts or stream.ts) together with it. Backend details with this "
        "status: " + "; ".join(sorted({repr(d.text) for d in candidates}))
    )


# The admin requeue refusals of a dead-letter finalize job (job_queue_mixin), which the admin
# panel explains in the admin's language. Pinned here as well, so their wording is checked
# whether or not the web mapping has landed yet.
ADMIN_FINALIZE_REQUEUE_DETAILS = [
    "Only the finalize job of a failed research can be requeued",
    "A newer finalize job has superseded this one",
    "Finalize job state changed. Please retry.",
]


@pytest.mark.parametrize("text", ADMIN_FINALIZE_REQUEUE_DETAILS)
def test_the_admin_finalize_requeue_refusals_are_raised_as_409(text):
    assert [d for d in BACKEND_DETAILS if d.status == 409 and d.text == text], text


@pytest.mark.parametrize("text", ADMIN_FINALIZE_REQUEUE_DETAILS)
def test_a_web_mapping_of_a_requeue_refusal_matches_its_text(text):
    mapped = [p for p in FRONTEND_PATTERNS if p.status == 409 and p.regex.search(text)]
    if not mapped:
        pytest.skip("the web UI does not map this detail yet")
    assert all(p.source.startswith("API_DETAIL_KEYS.") for p in mapped)
