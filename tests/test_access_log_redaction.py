"""Public share tokens must not reach stdout (and so Loki) through uvicorn's access log."""

import io
import logging
import time

import pytest
from uvicorn.logging import AccessFormatter

from src.api.app import create_app
from src.observability.logging import (
    ShareTokenRedactionFilter,
    configure_logging,
    install_access_log_redaction,
    redact_share_tokens,
)

TOKEN = "SECRETSHARETOKEN1234567890abcdef"
# The exact call uvicorn's h11/httptools protocols make for every response.
UVICORN_ACCESS_FORMAT = '%s - "%s %s HTTP/%s" %d'


@pytest.fixture
def access_logger():
    """uvicorn.access with a capturing handler that uses uvicorn's own formatter."""
    logger = logging.getLogger("uvicorn.access")
    saved = (logger.handlers[:], logger.filters[:], logger.level, logger.propagate)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(AccessFormatter('%(client_addr)s - "%(request_line)s" %(status_code)s', use_colors=False))
    logger.handlers = [handler]
    logger.filters = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield logger, stream
    logger.handlers, logger.filters, level, logger.propagate = saved
    logger.setLevel(level)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (f"/v1/public/research/{TOKEN}", "/v1/public/research/[redacted]"),
        (f"/v1/public/research/{TOKEN}?format=md", "/v1/public/research/[redacted]?format=md"),
        (f"/v1/public/research/{TOKEN}/export", "/v1/public/research/[redacted]/export"),
        (f"/r/{TOKEN}", "/r/[redacted]"),
        (f'1.2.3.4 - "GET /r/{TOKEN} HTTP/1.1" 200', '1.2.3.4 - "GET /r/[redacted] HTTP/1.1" 200'),
        (f"https://example.com/r/{TOKEN}", "https://example.com/r/[redacted]"),
        ("/v1/research/abc-123", "/v1/research/abc-123"),
        ("/v1/research/abc/r/keep", "/v1/research/abc/r/keep"),
        ("/v1/public/research/", "/v1/public/research/"),
        # The client chooses the spelling and uvicorn logs it verbatim, routed or not.
        (f"//v1/public/research/{TOKEN}", "//v1/public/research/[redacted]"),
        (f"/v1//public/research/{TOKEN}", "/v1//public/research/[redacted]"),
        (f"/v1/public/research//{TOKEN}?x=1", "/v1/public/research//[redacted]?x=1"),
        (f"/V1/Public/RESEARCH/{TOKEN}", "/V1/Public/RESEARCH/[redacted]"),
        (f"/v1/public/research%2F{TOKEN}", "/v1/public/research%2F[redacted]"),
        (f"/v1%2fpublic%2Fresearch%2f{TOKEN}", "/v1%2fpublic%2Fresearch%2f[redacted]"),
        (f"/v1/public/research%252F{TOKEN}", "/v1/public/research%252F[redacted]"),
        (f"path=/v1/public/research/{TOKEN}", "path=/v1/public/research/[redacted]"),
        (f"/prefix/v1/public/research/{TOKEN}", "/prefix/v1/public/research/[redacted]"),
        (f'{{"path": "/v1/public/research/{TOKEN}"}}', '{"path": "/v1/public/research/[redacted]"}'),
        (f"//r/{TOKEN}", "//r/[redacted]"),
        (f"/R/{TOKEN}", "/R/[redacted]"),
        (f"%2Fr%2F{TOKEN}", "%2Fr%2F[redacted]"),
        (f"path=/r/{TOKEN}&x=1", "path=/r/[redacted]&x=1"),
        (f"HTTPS://Example.com//r/{TOKEN}", "HTTPS://Example.com//r/[redacted]"),
        (f"https://h.example%2fr%2f{TOKEN}", "https://h.example%2fr%2f[redacted]"),
        (f"///v1/public/research/{TOKEN}", "///v1/public/research/[redacted]"),
        (f"%2f%2F%252fv1/public/research/{TOKEN}", "%2f%2F%252fv1/public/research/[redacted]"),
    ],
)
def test_redact_share_tokens(line, expected):
    assert redact_share_tokens(line) == expected


# Far past any real request line (nginx and uvicorn cap one near 8 KB): a pattern that
# backtracks through a run of separators from every start in it takes about a minute on
# this, a linear one a few milliseconds.
ADVERSARIAL_LENGTH = 64 * 1024
REDACTION_BUDGET_SECONDS = 0.05


def _redaction_seconds(line: str) -> float:
    """Best of three runs, to ride out a noisy machine; a run far over the budget is not
    noise and is not repeated."""
    best = float("inf")
    for _ in range(3):
        started = time.perf_counter()
        redact_share_tokens(line)
        best = min(best, time.perf_counter() - started)
        if best < REDACTION_BUDGET_SECONDS or best > 20 * REDACTION_BUDGET_SECONDS:
            break
    return best


@pytest.mark.parametrize("separator", ["/", "%2f", "%2F", "%252f"])
@pytest.mark.parametrize(
    "head",
    ["", "/v1", "/v1/public", "/v1/public/research", "/x?a=", "/x?redirect=", "https://h.example", "/r"],
)
def test_redaction_stays_linear_on_a_run_of_separators(head, separator):
    """SEC3-1: the filter runs on the event loop for every access-log line, and the client
    picks the path, so one request must not be able to stall the worker."""
    line = head + separator * (ADVERSARIAL_LENGTH // len(separator))

    assert _redaction_seconds(line) < REDACTION_BUDGET_SECONDS


def test_database_errors_do_not_carry_bound_parameters():
    """SQLAlchemy puts bound values in its error text, which reaches logs and job errors:
    share tokens, password hashes, prompts. The app's engines hide them."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import OperationalError

    from src.db.session import _engine_kwargs

    engine = create_engine("sqlite://", **_engine_kwargs("sqlite://"))
    with pytest.raises(OperationalError) as exc, engine.connect() as conn:
        conn.execute(text("SELECT * FROM researches WHERE share_token = :token"), {"token": TOKEN})

    assert TOKEN not in str(exc.value)
    assert "hidden" in str(exc.value)


def test_uvicorn_access_line_is_redacted(access_logger):
    logger, stream = access_logger
    install_access_log_redaction()

    logger.info(UVICORN_ACCESS_FORMAT, "127.0.0.1:61506", "GET", f"/v1/public/research/{TOKEN}", "1.1", 200)
    logger.info(UVICORN_ACCESS_FORMAT, "127.0.0.1:61506", "GET", "/v1/research/abc-123?limit=5", "1.1", 200)

    output = stream.getvalue()
    assert TOKEN not in output
    assert '"GET /v1/public/research/[redacted] HTTP/1.1" 200' in output
    # Other request lines are left alone, including the query string.
    assert '"GET /v1/research/abc-123?limit=5 HTTP/1.1" 200' in output


def test_configure_logging_installs_the_filter_once(access_logger):
    logger, _ = access_logger
    configure_logging()
    configure_logging()
    assert sum(isinstance(item, ShareTokenRedactionFilter) for item in logger.filters) == 1


@pytest.mark.anyio
async def test_api_lifespan_installs_the_filter(access_logger):
    # uvicorn configures its loggers before importing the app, so the app's lifespan is
    # the place that must attach the filter, in every worker process.
    logger, stream = access_logger
    app = create_app()
    async with app.router.lifespan_context(app):
        logger.info(UVICORN_ACCESS_FORMAT, "127.0.0.1:1", "GET", f"/r/{TOKEN}", "1.1", 200)
    assert TOKEN not in stream.getvalue()
    assert "/r/[redacted]" in stream.getvalue()
