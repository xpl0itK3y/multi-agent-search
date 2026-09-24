"""Public share tokens must not reach stdout (and so Loki) through uvicorn's access log."""

import io
import logging

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
    ],
)
def test_redact_share_tokens(line, expected):
    assert redact_share_tokens(line) == expected


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
