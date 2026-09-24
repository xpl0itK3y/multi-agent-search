import logging
import json
import re
from datetime import datetime, timezone

from src.config import settings
from src.observability.context import get_observability_context

# Public share links carry their bearer token in the URL path, and access logs are
# shipped to Loki. `/r/<token>` is the SPA share route, so it only counts at the start
# of a path (or right after a URL's host), not as an arbitrary `/r/` segment.
_SHARE_TOKEN_PATH_RE = re.compile(
    r"(?P<prefix>(?:^|[\s\"']|://[^/\s\"']+)(?:/v1/public/research|/r)/)[^/?#\s\"']+"
)
REDACTED_SHARE_TOKEN = "[redacted]"


class ObservabilityContextFilter(logging.Filter):
    DEFAULTS = {
        "request_id": "-",
        "worker_name": "-",
        "research_id": "-",
        "task_id": "-",
        "job_id": "-",
    }

    def filter(self, record: logging.LogRecord) -> bool:
        context = get_observability_context()
        for key, default in self.DEFAULTS.items():
            setattr(record, key, context.get(key, default))
        return True


def redact_share_tokens(text: str) -> str:
    return _SHARE_TOKEN_PATH_RE.sub(lambda match: match.group("prefix") + REDACTED_SHARE_TOKEN, text)


class ShareTokenRedactionFilter(logging.Filter):
    """Strip public share tokens from request lines before any handler sees them.

    uvicorn logs ``'%s - "%s %s HTTP/%s" %d'`` with the path as a separate argument and
    its AccessFormatter unpacks ``record.args`` positionally, so the arguments are
    redacted in place instead of pre-formatting the message.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_share_tokens(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(
                redact_share_tokens(arg) if isinstance(arg, str) else arg for arg in record.args
            )
        return True


def install_access_log_redaction() -> None:
    # A logger-level filter (not a handler one) survives uvicorn's dictConfig, which
    # replaces handlers but keeps logger filters, and covers every uvicorn worker
    # process because each runs the app lifespan that calls configure_logging().
    access_logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(existing, ShareTokenRedactionFilter) for existing in access_logger.filters):
        access_logger.addFilter(ShareTokenRedactionFilter())


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
            "worker_name": getattr(record, "worker_name", "-"),
            "research_id": getattr(record, "research_id", "-"),
            "task_id": getattr(record, "task_id", "-"),
            "job_id": getattr(record, "job_id", "-"),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format=(
                "%(asctime)s %(levelname)s [%(name)s] "
                "request_id=%(request_id)s worker=%(worker_name)s "
                "research_id=%(research_id)s task_id=%(task_id)s job_id=%(job_id)s "
                "%(message)s"
            ),
        )

    for handler in root_logger.handlers:
        if not any(isinstance(existing, ObservabilityContextFilter) for existing in handler.filters):
            handler.addFilter(ObservabilityContextFilter())
        if settings.log_format.strip().lower() == "json":
            handler.setFormatter(JsonLogFormatter())

    install_access_log_redaction()
