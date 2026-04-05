import logging
import json
from datetime import datetime, timezone

from src.config import settings
from src.observability.context import get_observability_context


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
