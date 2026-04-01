import logging

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
