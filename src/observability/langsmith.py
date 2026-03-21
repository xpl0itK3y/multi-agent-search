import os
from typing import Any, Callable

from src.config import settings

try:
    from langsmith import traceable as _traceable
except ImportError:
    _traceable = None

try:
    from langsmith.wrappers import wrap_openai as _wrap_openai
except ImportError:
    _wrap_openai = None


def configure_langsmith_environment() -> bool:
    if not settings.langsmith_tracing:
        return False
    if not settings.langsmith_api_key:
        return False

    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGSMITH_API_KEY"] = settings.langsmith_api_key
    os.environ["LANGSMITH_ENDPOINT"] = settings.langsmith_endpoint
    if settings.langsmith_project:
        os.environ["LANGSMITH_PROJECT"] = settings.langsmith_project
    return True


def maybe_traceable(*, name: str, run_type: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        if _traceable is None or not configure_langsmith_environment():
            return func
        return _traceable(name=name, run_type=run_type)(func)

    return decorator


def maybe_wrap_openai_client(client: Any) -> Any:
    if _wrap_openai is None or not configure_langsmith_environment():
        return client
    return _wrap_openai(client)
