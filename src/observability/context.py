from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


_OBSERVABILITY_CONTEXT: ContextVar[dict[str, str]] = ContextVar(
    "observability_context",
    default={},
)


def get_observability_context() -> dict[str, str]:
    return dict(_OBSERVABILITY_CONTEXT.get())


@contextmanager
def bind_observability_context(**values: object) -> Iterator[dict[str, str]]:
    current = get_observability_context()
    for key, value in values.items():
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            current[key] = normalized
    token = _OBSERVABILITY_CONTEXT.set(current)
    try:
        yield current
    finally:
        _OBSERVABILITY_CONTEXT.reset(token)
