"""Catalog of user-selectable LLM models.

The end user can pick which model powers a research run (composer selector,
frontend §4.2). This module is the single source of truth for which model ids
are allowed — never trust a raw model id from the client; validate against
``is_selectable`` / ``resolve_model_id`` here.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ModelOption:
    id: str
    label: str
    description: str
    tier: str            # "pro" | "flash"
    reasoning: bool      # exposes `reasoning_content` (powers the "thinking" stream, frontend §4.1)
    default: bool = False


# Order here = display order in the composer selector.
MODEL_CATALOG: tuple[ModelOption, ...] = (
    ModelOption(
        id="deepseek-v4-pro",
        label="V4 Pro",
        description="Глубже и точнее. Дороже и медленнее — для серьёзного ресёрча.",
        tier="pro",
        reasoning=True,   # ASSUMED — verify v4-pro actually exposes reasoning_content
        default=True,
    ),
    ModelOption(
        id="deepseek-v4-flash",
        label="V4 Flash",
        description="Быстрее и дешевле. Для лёгких задач и быстрых прогонов.",
        tier="flash",
        reasoning=False,
    ),
)

_BY_ID: dict[str, ModelOption] = {option.id: option for option in MODEL_CATALOG}
DEFAULT_MODEL_ID: str = next((o.id for o in MODEL_CATALOG if o.default), MODEL_CATALOG[0].id)


def list_models() -> list[dict]:
    """Serializable catalog for the `GET /v1/models` endpoint."""
    return [asdict(option) for option in MODEL_CATALOG]


def get_model(model_id: str | None) -> ModelOption | None:
    return _BY_ID.get(model_id or "")


def is_selectable(model_id: str | None) -> bool:
    return model_id in _BY_ID


def resolve_model_id(model_id: str | None, fallback: str) -> str:
    """Return ``model_id`` only if it's an allowed selectable model, else ``fallback``.

    Guards against arbitrary/unsafe model ids coming from the client.
    """
    return model_id if is_selectable(model_id) else fallback
