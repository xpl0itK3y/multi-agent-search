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
        id="deepseek-flash",
        label="V4.1 Flash",
        description="Быстрее и дешевле. Новейшая архитектура MoE 552B с контекстом 1M токенов.",
        tier="flash",
        reasoning=False,
    ),
    ModelOption(
        id="deepseek-chat",
        label="Chat",
        description="Самый быстрый и дешёвый — обычный чат-режим. Для быстрых ответов и черновиков.",
        tier="flash",
        reasoning=False,
    ),
)

_BY_ID: dict[str, ModelOption] = {option.id: option for option in MODEL_CATALOG}
_ALIASES: dict[str, str] = {
    "deepseek-v4.1-flash": "deepseek-flash",
    "deepseek-v4-flash": "deepseek-flash",
}
DEFAULT_MODEL_ID: str = next((o.id for o in MODEL_CATALOG if o.default), MODEL_CATALOG[0].id)


def list_models() -> list[dict]:
    """Serializable catalog for the `GET /v1/models` endpoint."""
    return [asdict(option) for option in MODEL_CATALOG]


def get_model(model_id: str | None) -> ModelOption | None:
    if not model_id:
        return None
    canonical_id = _ALIASES.get(model_id, model_id)
    return _BY_ID.get(canonical_id)


def is_selectable(model_id: str | None) -> bool:
    if not model_id:
        return False
    canonical_id = _ALIASES.get(model_id, model_id)
    return canonical_id in _BY_ID


def resolve_model_id(model_id: str | None, fallback: str) -> str:
    """Return ``model_id`` only if it's an allowed selectable model, else ``fallback``.

    Guards against arbitrary/unsafe model ids coming from the client.
    """
    if not model_id:
        return fallback
    canonical_id = _ALIASES.get(model_id, model_id)
    return canonical_id if canonical_id in _BY_ID else fallback


def resolve_trusted_model_id(model_id: str, trusted: set[str]) -> str | None:
    """Canonical id for a model the server itself picked, or None when it is not allowed.

    Like ``resolve_model_id`` but also accepts ``trusted`` ids (operator-configured models
    such as the reasoner or repair model, which are not user-selectable). For the provider
    only: client input is validated with ``resolve_model_id`` at the request boundary.
    """
    canonical_id = _ALIASES.get(model_id, model_id)
    if canonical_id in _BY_ID or canonical_id in trusted or model_id in trusted:
        return canonical_id
    return None
