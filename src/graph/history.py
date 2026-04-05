from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.config import settings


def _parse_event_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def compact_graph_step_events(
    existing_events: list[dict] | None,
    incoming_events: list[dict] | None,
) -> list[dict]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=max(settings.graph_step_event_retention_seconds, 0))
    merged: list[dict] = []
    seen: set[tuple] = set()

    for event in [*(existing_events or []), *(incoming_events or [])]:
        if not isinstance(event, dict):
            continue
        timestamp = _parse_event_timestamp(event.get("timestamp"))
        if timestamp is None:
            continue
        if timestamp < cutoff:
            continue
        normalized = {
            **event,
            "timestamp": timestamp.isoformat(),
        }
        dedupe_key = (
            normalized.get("timestamp"),
            normalized.get("step"),
            round(float(normalized.get("elapsed_ms") or 0.0), 2),
            bool(normalized.get("failed")),
            normalized.get("research_id"),
            normalized.get("worker_name"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(normalized)

    merged.sort(key=lambda item: item["timestamp"])
    limit = max(settings.graph_step_event_history_limit, 0)
    if limit > 0:
        merged = merged[-limit:]
    return merged


def compact_graph_trail(
    existing_events: list[dict] | None,
    incoming_events: list[dict] | None,
) -> list[dict]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=max(settings.graph_trail_retention_seconds, 0))
    merged: list[dict] = []
    seen: set[tuple] = set()

    for event in [*(existing_events or []), *(incoming_events or [])]:
        if not isinstance(event, dict):
            continue
        timestamp = _parse_event_timestamp(event.get("timestamp"))
        normalized = dict(event)
        if timestamp is not None:
            if timestamp < cutoff:
                continue
            normalized["timestamp"] = timestamp.isoformat()
        dedupe_key = (
            normalized.get("timestamp"),
            normalized.get("step"),
            normalized.get("detail"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(normalized)

    merged.sort(key=lambda item: str(item.get("timestamp") or ""))
    limit = max(settings.graph_trail_history_limit, 0)
    if limit > 0:
        merged = merged[-limit:]
    return merged
