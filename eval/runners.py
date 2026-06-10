"""Dataset loading and the two ways to produce samples: fixtures and live."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .sample import EvalSample

# A runner turns one gold query (dict) into an EvalSample.
Runner = Callable[[dict[str, Any]], EvalSample]


def load_dataset(path: str | Path) -> list[dict[str, Any]]:
    """Load a JSONL gold set. Blank lines and ``#`` comment lines are ignored."""
    queries: list[dict[str, Any]] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        queries.append(json.loads(line))
    return queries


def fixture_runner(fixtures_dir: str | Path) -> Runner:
    """Runner that loads a pre-saved ``{id}.json`` sample — deterministic, no network.

    Used by CI and the offline demo so the harness can be exercised end-to-end
    without API keys.
    """
    base = Path(fixtures_dir)

    def run(query: dict[str, Any]) -> EvalSample:
        data = json.loads((base / f"{query['id']}.json").read_text(encoding="utf-8"))
        data.setdefault("query_id", query["id"])
        data.setdefault("prompt", query.get("prompt", ""))
        return EvalSample.from_dict(data)

    return run


def live_runner(
    service: Any,
    produce: Callable[[dict[str, Any]], str],
) -> Runner:
    """Runner that executes a real research and reads it back from the service.

    ``produce(query) -> research_id`` is supplied by the caller (a local script)
    because *driving* a research to completion depends on the deployment (workers,
    finalize job). The harness only owns measurement, not orchestration. Wall-clock
    latency is captured around ``produce``.
    """

    def run(query: dict[str, Any]) -> EvalSample:
        started = time.perf_counter()
        research_id = produce(query)
        latency = time.perf_counter() - started
        return EvalSample.from_service(
            service,
            query_id=query["id"],
            prompt=query.get("prompt", ""),
            research_id=research_id,
            latency_seconds=latency,
        )

    return run


def run_dataset(
    queries: list[dict[str, Any]],
    runner: Runner,
    *,
    on_sample: Optional[Callable[[dict[str, Any], EvalSample], None]] = None,
) -> list[tuple[dict[str, Any], EvalSample]]:
    """Run every query through ``runner``, returning (query, sample) pairs."""
    results: list[tuple[dict[str, Any], EvalSample]] = []
    for query in queries:
        sample = runner(query)
        if on_sample is not None:
            on_sample(query, sample)
        results.append((query, sample))
    return results
