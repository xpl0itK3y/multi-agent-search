"""Grafana dashboards read per-process metrics only from the single-process workers.

The API runs `uvicorn --workers N` without prometheus_client multiprocess mode, so
each scrape of api:8000 reads one process's registry at random. On a counter every
switch between processes looks like a reset worth that process's whole total, so
rate()/increase() over the API series report LLM spend that never happened; the
API's copy of the queue gauges is refreshed ad hoc per process and can stay stale
forever. Panels over these metrics must select the worker scrape job.
"""
import json
import re
from pathlib import Path

import pytest

DASHBOARD_DIR = Path(__file__).resolve().parents[1] / "ops" / "grafana" / "provisioning" / "dashboards"
WORKER_JOB = 'job="multi-agent-search-workers"'
# Metrics whose API-side series are wrong under multi-process uvicorn.
WORKER_ONLY_METRICS = ("mas_llm_cost_usd_total", "mas_queue_jobs", "mas_queue_backlog")


def _panels(node):
    for panel in node.get("panels", []):
        yield panel
        yield from _panels(panel)


def _dashboard_exprs():
    for path in sorted(DASHBOARD_DIR.glob("*.json")):
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for panel in _panels(dashboard):
            for target in panel.get("targets", []):
                expr = target.get("expr")
                if expr:
                    yield path.name, panel.get("title", ""), expr


def _selectors(expr: str, metric: str) -> list[str]:
    """Every `metric{...}` / bare `metric` occurrence, as its label matcher text."""
    return [
        match.group(1) or ""
        for match in re.finditer(rf"\b{re.escape(metric)}\b(?!_)(\{{[^}}]*\}})?", expr)
    ]


def test_dashboards_have_queries_for_every_worker_only_metric():
    exprs = " ".join(expr for _, _, expr in _dashboard_exprs())
    for metric in WORKER_ONLY_METRICS:
        assert metric in exprs, f"no dashboard panel queries {metric}; update WORKER_ONLY_METRICS"


@pytest.mark.parametrize("metric", WORKER_ONLY_METRICS)
def test_worker_only_metrics_select_the_worker_scrape_job(metric):
    offenders = [
        f"{name}: {title!r}: {expr}"
        for name, title, expr in _dashboard_exprs()
        for selector in _selectors(expr, metric)
        if WORKER_JOB not in selector
    ]
    assert not offenders, "panels reading the multi-process API series:\n" + "\n".join(offenders)


def test_llm_cost_panels_say_they_are_worker_only():
    titles = [title for _, title, expr in _dashboard_exprs() if "mas_llm_cost_usd_total" in expr]
    assert titles
    assert all("worker" in title.lower() for title in titles), titles
