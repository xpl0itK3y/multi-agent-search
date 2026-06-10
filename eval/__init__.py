"""Offline eval harness — measure research quality against a gold query set.

The harness is deliberately decoupled from *how* a research is produced:
everything is reduced to a normalized :class:`~eval.sample.EvalSample`, and all
metrics operate purely on that shape. Samples come either from the live
``ResearchService`` (``--live``) or from saved JSON fixtures (deterministic, used
in CI and offline demos).

Entry point: ``python -m eval`` (see ``eval/__main__.py``).
"""

from .sample import EvalSample
from .metrics import METRICS, compute_metrics
from .scoring import aggregate, compare_to_baseline, Regression

__all__ = [
    "EvalSample",
    "METRICS",
    "compute_metrics",
    "aggregate",
    "compare_to_baseline",
    "Regression",
]
