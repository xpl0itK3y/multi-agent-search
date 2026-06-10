"""Aggregate per-sample metrics, diff against a saved baseline, format tables."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .metrics import METRICS, METRICS_BY_NAME


def _mean_ignoring_nan(values: list[float]) -> float:
    real = [v for v in values if v is not None and not math.isnan(v)]
    if not real:
        return math.nan
    return sum(real) / len(real)


def aggregate(per_sample: list[dict[str, float]]) -> dict[str, Any]:
    """Mean of each metric across samples (skipping ``nan``), plus sample count."""
    summary: dict[str, Any] = {"n_samples": len(per_sample)}
    for m in METRICS:
        col = [row.get(m.name, math.nan) for row in per_sample]
        summary[m.name] = _mean_ignoring_nan(col)
    return summary


@dataclass(frozen=True)
class Regression:
    metric: str
    baseline: float
    current: float
    higher_is_better: bool

    @property
    def delta(self) -> float:
        return self.current - self.baseline

    def __str__(self) -> str:
        arrow = "↓" if self.delta < 0 else "↑"
        return (
            f"{self.metric}: {self.baseline:.4g} → {self.current:.4g} "
            f"({arrow}{abs(self.delta):.4g})"
        )


def compare_to_baseline(
    current: dict[str, Any],
    baseline: dict[str, Any],
    *,
    tolerance: float = 0.05,
) -> list[Regression]:
    """Flag metrics that moved in the *worse* direction beyond a relative tolerance.

    ``tolerance`` is fractional (0.05 = 5%). A metric is a regression when, for a
    higher-is-better metric, it dropped below ``baseline * (1 - tolerance)``; for
    a lower-is-better metric, when it rose above ``baseline * (1 + tolerance)``.
    Metrics missing or ``nan`` on either side are skipped.
    """
    regressions: list[Regression] = []
    for name, metric in METRICS_BY_NAME.items():
        base = baseline.get(name)
        cur = current.get(name)
        if base is None or cur is None:
            continue
        if isinstance(base, float) and math.isnan(base):
            continue
        if isinstance(cur, float) and math.isnan(cur):
            continue
        if metric.higher_is_better:
            threshold = base * (1 - tolerance)
            worse = cur < threshold
        else:
            # For near-zero baselines allow a small absolute slack so noise
            # in cost/latency doesn't trip the gate.
            threshold = base * (1 + tolerance) + 1e-9
            worse = cur > threshold
        if worse:
            regressions.append(
                Regression(name, float(base), float(cur), metric.higher_is_better)
            )
    return regressions


def _fmt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        if math.isnan(value):
            return "—"
        return f"{value:.4g}"
    return str(value)


def format_summary(current: dict[str, Any], baseline: dict[str, Any] | None = None) -> str:
    """Render the aggregate metrics as a table, with baseline deltas if provided."""
    width = max(len(m.name) for m in METRICS)
    lines = [f"samples: {current.get('n_samples', 0)}", ""]
    header = f"{'metric'.ljust(width)}  {'current':>10}"
    if baseline:
        header += f"  {'baseline':>10}  {'delta':>10}"
    lines.append(header)
    lines.append("-" * len(header))
    for m in METRICS:
        cur = current.get(m.name)
        row = f"{m.name.ljust(width)}  {_fmt(cur):>10}"
        if baseline:
            base = baseline.get(m.name)
            if (
                isinstance(cur, (int, float))
                and isinstance(base, (int, float))
                and not (math.isnan(float(cur)) or math.isnan(float(base)))
            ):
                delta = float(cur) - float(base)
                row += f"  {_fmt(base):>10}  {('+' if delta >= 0 else '') + f'{delta:.4g}':>10}"
            else:
                row += f"  {_fmt(base):>10}  {'—':>10}"
        unit = f"  ({m.unit}, {'↑' if m.higher_is_better else '↓'} better)" if m.unit else f"  ({'↑' if m.higher_is_better else '↓'} better)"
        lines.append(row + unit)
    return "\n".join(lines)
