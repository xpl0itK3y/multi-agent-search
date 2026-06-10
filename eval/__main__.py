"""Run the eval harness from the command line.

Examples
--------
Offline (fixtures, deterministic) — what CI runs::

    python -m eval --fixtures eval/fixtures

Compare against the committed baseline and fail on regressions::

    python -m eval --fixtures eval/fixtures --gate

Refresh the baseline after an intentional improvement::

    python -m eval --fixtures eval/fixtures --update-baseline

A live run drives the real pipeline and is wired by a local script that builds a
``live_runner``; this entry point covers the offline/fixture path that needs no
API keys. See ``docs/eval-harness.md``.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from .metrics import compute_metrics
from .runners import fixture_runner, load_dataset, run_dataset
from .scoring import aggregate, compare_to_baseline, format_summary

_ROOT = Path(__file__).resolve().parent
_DEFAULT_DATASET = _ROOT / "datasets" / "gold.jsonl"
_DEFAULT_BASELINE = _ROOT / "baseline.json"


def _clean(obj: object) -> object:
    """Recursively turn nan floats into ``None`` so we emit strict, valid JSON."""
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    return obj


def _dump_json(obj: object, **kwargs: object) -> str:
    return json.dumps(_clean(obj), allow_nan=False, **kwargs)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m eval", description=__doc__)
    parser.add_argument("--dataset", default=str(_DEFAULT_DATASET), help="JSONL gold query set")
    parser.add_argument("--fixtures", default=str(_ROOT / "fixtures"), help="dir of {id}.json samples")
    parser.add_argument("--baseline", default=str(_DEFAULT_BASELINE), help="baseline metrics JSON")
    parser.add_argument("--update-baseline", action="store_true", help="overwrite the baseline with this run")
    parser.add_argument("--gate", action="store_true", help="exit non-zero if any metric regressed")
    parser.add_argument("--tolerance", type=float, default=0.05, help="relative regression tolerance (default 0.05)")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON instead of a table")
    args = parser.parse_args(argv)

    queries = load_dataset(args.dataset)
    runner = fixture_runner(args.fixtures)
    pairs = run_dataset(queries, runner)
    per_sample = [compute_metrics(sample, query) for query, sample in pairs]
    summary = aggregate(per_sample)

    baseline_path = Path(args.baseline)
    baseline = None
    if baseline_path.exists() and not args.update_baseline:
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

    if args.json:
        print(_dump_json({"summary": summary, "baseline": baseline}, indent=2))
    else:
        print(format_summary(summary, baseline))

    exit_code = 0
    if baseline is not None:
        regressions = compare_to_baseline(summary, baseline, tolerance=args.tolerance)
        if regressions:
            print("\nREGRESSIONS:")
            for reg in regressions:
                print(f"  • {reg}")
            if args.gate:
                exit_code = 1
        else:
            print("\nno regressions vs baseline ✓")

    if args.update_baseline:
        baseline_path.write_text(_dump_json(summary, indent=2) + "\n", encoding="utf-8")
        print(f"\nbaseline written → {baseline_path}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
