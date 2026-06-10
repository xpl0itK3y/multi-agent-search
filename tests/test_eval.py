"""Deterministic tests for the offline eval harness (no network, no LLM)."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from eval import EvalSample, aggregate, compare_to_baseline, compute_metrics
from eval.metrics import METRICS
from eval.runners import fixture_runner, load_dataset, run_dataset

_ROOT = Path(__file__).resolve().parent.parent / "eval"


@pytest.fixture
def crafted() -> EvalSample:
    return EvalSample(
        query_id="t",
        prompt="p",
        depth="easy",
        report="Alpha beta [S1]. Gamma delta [S2][S1].",
        plan_items=[
            {"description": "alpha beta gamma", "queries": []},
            {"description": "zzzz wwww qqqq", "queries": []},
        ],
        sources=[
            {"url": "https://a.com/x", "domain": "a.com", "source_quality": "high"},
            {"url": "https://a.com/y", "domain": "a.com", "source_quality": "low"},
            {"url": "https://b.com/z", "domain": None, "source_quality": "medium"},
        ],
        token_usage={"prompt_tokens": 100, "completion_tokens": 50, "estimated_cost_usd": 0.01},
        latency_seconds=12.0,
    )


def test_metric_values_are_exact(crafted: EvalSample):
    q = {"must_mention": ["alpha beta", "nope nope"]}
    m = compute_metrics(crafted, q)
    assert m["unique_sources"] == 3.0
    assert m["unique_domains"] == 2.0  # a.com (domain) + b.com (derived from url)
    assert m["high_quality_rate"] == pytest.approx(2 / 3, abs=1e-4)  # high+medium / 3
    assert m["citation_count"] == 3.0
    assert m["cited_sources"] == 2.0  # S1, S2
    assert m["report_words"] == 4.0  # citations stripped before counting
    assert m["citation_density"] == pytest.approx(750.0, abs=1e-2)  # 3 / 4 * 1000
    assert m["plan_coverage"] == 0.5  # item1 covered, item2 not
    assert m["must_mention_rate"] == 0.5  # 1 of 2 phrases present
    assert m["cost_usd"] == pytest.approx(0.01)
    assert m["total_tokens"] == 150.0
    assert m["latency_seconds"] == 12.0


def test_compute_metrics_covers_registry(crafted: EvalSample):
    m = compute_metrics(crafted, {})
    assert set(m) == {metric.name for metric in METRICS}
    # must_mention not applicable -> nan (no gold phrases supplied)
    assert math.isnan(m["must_mention_rate"])


def test_not_applicable_metrics_are_nan():
    empty = EvalSample(query_id="e", prompt="", report="", plan_items=[], sources=[])
    m = compute_metrics(empty, {})
    assert math.isnan(m["plan_coverage"])      # no plan
    assert math.isnan(m["high_quality_rate"])  # no graded sources
    assert math.isnan(m["citation_density"])   # no words
    assert m["unique_sources"] == 0.0


def test_aggregate_skips_nan():
    rows = [
        {"unique_sources": 4.0, "plan_coverage": 1.0},
        {"unique_sources": 6.0, "plan_coverage": math.nan},
    ]
    agg = aggregate(rows)
    assert agg["n_samples"] == 2
    assert agg["unique_sources"] == 5.0
    assert agg["plan_coverage"] == 1.0  # nan ignored, not averaged as 0


def test_compare_to_baseline_flags_both_directions():
    baseline = {"unique_sources": 10.0, "cost_usd": 0.05}
    # unique_sources dropped 30% (worse, higher-is-better); cost rose 40% (worse, lower-is-better)
    current = {"unique_sources": 7.0, "cost_usd": 0.07}
    regs = {r.metric for r in compare_to_baseline(current, baseline, tolerance=0.05)}
    assert regs == {"unique_sources", "cost_usd"}


def test_compare_to_baseline_within_tolerance_is_clean():
    baseline = {"unique_sources": 10.0, "cost_usd": 0.05}
    current = {"unique_sources": 9.7, "cost_usd": 0.052}  # within 5%
    assert compare_to_baseline(current, baseline, tolerance=0.05) == []


def test_compare_to_baseline_skips_nan_and_missing():
    baseline = {"unique_sources": 10.0, "latency_seconds": float("nan")}
    current = {"unique_sources": float("nan"), "latency_seconds": 99.0}
    assert compare_to_baseline(current, baseline) == []


def test_sample_roundtrip(crafted: EvalSample):
    restored = EvalSample.from_dict(json.loads(json.dumps(crafted.to_dict())))
    assert restored == crafted


def test_load_dataset_ignores_comments_and_blanks():
    queries = load_dataset(_ROOT / "datasets" / "gold.jsonl")
    assert len(queries) == 6
    assert all("id" in q and "prompt" in q for q in queries)


def test_fixture_run_over_gold_set_is_finite():
    queries = load_dataset(_ROOT / "datasets" / "gold.jsonl")
    runner = fixture_runner(_ROOT / "fixtures")
    pairs = run_dataset(queries, runner)
    assert len(pairs) == len(queries)
    per_sample = [compute_metrics(sample, query) for query, sample in pairs]
    agg = aggregate(per_sample)
    # Core quality/grounding metrics must be present and finite across the set.
    for key in ("unique_sources", "citation_count", "plan_coverage", "must_mention_rate", "cost_usd"):
        assert not math.isnan(agg[key]), key
    assert agg["unique_sources"] >= 5.0  # gold fixtures are reasonably sourced


def test_baseline_file_has_no_regressions_against_itself():
    baseline_path = _ROOT / "baseline.json"
    if not baseline_path.exists():
        pytest.skip("baseline not generated yet")
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    queries = load_dataset(_ROOT / "datasets" / "gold.jsonl")
    runner = fixture_runner(_ROOT / "fixtures")
    per_sample = [compute_metrics(s, q) for q, s in run_dataset(queries, runner)]
    current = aggregate(per_sample)
    assert compare_to_baseline(current, baseline) == []
