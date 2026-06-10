# Eval Harness (0.5)

Offline, dependency-light way to measure research quality and catch regressions.
Lives in [`eval/`](../eval). No new third-party deps — stdlib + the project schemas.

The harness is decoupled from *how* a research is produced: everything is reduced
to a normalized `EvalSample`, and every metric is a pure function over that shape.
Samples come from one of two runners:

- **fixtures** — saved `{id}.json` snapshots in [`eval/fixtures/`](../eval/fixtures).
  Deterministic, no network/keys. This is what CI and the commands below use.
- **live** — drive the real `ResearchService` and read the result back (needs keys
  + workers). Wired by a small local script; see "Live runs" below.

## Metrics

All metrics aggregate as a mean across the gold set, skipping `nan`
("not applicable for this sample", e.g. coverage with no plan, latency on a fixture).

| Metric | Meaning | Better |
|---|---|---|
| `unique_sources` | distinct source URLs gathered | ↑ |
| `unique_domains` | distinct domains (diversity) | ↑ |
| `high_quality_rate` | share of sources graded high/medium | ↑ |
| `citation_count` | `[Sn]` markers in the report | ↑ |
| `cited_sources` | distinct sources actually cited | ↑ |
| `citation_density` | citations per 1000 words (grounding intensity) | ↑ |
| `plan_coverage` | share of plan sub-questions reflected in the report | ↑ |
| `must_mention_rate` | share of gold `must_mention` terms surfaced | ↑ |
| `report_words` | report length (prose only, citations stripped) | ↑ |
| `cost_usd` | estimated LLM cost | ↓ |
| `total_tokens` | prompt + completion tokens | ↓ |
| `latency_seconds` | wall-clock per query (live only) | ↓ |

Metric definitions: [`eval/metrics.py`](../eval/metrics.py). They are intentionally
heuristic (token overlap, substring presence) so the harness stays runnable without
an LLM judge; treat the numbers as a **relative** signal vs the baseline, not ground truth.

## Commands

```bash
# Offline run over the gold set (what CI runs) — prints a table + baseline deltas
python -m eval --fixtures eval/fixtures

# Fail (exit 1) if any metric regressed beyond tolerance — use in CI
python -m eval --fixtures eval/fixtures --gate

# Refresh the committed baseline after an intentional improvement
python -m eval --fixtures eval/fixtures --update-baseline

# Machine-readable output
python -m eval --fixtures eval/fixtures --json
```

The committed baseline is [`eval/baseline.json`](../eval/baseline.json). The default
regression tolerance is 5% relative (`--tolerance`).

## Adding a gold query

1. Append a line to [`eval/datasets/gold.jsonl`](../eval/datasets/gold.jsonl):
   ```json
   {"id": "my_topic", "prompt": "…", "depth": "medium", "domain": "…", "must_mention": ["term a", "term b"]}
   ```
   `must_mention` should list specific terms a good report is expected to surface
   (drives `must_mention_rate`).
2. For the offline path, add a matching `eval/fixtures/my_topic.json` snapshot
   (fields mirror `EvalSample`: `report`, `plan_items`, `sources`, `token_usage`).
   The easiest way to seed one is to capture a real run via the live runner and
   save the resulting sample.
3. Re-run `python -m eval --fixtures eval/fixtures --update-baseline`.

## Live runs

A live run executes the real pipeline per query and measures the output. Because
driving a research to completion depends on the deployment (workers, finalize job),
the harness owns *measurement* and you supply *orchestration* via a
`produce(query) -> research_id` callable:

```python
from eval.runners import live_runner, load_dataset, run_dataset
from eval.metrics import compute_metrics
from eval.scoring import aggregate, format_summary

service = ...  # a ResearchService wired to real agents + LLM

def produce(query):
    rid = service.start_research(...)        # kick off
    wait_until_completed(rid)                # your deployment's wait
    service.complete_research_finalization(rid)
    return rid

runner = live_runner(service, produce)
queries = load_dataset("eval/datasets/gold.jsonl")
per_sample = [compute_metrics(s, q) for q, s in run_dataset(queries, runner)]
print(format_summary(aggregate(per_sample)))
```

Use live runs to (a) seed fixtures and (b) compare the current pipeline against a
saved baseline or against an external reference (e.g. Gemini Deep Research) on the
same gold prompts.
