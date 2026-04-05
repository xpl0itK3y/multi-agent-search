# Multi-Agent Search & Optimizer

FastAPI backend for a research workflow with persistent jobs, Postgres storage, and a separate worker process.

## LangGraph Finalize Loop

The project now includes an optional LangGraph-style finalize orchestration layer in `src/graph/`.

It is used for the post-search decision loop:

- collect source and evidence summaries
- branch into a replan step when coverage looks weak
- run the analyzer
- retry analysis once when the generated draft still contains report-note style quality warnings

By default, `USE_LANGGRAPH_FINALIZE_GRAPH=true`.

If `langgraph` is not installed, the project falls back to an internal sequential runner with the same decisions, so the app still works.

## Observability

The project now supports:

- structured JSON logging with `LOG_FORMAT=json`
- Prometheus metrics at `/metrics`
- Docker Compose services for `prometheus` and `grafana`

Useful flags:

```env
LOG_FORMAT=json
PROMETHEUS_METRICS_ENABLED=true
USE_LANGGRAPH_FINALIZE_GRAPH=true
```

## Requirements

- Python 3.11+
- Docker + Docker Compose
- DeepSeek API key

Minimal `.env`:

```env
DEEPSEEK_API_KEY=your_api_key_here
DEEPSEEK_MODEL=deepseek-chat
TASK_STORE_BACKEND=postgres

POSTGRES_USER=app
POSTGRES_PASSWORD=app
POSTGRES_DB=multi_agent_search
POSTGRES_HOST=localhost
POSTGRES_PORT=5433

FINALIZE_WORKER_INTERVAL=2.0
```

See [.env.example](./.env.example) for a full example.

## Native Text Processing Module

The repository now includes an optional Rust-backed text-processing module in `native/text_processing`.

Python keeps a safe fallback path in `src/core/rust_accel.py`, so the app still runs if the native module is not built.

To build the native module into the active virtualenv:

```bash
venv/bin/python -m pip install maturin
./scripts/build_native_module.sh
```

The script uses:

- `cargo`
- `maturin`
- `native/text_processing/Cargo.toml`

If the native extension is unavailable, the project automatically falls back to the pure-Python implementation.

## Run With Docker Compose

```bash
docker compose up --build
```

This starts:

- `db`
- `migrate`
- `api`
- `worker`
- `worker_2`
- `worker_3`
- `ui`
- `prometheus`
- `grafana`

The API will be available at `http://localhost:8000`.

The Streamlit UI will be available at `http://localhost:8501`.

Prometheus will be available at `http://localhost:9090`.

Grafana will be available at `http://localhost:3000`.

Stop everything:

```bash
docker compose down
```

Stop and remove the database volume:

```bash
docker compose down -v
```

## Run Locally

1. Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

2. Start Postgres:

```bash
docker compose up -d db
```

3. Run migrations:

```bash
python -m alembic upgrade head
```

4. Start the API:

```bash
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

5. In another terminal, start the worker:

```bash
python scripts/run_finalize_worker.py
```

6. Optional: start the Streamlit UI:

```bash
streamlit run src/ui/streamlit_app.py
```

Run the worker once:

```bash
python scripts/run_finalize_worker.py --once
```

## Infra Notes

Implemented in this repository:

- structured JSON logs to stdout
- Prometheus scraping from the API
- Grafana and Prometheus services in Docker Compose
- explicit `langgraph` dependency in `requirements.txt`

Still not implemented yet:

- broker-backed runtime
- external log shipping pipeline
- live production profiling on a real deployment

## Quick Check

Health:

```bash
curl http://localhost:8000/health
```

Prometheus metrics:

```bash
curl http://localhost:8000/metrics
```

Create a research:

```bash
curl -X POST "http://localhost:8000/v1/research" \
  -H "Content-Type: application/json" \
  -d '{"prompt":"research renewable energy trends 2024","depth":"easy"}'
```

Check queue health:

```bash
curl http://localhost:8000/health/queues
```

Check worker heartbeat:

```bash
curl http://localhost:8000/health/workers/job-worker
```

## Queue Admin

List running search jobs:

```bash
curl "http://localhost:8000/v1/search-jobs?status=running"
```

List dead-letter search jobs:

```bash
curl "http://localhost:8000/v1/search-jobs?status=dead_letter"
```

List running finalize jobs:

```bash
curl "http://localhost:8000/v1/research/finalize-jobs?status=running"
```

List dead-letter finalize jobs:

```bash
curl "http://localhost:8000/v1/research/finalize-jobs?status=dead_letter"
```

Requeue a dead-letter search job:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/<job_id>/requeue"
```

Requeue a dead-letter finalize job:

```bash
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/<job_id>/requeue"
```

Recover stale running jobs:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/recover-stale"
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/recover-stale"
```

Clean up old completed and dead-letter jobs:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/cleanup"
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/cleanup"
```

Run full queue maintenance manually:

```bash
curl -X POST "http://localhost:8000/health/queues/maintenance"
```

## Tests

Fast tests:

```bash
venv/bin/python -m pytest \
  tests/test_api.py \
  tests/test_app_logic.py \
  tests/test_search_jobs_store.py \
  tests/test_finalize_jobs_store.py \
  tests/test_requeue_jobs.py \
  tests/test_recovery_jobs.py \
  tests/test_maintenance_worker.py \
  tests/test_search_worker.py \
  tests/test_finalize_worker.py \
  tests/test_job_worker.py \
  tests/test_task_store_factory.py \
  tests/test_repository_mappers.py \
  tests/test_config.py \
  tests/test_agent.py -q
```

Postgres integration tests:

```bash
venv/bin/python -m pytest -m postgres -q
```

If Postgres is not running, these tests will be `skipped`.

## Smoke Check

Full runtime smoke check against a live API and Postgres:

```bash
venv/bin/python scripts/smoke_postgres_runtime.py
```

The script will:

- run migrations
- start a local `uvicorn` process
- verify health endpoints
- verify search/finalize jobs
- verify worker heartbeat
- verify final state persisted in Postgres
