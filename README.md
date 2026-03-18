# Multi-Agent Search & Optimizer

FastAPI backend for a research workflow with persistent jobs, Postgres storage, and a separate worker process.

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

## Run With Docker Compose

```bash
docker compose up --build
```

This starts:

- `db`
- `migrate`
- `api`
- `worker`

The API will be available at `http://localhost:8000`.

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

Run the worker once:

```bash
python scripts/run_finalize_worker.py --once
```

## Quick Check

Health:

```bash
curl http://localhost:8000/health
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
