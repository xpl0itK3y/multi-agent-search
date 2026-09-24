# Multi-Agent Search & Optimizer

FastAPI backend for a research workflow with persistent jobs, Postgres storage, and a separate worker process.

## LangGraph Finalize Loop

The post-search finalize orchestration lives in `src/graph/` as a single
LangGraph state machine (a pinned hard dependency):

- collect source and evidence summaries
- branch into a replan step when coverage looks weak
- run the analyzer
- retry analysis once when the generated draft still contains report-note style quality warnings

Fresh and resumed runs (after a worker crash) walk the SAME topology — a
checkpointed run re-enters the graph at the successor of its last completed
step, so a research's behavior no longer depends on whether a worker died
mid-finalize.

## Observability

The project now supports:

- structured JSON logging with `LOG_FORMAT=json`
- Prometheus metrics at `/metrics` (API) and one exporter per worker
  (`WORKER_METRICS_PORT`, internal network only)
- Docker Compose services for `prometheus` and `grafana`, with provisioned
  dashboards (Platform Overview, Research Operations) and alert rules
  (`WorkerDown`, `APIDown`, `DeadLetterQueueGrowth`, `APIHighErrorRate`,
  `WorkerJobFailureRate`, `FinalizeQueueBacklog`); test the rules with
  `promtool test rules ops/prometheus/alerts.test.yml`
- `loki` + `promtail` collecting every container's stdout. JSON log lines are
  stored whole, so the Research Operations dashboard's Logs panel shows the API
  and worker logs of one research (type its id into the Research ID box)

Metrics caveat: the API runs `uvicorn --workers $API_WORKERS` (2 in Compose)
without `prometheus_client` multiprocess mode, so each API process keeps its own
counters and every scrape reads one of them at random. API request rates,
latency and `APIHighErrorRate` are therefore approximate (process switches look
like counter resets). Set `API_WORKERS: "1"` on the `api` service in
`docker-compose.yml` when you need exact API metrics. The workers are
single-process and unaffected, which is why the queue alerts use the worker
series.

Health endpoints: `GET /health` is the cheap readiness probe (status +
dependency pings); `GET /health/detail` (admin) returns the full operational
payload — queue metrics, graph alerts and trends.

`/metrics` is blocked at the nginx edge and, optionally, protected by a shared
secret (`METRICS_TOKEN`; sent as `Authorization: Bearer …` or `X-Metrics-Token`).
When you set it, add the same token to the Prometheus scrape jobs in
`ops/prometheus/prometheus.yml`.

Note: with `LANGSMITH_TRACING=true`, prompts and generated content are sent to
the configured LangSmith project — keep it off for sensitive workloads.

Useful flags:

```env
LOG_FORMAT=json
PROMETHEUS_METRICS_ENABLED=true
# METRICS_TOKEN=change-me
# Retention for finished researches; 0 keeps everything (default).
# RESEARCH_RETENTION_SECONDS=0
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
QUEUE_MAINTENANCE_INTERVAL_SECONDS=60
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
- `web`
- `redis`
- `prometheus`
- `loki`
- `promtail`
- `grafana`

The API will be available at `http://localhost:8000`.

The Vue web UI will be available at `http://localhost:8501`.

Prometheus will be available at `http://localhost:9090`.

Loki will be available at `http://localhost:3100`.

Grafana will be available at `http://localhost:3001` (Prometheus and Loki are pre-provisioned as datasources).

### Exposed ports

Only the web UI listens on all interfaces. The API, Postgres (`5433`),
PgBouncer (`6432`), Redis (`6379`), Prometheus (`9090`), Loki (`3100`) and
Grafana (`3001`) are published on `127.0.0.1` only; the containers reach each
other over the Compose network.

- To reach them from another machine, use an SSH tunnel
  (`ssh -L 3001:127.0.0.1:3001 user@host`) or an authenticated reverse proxy.
  Do not rebind them to `0.0.0.0`: Redis, Prometheus and Loki have no
  authentication at all.
- Docker-published ports bypass host firewalls: Docker forwards them through
  its own iptables rules, which UFW/firewalld input rules never see. Firewalling
  the host does not replace the loopback binding.
- The credentials in the Compose file are local-development defaults. For any
  real deployment set a strong Grafana admin password (`GRAFANA_ADMIN_PASSWORD`,
  passed to `GF_SECURITY_ADMIN_PASSWORD`) and change the Postgres credentials
  (`POSTGRES_USER` / `POSTGRES_PASSWORD`, which PgBouncer uses too) in `.env`.

### Workers and the Redis broker

The API and all three workers run with `USE_REDIS_BROKER=true`; the workers
share one `x-worker-env` block in `docker-compose.yml`. The LLM concurrency
limit (`LLM_MAX_CONCURRENT`, default 16) is therefore enforced through Redis
across the API and every worker together, not per process; raise it if worker
throughput drops.

Older Compose files silently dropped the Redis settings from the workers, which
then polled Postgres while the API kept pushing job ids that nobody popped. When
upgrading such a deployment, clear the stale Redis lists before starting the
new workers:

```bash
docker compose stop worker worker_2 worker_3
docker compose exec redis redis-cli DEL mas:search_jobs mas:finalize_jobs
docker compose up -d worker worker_2 worker_3
```

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

6. Optional: start the Vue web UI (dev server with API proxy):

```bash
cd web
npm install
npm run dev   # http://localhost:5173
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

- live production profiling on a real deployment

## Quick Check

Health:

```bash
curl http://localhost:8000/health          # cheap readiness probe
curl http://localhost:8000/health/detail   # full operational payload (admin)
```

Delete an account with all owned data (researches, results, public share
links — cascades in the database; requires the current password and
`confirm: true`):

```bash
curl -X DELETE "http://localhost:8000/v1/auth/account" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"confirm": true, "current_password": "..."}'
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

## PostgreSQL Backup and Restore

Create a compressed custom-format backup through the running `db` service:

```bash
./scripts/backup_postgres.sh
# Optional explicit destination:
./scripts/backup_postgres.sh /srv/mas-backups/postgres-$(date -u +%F).dump
```

The script writes through a temporary file, validates the archive with
`pg_restore --list`, refuses to overwrite an existing backup, and creates a
`.sha256` checksum when `sha256sum` is available. Keep backups outside the
repository and copy them to storage independent from the Docker host.

Schedule at least one daily backup and retain a policy appropriate to the
deployment; a practical baseline is 7 daily, 4 weekly, and 6 monthly copies.
For example, after a successful daily backup, files older than the chosen
window can be removed by the host's backup job. Alert on both backup failure and
absence of a recent archive.

Restore is destructive and deliberately requires the application, workers, and
PgBouncer to be stopped plus an explicit `--confirm` argument:

```bash
docker compose stop api worker worker_2 worker_3 pgbouncer
./scripts/restore_postgres.sh /srv/mas-backups/postgres-2026-09-09.dump --confirm
docker compose run --rm migrate
docker compose up -d pgbouncer api worker worker_2 worker_3
curl http://localhost:8000/health
```

Run a restore drill at least quarterly on an isolated host or maintenance
window: restore the newest archive, apply migrations, verify `/health`, open a
known completed report, and confirm its tasks and sources are present. Record
the archive name, checksum result, recovery duration, and verifier. Never use
`docker compose down -v` as part of a drill against the production project.

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

## License

Released under the [MIT License](./LICENSE).
