---
name: config-ops
description: How to change configuration and operations in multi-agent-search — adding an environment setting (src/config.py, .env.example, README, docker-compose.yml), startup validation in src/bootstrap.py, Prometheus metrics and their cardinality rules, alert rules with promtool unit tests, Grafana dashboards, the Compose topology and nginx.conf (CSP, share-token log redaction), and the CI workflows coupled to the main-branch ruleset. Use it whenever you touch config.py, bootstrap.py, .env.example, docker-compose.yml, Dockerfiles, web/nginx.conf, ops/, src/observability/, .github/workflows or .github/rulesets, or when the user asks to "add a setting/flag/env var", "add a metric/alert/dashboard", or "add a CI job".
---

# Configuration, observability, deployment files, CI

## Settings

- **Definition.** Every setting is a typed field with a default on the single `class Settings(BaseSettings)` in `src/config.py`. The env var is the field name in upper case. `.env` is read from the current directory, and unknown keys are ignored.
- **Access.** `from src.config import settings`, a singleton built at import time, so an invalid value breaks every import. In tests: `monkeypatch.setattr(settings, "x", …)`.
- **Types.**
  - Booleans use pydantic parsing (true/false/1/0/yes/no/on/off).
  - List-like settings are kept as a `str` and split where they are used (`cors_allow_origins`, `admin_emails`).
  - The only derived values are properties: `oauth_enabled`, `email_delivery_enabled`, `resolved_database_url` (which builds from `POSTGRES_*` unless `DATABASE_URL` is set).
- **Not in Settings.** `WORKER_NAME`, `WORKER_METRICS_PORT` and `FINALIZE_WORKER_INTERVAL` are read with `os.environ` in `scripts/run_finalize_worker.py`, and `API_WORKERS` only in the Compose api command.
- **What the API refuses at startup.** The lifespan in `src/bootstrap.py` runs `_validate_security_config` and `_validate_email_config`; workers skip both. The API raises on:
  - a weak `AUTH_SECRET_KEY` (the dev default, or shorter than 32 characters) while auth is on, or while `ADMIN_EMAILS` is set even with `AUTH_DISABLED=true`;
  - an unknown `EMAIL_BACKEND`;
  - smtp without host or from;
  - a bad `SMTP_SECURITY` (checked only with `EMAIL_BACKEND=smtp`);
  - a non-http(s) `PUBLIC_APP_URL` while email is on.
- **What only warns:**
  - `AUTH_COOKIE_SECURE=false`, while auth is on;
  - the console email backend;
  - with the console or smtp backend: `SMTP_SECURITY=none` to a non-loopback `SMTP_HOST`, and a plain-http `PUBLIC_APP_URL` on a non-loopback host.
- **What does not crash.** A missing `DEEPSEEK_API_KEY` sets `llm_available=False`, and `/health` reports `degraded`. The Compose api healthcheck then fails.

### Recipe: add a setting

1. **`src/config.py`**: add the field with its default and a comment, in its group.
2. **`.env.example`**: optional settings go in commented form (`# NAME=default`) with a line of explanation. Only required or basic values are uncommented.
3. **README**: document it in the matching section. Candidates:
   - Observability "useful flags"
   - Authentication and Admins
   - the Email settings table
   - Security
   - Exposed ports / Workers
   - the "Minimal `.env`" block, only if it is required.
4. **`docker-compose.yml`**, only if the value must differ inside containers. api, workers and migrate all read `.env` via `env_file`, and a service's `environment:` overrides it.
   - **Worker-wide values** go in the `x-worker-env` anchor, which each worker merges with `<<: *worker-env` inside its own `environment:`.
   - Do not put them in `x-worker-base`: a service-level `environment:` replaces that block wholesale.
   - **The api service does not merge `x-worker-env`.** It has its own copy of `POSTGRES_HOST`, `REDIS_URL`, `USE_REDIS_BROKER` and `LOG_FORMAT`, so a container-specific value the API also needs goes there as well.
5. **Test isolation.** If a developer's `.env` could change test behaviour through the new setting, pin it in `tests/conftest.py::_isolate_auth_settings`.

**No test keeps config.py, .env.example, README and Compose in sync.** About half the fields are not in `.env.example`, so completeness is on you. What the guards do check:

- **`tests/test_docs_quickstart.py`** parses the README "Minimal `.env`:" block and `.env.example` through `Settings(_env_file=…)`.
  - Values of known fields must validate. `Settings` ignores unknown keys (`extra="ignore"`), so a misspelled key passes silently: check the names against config.py.
  - The shipped `AUTH_SECRET_KEY=` line must be rejected, and accepted once it is filled in.
  - `# AUTH_DISABLED=true` must be present.
  - The phrase "off by default" must never appear in either file.
- **`tests/test_compose_config.py`** loads the file with YAML, `<<` merges resolved, and checks:
  - the workers run `run_finalize_worker.py`;
  - each worker's effective env has `REDIS_URL`, `USE_REDIS_BROKER == "true"` and `LOG_FORMAT == "json"`. The values are compared as strings, so **quote booleans in YAML** (`"true"`): an unquoted `true` becomes `"True"` and fails;
  - `WORKER_NAME` and `WORKER_METRICS_PORT` are distinct across workers;
  - every published port except `web` binds `127.0.0.1`.
- **`tests/test_config.py`** pins about 20 defaults. **`tests/test_cleanup_jobs.py`** pins the retention defaults.

## Metrics

- **Where.** Define the metric in `src/observability/metrics.py`, inside the `if Counter is not None and settings.prometheus_metrics_enabled:` block, and set it to `None` in the `else` branch. Add an `observe_*`/`set_*` helper that is a no-op when the metric is None, and export the helper from `src/observability/__init__.py`.
- **Naming.** Prefix `mas_`, snake_case. Counters are exposed with `_total` appended (`mas_llm_cost_usd` is exposed as `mas_llm_cost_usd_total`), and dashboards and alerts use the exposed name.
- **Labels must be bounded.**
  - Paths come from `metric_route_template(request.scope["route"])`: the route template, or `unmatched`.
  - Methods come from `metric_http_method`, which maps anything non-standard to `OTHER`.
  - Never a user id, research id, token or raw path. Per-user and per-research cost lives in the `llm_usage_logs` table, not in metrics.
  - `tests/test_metrics_cardinality.py` drives traffic over many researches and random tokens and fails if any series appears. It drives only API routes and LLM cost, so a new labelled metric is not covered until you extend the test.
- **Workers** serve the same default registry on `/metrics` at `WORKER_METRICS_PORT` (default 9101; 0 disables it), with no token. The API `/metrics` requires `X-Metrics-Token` or a Bearer token when `METRICS_TOKEN` is set, and nginx returns 404 for it. `ops/prometheus/prometheus.yml` has no `authorization` block, so add one when you set the token.
- **The API runs `uvicorn --workers 2` without multiprocess mode**, so API series flip between processes. Queue gauges and LLM cost must be read from the worker scrape job. A new metric that must be read from the worker job also goes into `WORKER_ONLY_METRICS` in `tests/test_ops_dashboards.py`, so the dashboard guard applies to it.
- **Pricing-coupled tests.** A pricing change in `src/providers/deepseek.py` breaks `tests/test_llm_cost_metrics.py` (it expects an exact cost) and `test_deepseek_pricing.py`.

## Alerts and dashboards

- **`ops/prometheus/alerts.yml`** has one group. Each rule has `labels.severity` (critical or warning) and `annotations.summary/description`.
  - Queue and worker rules filter `job="multi-agent-search-workers"`.
  - Gauges use `delta()`, not `increase()`.
  - Ratios guard the divisor with `clamp_min(…, 1e-9)`.
- **`ops/prometheus/alerts.test.yml`** holds promtool unit tests. Each has `interval`, `input_series` (the full label set, including `job` and `instance`; `values` like `'0x10 1x50'`), and `alert_rule_test` with `eval_time`, `alertname` and `exp_alerts`.
  - `exp_labels` lists `severity` plus the `by()` labels. A rule with no aggregation (like `WorkerDown`: `up{…} == 0`) keeps every label of the input series (job, instance, worker, …).
  - `exp_annotations` must match the rendered text exactly (`humanizePercentage` renders `50%`).
  - `exp_alerts: []` asserts silence.
  - Changing a summary or description means updating its test. Give new alerts a test.
- **Run it** with `ci_local.sh promtool`. It uses the same digest-pinned image as CI and Compose, and needs a running Docker; without one it reports `skipped`, and CI's "Prometheus alert rules" job is the only check.
- **Adding a rule file** needs:
  - a Compose volume and a `rule_files` entry in prometheus.yml;
  - the file added to the CI promtool commands (quality-gates.yml names only `alerts.yml` and `alerts.test.yml`);
  - the file added to `ci_local.sh promtool`;
  - the file added to `rule_files:` in the test file.
  Otherwise the new file is never checked or tested.
- **No Alertmanager.** Alerts show only in the Prometheus UI. Update the alert list in the README Observability section when you add one.
- **Dashboards** live in `ops/grafana/provisioning/dashboards/*.json` (datasource uids `prometheus` and `loki`).
  - `tests/test_ops_dashboards.py` requires every panel query that uses `mas_llm_cost_usd_total`, `mas_queue_jobs` or `mas_queue_backlog` to carry exactly `job="multi-agent-search-workers"` in its selector (`job=~` fails).
  - Each of those metrics must appear somewhere, and LLM-cost panel titles must say "worker".
- **Logs.** Promtail labels are only container, service, stream, level and worker_name. Filter ids at query time, never as labels.

## Compose and nginx

- **Services.**

| Service | Host port | Notes |
|---|---|---|
| api | `127.0.0.1:${API_PORT:-8001}` → 8000 | healthcheck: `/health` must report `ok` |
| web | `${WEB_PORT:-8502}` → nginx 80 | the only public service |
| worker, worker_2, worker_3 | none | metrics on 9101/9102/9103 |
| db | `127.0.0.1:5433` | |
| pgbouncer | `127.0.0.1:6432` | transaction mode; api and workers connect through it |
| redis | `127.0.0.1:6379` | |
| prometheus, loki, grafana | `127.0.0.1` only | grafana on 3001 |
| migrate | none | one-shot `alembic upgrade head`, straight to db |
| promtail | none | |

- **Images.** Pulled images (redis, postgres, pgbouncer, prometheus, loki, promtail, grafana) are pinned by digest; the Dockerfile base images (`python:3.11-slim`, `node:22-alpine`, `nginx:1.27-alpine`) are tag-only. Copy digests by hand into CI (the Prometheus image, the Postgres service image) when you bump them.
- **Rebuilds.** Only `./src` is mounted into api and workers; changes to `scripts/` or `alembic/` need an image rebuild. The `web` image builds the SPA itself (`web/Dockerfile`), so a frontend change needs `docker compose up -d --build web`. **Never bind-mount a host `web/dist` over `/usr/share/nginx/html`:** it shadows the image's build with whatever was last built on the host. Such a mount once kept a weeks-old UI in service, including a public report that could not scroll, through every rebuild. `tests/test_compose_config.py` refuses it.
- **Adding a worker replica** takes:
  - a unique `WORKER_NAME` and `WORKER_METRICS_PORT`;
  - its own `container_name` (a copied block clashes with the existing container);
  - a Prometheus scrape target with a `worker` label (WorkerDown uses it);
  - a `worker*` service name, which the logs panel matches;
  - the service lists in `scripts/restore_postgres.sh` and the README.
- **`web/nginx.conf`** (copied into the web image: `docker compose up -d --build web`):
  - **CSP.** The SPA policy is assembled from `set $spa_csp` lines. `script-src` carries the sha256 of `web/index.html`'s inline theme script twice, as LF and as CRLF. Change the script and both hashes change; `web/src/securityHeaders.test.ts` fails and prints the expected values (see the `web-frontend` skill).
  - **Headers.** Any `location` with its own `add_header` must repeat all the security headers with `always`.
  - **Referrer and cache maps.** `/r/`, `/reset-password` and `/verify-email` get `no-referrer`; the recovery pages get `no-store`.
  - **Share-token redaction.** `map` blocks redact share tokens from the access log's request line and Referer. `tests/test_nginx_log_redaction.py` runs those regexes with Python `re` and checks that they stay linear. Keep them in step with `_SHARE_TOKEN_PATH_RE` in `src/observability/logging.py`.
  - **A cross-origin API** (`VITE_API_BASE`) needs `connect-src` in the CSP and `CORS_ALLOW_ORIGINS`.

## CI and the ruleset

- **The six required checks** are the job `name:`s: `Backend unit tests`, `Frontend typecheck and tests`, `Native Rust tests`, `Offline evaluation gate`, `Prometheus alert rules` (all in quality-gates.yml) and `Postgres smoke` (postgres-smoke.yml).
  - Both workflows run on push to main, on pull_request and on workflow_dispatch.
  - The `"on":` key is quoted on purpose.
  - Actions are pinned by commit SHA.
- **`.github/rulesets/main.json`** requires those six contexts from GitHub Actions (`integration_id` 15368), strict up-to-date branches, resolved conversations, no required approvals and no bypass actors.
- **`tests/test_branch_protection_config.py`** requires the ruleset's contexts to equal the job names of all PR-triggered workflows. It also keeps the exact line `  "name": "main protection",`, which the apply script's sed relies on.
- **Adding or renaming a PR-triggered job:**
  1. Update the ruleset contexts.
  2. Update the README "Branch Protection" list (it says "all six").
  3. A repository admin applies the ruleset with `scripts/apply_branch_protection.sh [owner/repo]` (it uses `gh api`); GitHub does not read the file by itself. **Timing matters:**
     - For an **added** job, apply it after the merge.
     - For a **renamed or removed** job, apply it just before merging that PR. A live ruleset keeps waiting for the old check name, and there are no bypass actors, so the PR could never merge.

## Scripts (one line each)

- **`create_admin.py <email> [--password-stdin]`**: provisions or resets an `ADMIN_EMAILS` account. It never takes the password as an argument.
- **`issue_password_reset.py <email>`**: prints a one-time reset link, for deployments without email.
- **`apply_branch_protection.sh`**: creates or updates the ruleset by name.
- **`backup_postgres.sh [dest]`** / **`restore_postgres.sh FILE --confirm`**: `pg_dump -Fc` with a verified checksum; the restore refuses while api, workers or pgbouncer run.
- **`smoke_postgres_runtime.py`**: migrations, uvicorn, one worker pass, then database assertions. It writes to the configured database.
- **`run_finalize_worker.py [--once]`**: the worker loop with a metrics thread.
- **`loadtest.py`**: loads cheap read endpoints only, after registering or logging in `loadtest@local.test` on the target (so it creates that account); it never starts researches.
- **`build_native_module.sh`**: `maturin develop` for the Rust extension. Set `VENV_PYTHON` when the venv is `.venv`.

Verify with `ci_local.sh guards`, plus `backend` (or a targeted `pytest`) for settings, bootstrap and metrics changes: `test_config`, `test_security_config`, `test_bootstrap_health`, `test_cleanup_jobs`, `test_metrics_cardinality` and `test_llm_cost_metrics` are not guards. Add `promtool` or `frontend` as relevant (see the `run-checks` skill).
