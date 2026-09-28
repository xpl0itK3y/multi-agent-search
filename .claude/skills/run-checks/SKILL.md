---
name: run-checks
description: Run this repository's CI gates locally before calling a change done — ruff, shell syntax, backend unit tests on the memory store with the coverage floor, the offline eval gate, vue-tsc and vitest, the repo-file guard tests, and the Postgres-marked tests or a full Postgres Smoke simulation against a throwaway PostgreSQL (no Docker needed). Use this whenever you finish or verify a change in multi-agent-search, when a CI job (Quality Gates / Postgres Smoke) fails, when asked to "run the tests", "check CI", "проверь", "прогони тесты", or before pushing or opening a PR.
---

# Run the checks CI runs

CI is two workflows with six required jobs. They are the `main` ruleset's required checks:

| Job (`name:`) | Workflow | What it runs |
|---|---|---|
| Backend unit tests | quality-gates.yml | `ruff check src/ scripts/ eval/ tests/`, `bash -n scripts/*.sh`, then `pytest -q -m "not postgres" --cov=src` (coverage floor `fail_under = 78` in pyproject.toml), with `TASK_STORE_BACKEND=memory ALLOW_MEMORY_TASK_STORE=true AUTH_DISABLED=true` |
| Frontend typecheck and tests | quality-gates.yml | in `web/`: `npm ci`, `npx vue-tsc --noEmit`, `npm run test` (vitest) |
| Offline evaluation gate | quality-gates.yml | `python -m eval --fixtures eval/fixtures --gate` |
| Native Rust tests | quality-gates.yml | `cargo test --manifest-path native/text_processing/Cargo.toml` |
| Prometheus alert rules | quality-gates.yml | `promtool check rules` and `promtool test rules` in the digest-pinned Prometheus image |
| Postgres smoke | postgres-smoke.yml | a Postgres 16 service, `alembic upgrade head`, `scripts/smoke_postgres_runtime.py`, then the **full** `pytest -q` with `TASK_STORE_BACKEND=postgres` |

## One script for all of it

`scripts/ci_local.sh`, in this skill's directory, runs each gate the way CI does. It finds the interpreters itself: `.venv` or `venv` in POSIX or Windows layout, `node` on PATH, or the Windows binaries from WSL. It changes to the repo root itself. The paths below are relative to the repo root; if the file lost its executable bit (for example after a Windows checkout), run it as `bash $S/ci_local.sh …`.

```bash
S=.claude/skills/run-checks/scripts
$S/ci_local.sh all                       # lint, shell syntax, backend unit + coverage, eval, vue-tsc, vitest (+ cargo/promtool if present)
$S/ci_local.sh pytest tests/test_api.py -k share   # targeted pytest with the backend-unit env
$S/ci_local.sh vitest src/components/SlideOver.test.ts
$S/ci_local.sh guards                    # backend tests that read web/, README, compose, nginx, ops, CI files
$S/ci_local.sh build                     # vue-tsc + vite build (not in CI, but catches build-only breakage)
$S/ci_local.sh pg up                     # throwaway PostgreSQL (see below); pg status | down | destroy
$S/ci_local.sh postgres                  # postgres-marked tests
$S/ci_local.sh smoke                     # the Postgres Smoke job end to end (throwaway server only)
```

- **Overrides.** Set `PY=/path/to/python` or `NODE=/path/to/node` to skip detection.
- **Frontend dependencies.** If `web/node_modules` is missing, run `npm ci` in `web/` first.
- **Python tools.** The venv needs `ruff` (CI pins `ruff==0.16.8`) and `pytest-cov` (CI pins `7.1.0`). Without pytest-cov the script runs without the coverage floor and says so.
- **Skipped is not passed.** A step whose tool is missing (cargo, a running Docker) is reported as `skipped`, and the summary lists it. Report it as "not run", never as passed.

## Pick the right depth

Run `all` before pushing. While you iterate, run what the change can break:

- **Backend Python.** A targeted `pytest` for the touched area, then `backend` and `lint`. An unused import fails the ruff step of the required job.
- **Anything in `web/`.** `vitest <file>` while working, then `frontend`. vue-tsc runs with `noUnusedLocals` and type-checks the test files too. Also run `guards`, because backend tests parse `web/src/lib/api.ts`, `web/src/lib/stream.ts` and `web/nginx.conf`.
- **README, `.env.example`, `docker-compose.yml`, `web/nginx.conf`, `ops/grafana`, `.github/`.** Run `guards`. Settings, bootstrap and metrics changes also need `backend` (their tests are not guards).
- **`ops/prometheus/alerts*.yml`.** Run `promtool`, which needs Docker. Without Docker, CI's "Prometheus alert rules" job is the only real check; say so.
- **Store, model or migration changes.** Run `postgres`: the conformance suite runs on both stores, but its postgres leg only runs with a database. Run `smoke` before pushing.
- **`eval/` or anything that scores reports.** Run `eval`.
- **`native/text_processing`.** Run `native` (needs cargo). CI never builds the extension for the Python tests; they exercise the pure-Python fallbacks in `src/core/rust_accel.py`.

Tests marked `postgres` **skip** (they do not fail) when no database is reachable, so a green `backend` run says nothing about them. CI's Postgres Smoke job is where they really run. Run them locally when you touch persistence.

## A PostgreSQL without Docker

`scripts/pg_throwaway.py` gets PostgreSQL 16 binaries from the `pgserver` wheel. It installs the wheel with `--no-deps` into a temp directory, never into the repo or its venv. It runs a disposable server on `127.0.0.1:55432` with user and password `app`/`app` (the CI job's credentials) and fsync off. `ci_local.sh pg <cmd>` runs it with the same Python as the tests, so the binaries match the platform:

```bash
$S/ci_local.sh pg up          # install if needed, initdb, start, create multi_agent_search
$S/ci_local.sh postgres       # or: $S/ci_local.sh smoke
$S/ci_local.sh pg down        # stop; `pg destroy` also deletes the directory
```

- **`smoke` drops and recreates the `multi_agent_search` database**, as a fresh CI service container would. It therefore refuses to run unless the throwaway server is up on that port. Never point it at the compose `db` (port 5433): that one keeps development data in a persistent volume.
- **`postgres` is safe against any server** with CREATE DATABASE rights, the compose db included (`PG_PORT=5433`). `tests/conftest.py::_postgres_test_db` drops and recreates its own `mas_postgres_tests` database from `POSTGRES_HOST/PORT/USER/PASSWORD` and migrates it to head in-process. `postgres_session_factory` truncates the runtime tables before each test. The migration-step tests use their own `mas_postgres_migration_steps` database.
- **Docker on WSL.** Without Docker Desktop's WSL integration the `docker` shim fails; use `docker.exe compose up -d db`. `ci_local.sh promtool` tries `docker.exe` itself.

## Windows and WSL notes

These matter only when the repo lives on a Windows drive and is driven from WSL without a Linux toolchain (`git.exe`, `.venv/Scripts/python.exe`, `node.exe`):

- **Environment variables.** Windows executables only see the variables named in `WSLENV`. `ci_local.sh` exports it for you. For ad-hoc runs, do the same by hand, e.g. `TASK_STORE_BACKEND=memory ALLOW_MEMORY_TASK_STORE=true AUTH_DISABLED=true WSLENV=TASK_STORE_BACKEND:ALLOW_MEMORY_TASK_STORE:AUTH_DISABLED .venv/Scripts/python.exe -m pytest -q -m "not postgres" -p no:cacheprovider`.
- **Postgres client encoding.** Some Windows setups default it to the ANSI code page (e.g. WIN1251), which breaks non-ASCII round trips. `PGCLIENTENCODING=UTF8` fixes it, and the scripts set it.
- **Console windows.** Every Windows process may flash a console window on the user's desktop. Batch work into few invocations, and never leave servers running when you are done.
- **Flaky timestamps.** Timestamp-tie tests can be flaky locally, because the Windows clock has about 15 ms resolution. Linux CI does not have this; re-run before debugging.

## Reading failures

- **`test_frontend_error_contract`.** A web error mapping in `api.ts` or `stream.ts` no longer matches a backend detail, or the reverse. See the `api-endpoint` skill.
- **`test_every_protocol_method_runs_in_the_conformance_suite` / `test_both_stores_implement_the_protocol_signatures`.** A TaskStore method has no conformance test, or the two stores' signatures drifted. See the `data-layer` skill.
- **`test_migration_contract`.** The ORM models and the migrated schema differ, server defaults included. See the `data-layer` skill.
- **`securityHeaders.test.ts`.** An inline script in `web/index.html` changed, so its CSP hashes in `web/nginx.conf` must change too. The test prints the expected values. See the `web-frontend` skill.
- **`test_branch_protection_config`.** A CI job was added or renamed without updating `.github/rulesets/main.json`. See the `config-ops` skill.
- **Eval gate.** A metric got worse than `eval/baseline.json` by more than 5% (relative, direction-aware; improvements pass). See the `research-pipeline` skill before touching the baseline.
- **Coverage below `fail_under`.** New code without tests. Add tests; do not lower the floor.

Report results as they are. When something was skipped (no cargo, no Docker, no database), say so rather than implying it passed.
