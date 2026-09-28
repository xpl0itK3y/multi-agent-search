#!/usr/bin/env bash
# Run this repository's CI gates locally, the way .github/workflows/*.yml run them.
#
#   ci_local.sh all                 lint, shell syntax, backend unit tests (with the coverage floor),
#                                   eval gate, vue-tsc, vitest, and native/promtool when their tools exist
#   ci_local.sh lint                ruff check src/ scripts/ eval/ tests/ + bash -n on every scripts/*.sh
#   ci_local.sh backend [args]      pytest -m "not postgres" on the memory store, with --cov like CI
#   ci_local.sh pytest <args>       pytest with the backend-unit env, e.g. tests/test_api.py -k share
#   ci_local.sh guards              backend tests that read web/, README, compose, nginx, ops, CI and ruleset files
#   ci_local.sh frontend            vue-tsc --noEmit + vitest run
#   ci_local.sh vitest [args]       vitest run with args, e.g. src/components/SlideOver.test.ts
#   ci_local.sh build               production build (vue-tsc + vite build), not a CI step but catches more
#   ci_local.sh eval                offline evaluation gate
#   ci_local.sh native              cargo test for native/text_processing
#   ci_local.sh promtool            Prometheus rule check + unit tests (needs a running Docker)
#   ci_local.sh pg <cmd>            pg_throwaway.py with the detected Python: up | status | down | destroy | env
#   ci_local.sh postgres [args]     postgres-marked tests against a running server (default: the throwaway
#                                   one on 55432; PG_PORT=5433 for `docker compose up -d db`)
#   ci_local.sh smoke               the Postgres Smoke job: fresh database, alembic upgrade head,
#                                   scripts/smoke_postgres_runtime.py, then the full pytest. It DROPS the
#                                   multi_agent_search database, so it runs only against the throwaway server.
#
# A step whose tool is missing (cargo, Docker, pytest-cov) is reported as "skipped", never as ok.
# Interpreters: $PY and $NODE override detection. Without them the script takes the repo venv
# (.venv or venv, POSIX or Windows layout) and node on PATH, falling back to the Windows
# binaries when run from WSL with no native toolchain (env vars are then forwarded via WSLENV).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT" || exit 2

pick_python() {
  if [[ -n "${PY:-}" ]]; then echo "$PY"; return; fi
  local c
  for c in .venv/bin/python venv/bin/python .venv/Scripts/python.exe venv/Scripts/python.exe; do
    if [[ -x "$c" ]]; then echo "$c"; return; fi
  done
  command -v python3 || command -v python
}

pick_node() {
  if [[ -n "${NODE:-}" ]]; then echo "$NODE"; return; fi
  if command -v node >/dev/null; then command -v node; return; fi
  local win="/mnt/c/Program Files/nodejs/node.exe"
  if [[ -x "$win" ]]; then echo "$win"; fi
}

PY="$(pick_python)"
NODE="$(pick_node)"
FAILED=()
SKIPPED=()
SKIP=3  # a step function returns this when its tool is missing

# Windows executables started from WSL only see the variables listed in WSLENV.
forward() {
  if [[ "$PY" == *.exe || "$NODE" == *.exe ]]; then
    local name
    for name in "$@"; do
      case ":${WSLENV:-}:" in *":$name:"*) ;; *) export WSLENV="${WSLENV:+$WSLENV:}$name" ;; esac
    done
  fi
}

step() {
  local name="$1"; shift
  printf '\n\033[1m== %s\033[0m\n' "$name"
  "$@"
  local code=$?
  if ((code == 0)); then
    printf '\033[32mok\033[0m %s\n' "$name"
  elif ((code == SKIP)); then
    printf '\033[33mskipped\033[0m %s\n' "$name"
    SKIPPED+=("$name")
  else
    printf '\033[31mFAILED\033[0m %s\n' "$name"
    FAILED+=("$name")
  fi
}

have() { command -v "$1" >/dev/null 2>&1; }

unit_env() {
  export TASK_STORE_BACKEND=memory ALLOW_MEMORY_TASK_STORE=true AUTH_DISABLED=true PYTHONUTF8=1
  forward TASK_STORE_BACKEND ALLOW_MEMORY_TASK_STORE AUTH_DISABLED PYTHONUTF8
}

need_node() {
  if [[ -z "$NODE" ]]; then echo "node not found (set NODE=/path/to/node)"; return 1; fi
  if [[ ! -d web/node_modules ]]; then echo "web/node_modules is missing: run 'npm ci' in web/"; return 1; fi
}

ruff_check() {
  if "$PY" -m ruff --version >/dev/null 2>&1; then
    "$PY" -m ruff check src/ scripts/ eval/ tests/
  else
    echo "ruff is not installed in $PY (CI pins ruff==0.16.8: $PY -m pip install ruff==0.16.8)"
    return 1
  fi
}

# Each file on its own: `bash -n a.sh b.sh` checks only a.sh (the rest become its arguments).
shell_syntax() {
  local f code=0
  for f in scripts/*.sh; do bash -n "$f" || code=1; done
  return $code
}

# CI adds --cov=src; pyproject's fail_under (the coverage floor) is enforced through pytest-cov.
pytest_unit() {
  unit_env
  local cov=()
  if "$PY" -c "import pytest_cov" >/dev/null 2>&1; then
    cov=(--cov=src --cov-report=term)
  else
    echo "pytest-cov is not installed: running without the coverage floor (CI pins pytest-cov==7.1.0)"
  fi
  "$PY" -m pytest -q -p no:cacheprovider -m "not postgres" "${cov[@]}" "$@"
}

pytest_args() { unit_env; "$PY" -m pytest -q -p no:cacheprovider "$@"; }

# Tests that parse files outside src/ (the frontend, README/.env.example, compose, nginx, ops
# dashboards, CI workflows, the ruleset), plus the telemetry test that mirrors web/src/lib/telemetry.ts.
GUARDS=(
  tests/test_frontend_error_contract.py
  tests/test_docs_quickstart.py
  tests/test_branch_protection_config.py
  tests/test_compose_config.py
  tests/test_nginx_log_redaction.py
  tests/test_ops_dashboards.py
  tests/test_telemetry_input_bounds.py
)

guards() { pytest_args "${GUARDS[@]}"; }

vue_tsc() { need_node && (cd web && "$NODE" node_modules/vue-tsc/bin/vue-tsc.js --noEmit); }

vitest() { need_node && (cd web && "$NODE" node_modules/vitest/vitest.mjs run "$@"); }

vite_build() { need_node && (cd web && "$NODE" node_modules/vue-tsc/bin/vue-tsc.js --noEmit && "$NODE" node_modules/vite/bin/vite.js build); }

eval_gate() { "$PY" -m eval --fixtures eval/fixtures --gate; }

PG_HELPER=".claude/skills/run-checks/scripts/pg_throwaway.py"

pg_helper() { "$PY" "$PG_HELPER" "$@"; }

# The Postgres Smoke job's env, pointed at a local server (pg_throwaway.py's by default).
pg_env() {
  export TASK_STORE_BACKEND=postgres POSTGRES_HOST="${PG_HOST:-127.0.0.1}" POSTGRES_PORT="${PG_PORT:-55432}"
  export POSTGRES_USER=app POSTGRES_PASSWORD=app POSTGRES_DB=multi_agent_search
  export AUTH_DISABLED=true DEEPSEEK_API_KEY=test-key PGCLIENTENCODING=UTF8 PYTHONUTF8=1
  forward TASK_STORE_BACKEND POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB \
    AUTH_DISABLED DEEPSEEK_API_KEY PGCLIENTENCODING PYTHONUTF8
}

# The tests create and drop their own mas_postgres_tests database, so any server with
# CREATE DATABASE rights is safe here, the compose db included.
pytest_postgres() { pg_env; "$PY" -m pytest -q -p no:cacheprovider -m postgres "$@"; }

# Mirrors .github/workflows/postgres-smoke.yml step by step (only the server differs). It drops
# and recreates multi_agent_search, so it refuses any server but the throwaway one: the compose
# db on 5433 keeps real development data in a persistent volume.
smoke() {
  pg_env
  if ! pg_helper status | grep -q "running on 127.0.0.1:${POSTGRES_PORT} "; then
    echo "no throwaway server on port ${POSTGRES_PORT}: run 'ci_local.sh pg up' first (smoke never touches another database)"
    return 1
  fi
  pg_helper reset-db --port "$POSTGRES_PORT" &&
    "$PY" -m alembic upgrade head &&
    "$PY" scripts/smoke_postgres_runtime.py &&
    "$PY" -m pytest -q -p no:cacheprovider
}

native_tests() {
  if ! have cargo; then echo "cargo not found (CI runs this as 'Native Rust tests')"; return $SKIP; fi
  cargo test --manifest-path native/text_processing/Cargo.toml
}

# First docker CLI that reaches a daemon: on WSL without Docker Desktop's WSL integration the
# `docker` shim fails while docker.exe works.
find_docker() {
  local d
  for d in docker docker.exe; do
    if have "$d" && "$d" info >/dev/null 2>&1; then command -v "$d"; return 0; fi
  done
  return 1
}

promtool() {
  local image docker rules
  image="$(sed -n 's/^ *PROMETHEUS_IMAGE: *//p' .github/workflows/quality-gates.yml | head -1)"
  if ! docker="$(find_docker)"; then
    echo "no running Docker (CI runs this as 'Prometheus alert rules')"; return $SKIP
  fi
  rules="$PWD/ops/prometheus"
  if [[ "$docker" == *.exe ]] && have wslpath; then rules="$(wslpath -w "$rules")"; fi
  "$docker" run --rm --entrypoint promtool -v "$rules:/rules:ro" "$image" check rules /rules/alerts.yml &&
    "$docker" run --rm --entrypoint promtool -v "$rules:/rules:ro" "$image" test rules /rules/alerts.test.yml
}

echo "repo: $ROOT"
echo "python: ${PY:-<none>}   node: ${NODE:-<none>}"

cmd="${1:-all}"
shift || true
case "$cmd" in
  lint) step "ruff" ruff_check; step "bash -n scripts/*.sh" shell_syntax ;;
  backend) step "backend unit tests" pytest_unit "$@" ;;
  pytest) step "pytest $*" pytest_args "$@" ;;
  guards) step "repo-file guard tests" guards ;;
  frontend) step "vue-tsc" vue_tsc; step "vitest" vitest ;;
  vitest) step "vitest $*" vitest "$@" ;;
  build) step "vite build" vite_build ;;
  eval) step "eval gate" eval_gate ;;
  native) step "native cargo test" native_tests ;;
  promtool) step "promtool" promtool ;;
  pg) pg_helper "$@"; exit $? ;;
  postgres) step "postgres-marked tests" pytest_postgres "$@" ;;
  smoke) step "Postgres Smoke simulation" smoke ;;
  all)
    step "ruff" ruff_check
    step "bash -n scripts/*.sh" shell_syntax
    step "backend unit tests" pytest_unit
    step "eval gate" eval_gate
    step "vue-tsc" vue_tsc
    step "vitest" vitest
    step "native cargo test" native_tests
    step "promtool" promtool
    ;;
  *) sed -n '2,27p' "$0"; exit 2 ;;
esac

if ((${#SKIPPED[@]})); then
  printf '\n\033[33mskipped (not run, not passed):\033[0m %s\n' "${SKIPPED[*]}"
fi
if ((${#FAILED[@]})); then
  printf '\033[31mfailed:\033[0m %s\n' "${FAILED[*]}"
  exit 1
fi
printf '\033[32mall checks that ran passed\033[0m\n'
