#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

archive="${1:-}"
confirmation="${2:-}"

if [[ -z "$archive" || "$confirmation" != "--confirm" ]]; then
  echo "Usage: $0 PATH_TO_BACKUP.dump --confirm" >&2
  echo "Restore replaces the current PostgreSQL database." >&2
  exit 2
fi
if [[ ! -f "$archive" || ! -r "$archive" ]]; then
  echo "Backup is not a readable file: $archive" >&2
  exit 2
fi

running_services="$(docker compose ps --status running --services)"
for service in api worker worker_2 worker_3 pgbouncer; do
  if grep -qx "$service" <<< "$running_services"; then
    echo "Refusing restore while $service is running." >&2
    echo "Stop API, workers, and pgbouncer first; see the restore runbook in README.md." >&2
    exit 2
  fi
done

if [[ -f "${archive}.sha256" ]] && command -v sha256sum > /dev/null 2>&1; then
  (
    cd "$(dirname "$archive")"
    sha256sum --check "$(basename "$archive").sha256"
  )
fi

# Validate before executing any DROP/CREATE statements from the archive.
docker compose exec -T db sh -c \
  'exec pg_restore --list -U "$POSTGRES_USER"' \
  < "$archive" > /dev/null

echo "Restoring PostgreSQL from: $archive"
docker compose exec -T db sh -c \
  'exec pg_restore --clean --if-exists --create --exit-on-error --no-owner --no-acl -U "$POSTGRES_USER" --dbname=postgres' \
  < "$archive"
echo "Restore complete. Run migrations, verify /health, then restart application services."
