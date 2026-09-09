#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
destination="${1:-backups/postgres-${timestamp}.dump}"
destination_dir="$(dirname "$destination")"

if [[ -e "$destination" || -e "${destination}.sha256" ]]; then
  echo "Refusing to overwrite existing backup: $destination" >&2
  exit 2
fi

mkdir -p "$destination_dir"
partial="$(mktemp "${destination}.partial.XXXXXX")"
cleanup() {
  rm -f -- "$partial"
}
trap cleanup EXIT

echo "Creating PostgreSQL custom-format backup: $destination"
docker compose exec -T db sh -c \
  'exec pg_dump --format=custom --create --no-owner --no-acl -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  > "$partial"

# Refuse to publish a truncated/corrupt archive even when pg_dump happened to exit 0.
docker compose exec -T db sh -c \
  'exec pg_restore --list -U "$POSTGRES_USER"' \
  < "$partial" > /dev/null

mv -- "$partial" "$destination"
trap - EXIT

if command -v sha256sum > /dev/null 2>&1; then
  (
    cd "$destination_dir"
    sha256sum "$(basename "$destination")" > "$(basename "$destination").sha256"
  )
fi

echo "Backup complete: $destination"
