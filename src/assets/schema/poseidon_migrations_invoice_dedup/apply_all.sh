#!/usr/bin/env bash
set -euo pipefail
DB_URI="${1:-}"
if [[ -z "$DB_URI" ]]; then
  echo "Usage: $0 postgresql://user:pass@host:port/dbname"
  exit 1
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for f in {001..016}_*.sql; do
  echo "Applying $f ..."
  psql "$DB_URI" -v ON_ERROR_STOP=1 -f "$DIR/$f"
done
echo "All migrations applied."
