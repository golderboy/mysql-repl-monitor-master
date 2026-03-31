#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

require_cmd sha256sum

if [ $# -lt 1 ]; then
  echo "usage: $0 <backup_dir> [target_db]" >&2
  exit 1
fi

BACKUP_DIR="$1"
TARGET_DB="${2:-db_monitor_restore_test}"

test -d "$BACKUP_DIR"
test -f "$BACKUP_DIR/SHA256SUMS"
cd "$BACKUP_DIR"
sha256sum -c SHA256SUMS

DB_DUMP="$(find "$BACKUP_DIR" -maxdepth 1 -name '*.sql' | head -n1)"
[ -n "$DB_DUMP" ]

echo "restore bundle verified: $BACKUP_DIR"
echo "target restore db name (for DBA procedure): $TARGET_DB"
echo "No database import is performed by this script in production."
echo "Use docs/WS6_BACKUP_RESTORE_DRILL.txt and run the DBA-controlled restore procedure outside app runtime users."
