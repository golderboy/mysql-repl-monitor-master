#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

LATEST="${1:-$(find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d | sort | tail -n1)}"
TEST_DB="${2:-db_monitor_restore_test}"

[ -n "$LATEST" ]
"$SCRIPT_DIR/restore_monitor.sh" "$LATEST" "$TEST_DB"

echo "DBA restore drill required for full WS6 closure."
echo "1) DBA imports backup into $TEST_DB on a non-production target."
echo "2) DBA validates row counts for monitor_servers, monitor_settings, monitor_incidents."
echo "3) DBA drops $TEST_DB after validation."

echo "restore drill package check OK: $TEST_DB from $LATEST"
