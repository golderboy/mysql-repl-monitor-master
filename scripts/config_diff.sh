#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

REF_DIR="${1:-$APP_ROOT/deploy}"
OUT_DIR="${2:-$APP_ROOT/run/config_diff}"
mkdir -p "$OUT_DIR"

if [ -f "$ENV_FILE" ]; then
  redact_file "$ENV_FILE" "$OUT_DIR/current.env.redacted"
fi

for f in mysql-repl-monitor-web.service mysql-repl-monitor-cycle.service mysql-repl-monitor-cycle.timer mysql-repl-monitor-summary.service mysql-repl-monitor-summary.timer mysql-repl-monitor-worker.service mysql-repl-monitor-worker.timer; do
  cp -f "/etc/systemd/system/$f" "$OUT_DIR/$f.current" 2>/dev/null || true
  cp -f "$REF_DIR/$f" "$OUT_DIR/$f.reference" 2>/dev/null || true
  if [ -f "$OUT_DIR/$f.current" ] || [ -f "$OUT_DIR/$f.reference" ]; then
    diff -u "$OUT_DIR/$f.reference" "$OUT_DIR/$f.current" > "$OUT_DIR/$f.diff" 2>/dev/null || true
  fi
done

cp -f /etc/apache2/sites-available/apache-monitor.conf "$OUT_DIR/apache-monitor.current" 2>/dev/null || true
cp -f "$REF_DIR/apache-monitor.conf" "$OUT_DIR/apache-monitor.reference" 2>/dev/null || true
if [ -f "$OUT_DIR/apache-monitor.current" ] || [ -f "$OUT_DIR/apache-monitor.reference" ]; then
  diff -u "$OUT_DIR/apache-monitor.reference" "$OUT_DIR/apache-monitor.current" > "$OUT_DIR/apache-monitor.diff" 2>/dev/null || true
fi

echo "$OUT_DIR"
