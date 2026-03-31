#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

require_cmd mysqldump
require_cmd tar
require_cmd sha256sum
require_backup_env

TS="$(now_ts)"
DEST_DIR="$BACKUP_ROOT/$TS"
mkdir -p "$DEST_DIR"

DB_NAME="${BACKUP_DB_NAME:-${MONITOR_DB_NAME:-db_monitor}}"
DB_DUMP="$DEST_DIR/${DB_NAME}.sql"
EXPORT_TGZ="$DEST_DIR/exports.tar.gz"
META_DIR="$DEST_DIR/meta"
mkdir -p "$META_DIR"

mysqldump   -h"${BACKUP_DB_HOST:-${MONITOR_DB_HOST:-127.0.0.1}}"   -P"${BACKUP_DB_PORT:-${MONITOR_DB_PORT:-3307}}"   -u"${BACKUP_DB_USER:-mon_backup}"   -p"${BACKUP_DB_PASSWORD:-}"   --single-transaction --quick --routines --triggers   "$DB_NAME" > "$DB_DUMP"

if [ -d "$EXPORT_ROOT" ]; then
  tar -C "$EXPORT_ROOT" -czf "$EXPORT_TGZ" .
else
  tar -czf "$EXPORT_TGZ" --files-from /dev/null
fi

if [ -f "$ENV_FILE" ]; then
  redact_file "$ENV_FILE" "$META_DIR/.env.redacted"
fi

mkdir -p "$META_DIR/systemd" "$META_DIR/apache"
cp -f /etc/systemd/system/mysql-repl-monitor-web.service "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-cycle.service "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-cycle.timer "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-summary.service "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-summary.timer "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-worker.service "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/systemd/system/mysql-repl-monitor-worker.timer "$META_DIR/systemd/" 2>/dev/null || true
cp -f /etc/apache2/sites-available/apache-monitor.conf "$META_DIR/apache/" 2>/dev/null || true

sha256sum "$DB_DUMP" "$EXPORT_TGZ" > "$DEST_DIR/SHA256SUMS"

echo "$DEST_DIR"
