#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/opt/mysql-repl-monitor}"
ENV_FILE="${ENV_FILE:-$APP_ROOT/.env}"
BACKUP_ROOT="${BACKUP_ROOT:-$APP_ROOT/backups}"
EXPORT_ROOT="${EXPORT_ROOT:-$APP_ROOT/exports}"
RUNBOOK_VERSION="ws6-20260329b"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
}

now_ts() {
  date +%Y%m%d_%H%M%S
}

redact_file() {
  local src="$1"
  local dst="$2"
  sed -E     -e 's/^(.*PASSWORD=).*/***REDACTED***/'     -e 's/^(.*SECRET.*=).*/***REDACTED***/'     -e 's/^(.*TOKEN=).*/***REDACTED***/'     -e 's/^(.*CHAT_ID=).*/***REDACTED***/'     "$src" > "$dst"
}

mysql_args_monitor_app() {
  printf -- "-h%s -P%s -u%s -p%s"     "${MONITOR_DB_HOST:-127.0.0.1}"     "${MONITOR_DB_PORT:-3307}"     "${MONITOR_DB_USER:-mon_app}"     "${MONITOR_DB_PASSWORD:-}"
}

mysql_args_monitor_admin() {
  printf -- "-h%s -P%s -u%s -p%s"     "${WEB_DB_HOST:-127.0.0.1}"     "${WEB_DB_PORT:-3307}"     "${WEB_DB_USER:-mon_web_admin}"     "${WEB_DB_PASSWORD:-}"
}

mysql_args_monitor_backup() {
  printf -- "-h%s -P%s -u%s -p%s"     "${BACKUP_DB_HOST:-${MONITOR_DB_HOST:-127.0.0.1}}"     "${BACKUP_DB_PORT:-${MONITOR_DB_PORT:-3307}}"     "${BACKUP_DB_USER:-mon_backup}"     "${BACKUP_DB_PASSWORD:-}"
}

require_backup_env() {
  : "${BACKUP_DB_USER:=mon_backup}"
  if [ -z "${BACKUP_DB_PASSWORD:-}" ]; then
    echo "BACKUP_DB_PASSWORD is not set in .env; production backup must use mon_backup credentials." >&2
    exit 1
  fi
}
