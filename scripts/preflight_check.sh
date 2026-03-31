#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

require_cmd systemctl
require_cmd mysql
require_cmd mysqldump
require_cmd tar
require_cmd openssl
require_backup_env

echo "[1/9] env file"
test -f "$ENV_FILE"

echo "[2/9] app root"
test -d "$APP_ROOT/app"

echo "[3/9] systemd services"
systemctl status mysql-repl-monitor-web.service >/dev/null
systemctl status mysql-repl-monitor-cycle.timer >/dev/null

echo "[4/9] monitor db connectivity via web account"
mysql $(mysql_args_monitor_admin) -N -e "SELECT 1" >/dev/null

echo "[5/9] monitor db connectivity via app account"
mysql $(mysql_args_monitor_app) -N -e "SELECT 1" >/dev/null

echo "[6/9] monitor db connectivity via backup account"
mysql $(mysql_args_monitor_backup) -N -e "SELECT 1" >/dev/null

echo "[7/9] current release routes"
curl -ksSf https://127.0.0.1:18443/ >/dev/null || true

echo "[8/9] exports dir"
mkdir -p "$EXPORT_ROOT"

echo "[9/9] backup dir"
mkdir -p "$BACKUP_ROOT"

echo "OK preflight"
