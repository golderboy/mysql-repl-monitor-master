#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

require_cmd systemctl
require_cmd curl

systemctl is-active --quiet mysql-repl-monitor-web.service
systemctl is-active --quiet mysql-repl-monitor-cycle.timer

curl -sSf http://127.0.0.1:18080/ >/dev/null
curl -sSf http://127.0.0.1:18080/health >/dev/null || true
curl -sSf http://127.0.0.1:18080/trends >/dev/null || true
curl -sSf http://127.0.0.1:18080/jobs >/dev/null || true
curl -sSf http://127.0.0.1:18080/alerts >/dev/null || true

echo "OK postflight"
