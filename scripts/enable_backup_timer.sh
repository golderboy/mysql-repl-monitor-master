#!/usr/bin/env bash
set -euo pipefail
APP_ROOT="${APP_ROOT:-/opt/mysql-repl-monitor}"
cp -f "$APP_ROOT/deploy/mysql-repl-monitor-backup.service" /etc/systemd/system/
cp -f "$APP_ROOT/deploy/mysql-repl-monitor-backup.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now mysql-repl-monitor-backup.timer
systemctl list-timers --all | grep mysql-repl-monitor-backup || true
