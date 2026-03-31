#!/usr/bin/env bash
set -euo pipefail
cp -f /opt/mysql-repl-monitor/deploy/mysql-repl-monitor-summary.service /etc/systemd/system/
cp -f /opt/mysql-repl-monitor/deploy/mysql-repl-monitor-summary.timer /etc/systemd/system/
cp -f /opt/mysql-repl-monitor/deploy/mysql-repl-monitor-worker.service /etc/systemd/system/
cp -f /opt/mysql-repl-monitor/deploy/mysql-repl-monitor-worker.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now mysql-repl-monitor-summary.timer
systemctl enable --now mysql-repl-monitor-worker.timer
systemctl list-timers --all | grep mysql-repl-monitor || true
