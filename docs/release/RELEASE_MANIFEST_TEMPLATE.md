# Release Manifest Template

## Release
- Release number:
- Baseline source:
- Patch date:
- Prepared by:

## Files changed
- path/to/file

## Services/timers to restart
- mysql-repl-monitor-web.service
- mysql-repl-monitor-cycle.timer
- mysql-repl-monitor-summary.timer
- mysql-repl-monitor-worker.timer

## Pre-deploy
- backup created
- preflight passed
- config diff reviewed

## Post-deploy
- import smoke passed
- route smoke passed
- systemd status checked
- rollback instructions verified

## Rollback
- files to restore:
- services to restart:
- expected smoke after rollback:
