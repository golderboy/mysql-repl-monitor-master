# Baseline Inventory (WS1)

Baseline package: mysql-repl-monitor-v2-complace

## Runtime layout
- app/: Flask app, repositories, services, jobs, SQL migrations, templates
- deploy/: systemd units, timers, Apache config, env example
- scripts/: preflight/postflight, backup/restore, config diff, smoke helpers
- exports/: export targets and maintenance reports
- run/: runtime output including config diff artifacts

## Critical entrypoints that must not regress
- `app/webapp.py`
- `app/jobs/run_cycle.py`
- `app/jobs/run_summary.py`
- `app/jobs/run_worker.py`
- `app/config.py`
- `app/db.py`

## Runtime contracts
- Web entrypoint remains `webapp:app`
- Reverse proxy remains Apache HTTPS -> 127.0.0.1:18080
- Monitor DB remains local MariaDB `db_monitor`
- Production access remains read-only
- Migrations must remain additive-first

## WS1 patch goals
- replace obvious placeholders that weaken evidence integrity
- provide standard smoke/import verification script
- provide release manifest/checklist template
- keep backward compatibility for existing routes and workers
