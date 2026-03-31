#!/usr/bin/env python3
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

REQUIRED_MODULES = [
    "app.webapp",
    "app.jobs.run_cycle",
    "app.jobs.run_summary",
    "app.jobs.run_worker",
    "app.jobs.run_schema_job",
    "app.jobs.run_deep_job",
    "app.jobs.run_report_job",
    "app.services.signature_service",
    "app.services.replication_service",
]

REQUIRED_ROUTES = {
    "/",
    "/login",
    "/health",
    "/incidents",
    "/mismatches",
    "/schema-runs",
    "/deep-runs",
    "/settings",
    "/maintenance",
    "/access",
    "/access/audit",
    "/trends",
    "/jobs",
    "/alerts",
    "/reports",
    "/reports/daily",
    "/reports/admin",
    "/reports/executive",
    "/settings/advanced",
}


def verify_imports() -> list[str]:
    loaded = []
    for mod in REQUIRED_MODULES:
        importlib.import_module(mod)
        loaded.append(mod)
    return loaded


def verify_routes() -> list[str]:
    from app.webapp import app

    routes = {rule.rule for rule in app.url_map.iter_rules()}
    missing = sorted(REQUIRED_ROUTES - routes)
    if missing:
        raise AssertionError(f"missing routes: {missing}")
    return sorted(REQUIRED_ROUTES)


def main() -> int:
    imported = verify_imports()
    routes = verify_routes()
    print({
        "status": "ok",
        "project_root": str(PROJECT_ROOT),
        "imported_modules": imported,
        "verified_routes": routes,
        "python": sys.version.split()[0],
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
