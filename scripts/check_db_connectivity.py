#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class TargetConfig:
    name: str
    host: str
    port: int
    database: str
    user: str
    password: str
    connect_timeout: int = 5
    read_timeout: int = 10


def _target_from_env(name: str) -> TargetConfig:
    if name == "monitor":
        return TargetConfig(
            name="monitor",
            host=os.getenv("MONITOR_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("MONITOR_DB_PORT", "3307")),
            database=os.getenv("MONITOR_DB_NAME", "db_monitor"),
            user=os.getenv("MONITOR_DB_USER", "mon_app"),
            password=os.getenv("MONITOR_DB_PASSWORD", ""),
            connect_timeout=int(os.getenv("PROD_DB_CONNECT_TIMEOUT", "8")),
            read_timeout=int(os.getenv("PROD_DB_READ_TIMEOUT", "30")),
        )
    if name == "web":
        return TargetConfig(
            name="web",
            host=os.getenv("WEB_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("WEB_DB_PORT", "3307")),
            database=os.getenv("WEB_DB_NAME", "db_monitor"),
            user=os.getenv("WEB_DB_USER", "mon_web_admin"),
            password=os.getenv("WEB_DB_PASSWORD", ""),
            connect_timeout=5,
            read_timeout=10,
        )
    if name == "backup":
        return TargetConfig(
            name="backup",
            host=os.getenv("BACKUP_DB_HOST", os.getenv("MONITOR_DB_HOST", "127.0.0.1")),
            port=int(os.getenv("BACKUP_DB_PORT", os.getenv("MONITOR_DB_PORT", "3307"))),
            database=os.getenv("BACKUP_DB_NAME", os.getenv("MONITOR_DB_NAME", "db_monitor")),
            user=os.getenv("BACKUP_DB_USER", "mon_backup"),
            password=os.getenv("BACKUP_DB_PASSWORD", ""),
            connect_timeout=5,
            read_timeout=10,
        )
    raise ValueError(f"unsupported target: {name}")


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}***{value[-2:]}"


def test_target(config: TargetConfig, fail_missing_password: bool = False) -> dict:
    if not config.password:
        status = "failed" if fail_missing_password else "skipped"
        return {
            "target": config.name,
            "status": status,
            "reason": "password_missing",
            "config": {
                **asdict(config),
                "password": _mask(config.password),
            },
        }

    conn = pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=True,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        write_timeout=config.read_timeout,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS database_name, CURRENT_USER() AS current_user")
            row = cur.fetchone() or {}
        return {
            "target": config.name,
            "status": "ok",
            "database_name": row.get("database_name"),
            "current_user": row.get("current_user"),
            "config": {
                **asdict(config),
                "password": _mask(config.password),
            },
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe DB connectivity check for monitor/web/backup accounts")
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=["monitor", "web", "backup"],
        default=["monitor", "web", "backup"],
        help="Targets to verify",
    )
    parser.add_argument(
        "--fail-missing-password",
        action="store_true",
        help="Return non-zero when a target password is missing from environment",
    )
    args = parser.parse_args()

    results = []
    failures = []
    for name in args.targets:
        try:
            result = test_target(_target_from_env(name), fail_missing_password=args.fail_missing_password)
        except Exception as exc:  # pragma: no cover - exercised in real environment
            result = {"target": name, "status": "failed", "reason": str(exc)}
        results.append(result)
        if result["status"] == "failed":
            failures.append(name)

    payload = {
        "status": "ok" if not failures else "failed",
        "targets": results,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
