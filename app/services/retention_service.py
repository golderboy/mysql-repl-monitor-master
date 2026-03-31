from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from app.repositories.maintenance_repo import get_setting_map

DEFAULT_RETENTION_DAYS: dict[str, int] = {
    "monitor_check_runs": 180,
    "monitor_replication_logs": 180,
    "monitor_table_signature_logs": 120,
    "monitor_schema_runs": 365,
    "monitor_schema_diffs": 365,
    "monitor_deep_compare_runs": 60,
    "monitor_deep_compare_results": 45,
    "monitor_telegram_logs": 365,
}

TIMESTAMP_CANDIDATES: dict[str, list[str]] = {
    "monitor_check_runs": ["started_at", "created_at"],
    "monitor_replication_logs": ["checked_at", "created_at"],
    "monitor_table_signature_logs": ["checked_at", "created_at"],
    "monitor_schema_runs": ["started_at", "created_at"],
    "monitor_schema_diffs": ["created_at", "checked_at"],
    "monitor_deep_compare_runs": ["started_at", "created_at"],
    "monitor_deep_compare_results": ["created_at", "checked_at"],
    "monitor_telegram_logs": ["sent_at", "created_at"],
}


def _env(name: str, fallback: str | None = None) -> str | None:
    return os.getenv(name, fallback)


def get_export_root() -> str:
    return _env("EVIDENCE_EXPORT_DIR", "/opt/mysql-repl-monitor/exports") or "/opt/mysql-repl-monitor/exports"


def get_maintenance_conn():
    return pymysql.connect(
        host=_env("MAINT_DB_HOST", _env("MONITOR_DB_HOST", "127.0.0.1")),
        port=int(_env("MAINT_DB_PORT", _env("MONITOR_DB_PORT", "3307")) or 3307),
        user=_env("MAINT_DB_USER", _env("MONITOR_DB_USER", "mon_app")),
        password=_env("MAINT_DB_PASSWORD", _env("MONITOR_DB_PASSWORD", "")),
        database=_env("MAINT_DB_NAME", _env("MONITOR_DB_NAME", "db_monitor")),
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )


def _detect_timestamp_column(conn, table_name: str) -> str | None:
    candidates = TIMESTAMP_CANDIDATES.get(table_name, ["created_at"])
    sql = """
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name,))
        existing = {row["column_name"] for row in cur.fetchall()}
    for col in candidates:
        if col in existing:
            return col
    return None


def get_retention_plan(read_conn=None) -> list[dict[str, Any]]:
    owns_conn = False
    conn = read_conn
    if conn is None:
        conn = get_maintenance_conn()
        owns_conn = True
    try:
        settings_map = get_setting_map(conn)
        plan: list[dict[str, Any]] = []
        for table_name, default_days in DEFAULT_RETENTION_DAYS.items():
            days = int(settings_map.get(f"retention.{table_name}.days", default_days) or default_days)
            time_column = _detect_timestamp_column(conn, table_name)
            plan.append({
                "table_name": table_name,
                "days": days,
                "time_column": time_column,
            })
        return plan
    finally:
        if owns_conn:
            conn.close()


def _write_cleanup_report(result: dict[str, Any]) -> str:
    root = Path(get_export_root()) / "maintenance_reports"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = root / f"cleanup_{ts}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(path)


def run_retention_cleanup(triggered_by: str = "system", dry_run: bool = False, batch_size: int = 5000) -> dict[str, Any]:
    conn = get_maintenance_conn()
    try:
        plan = get_retention_plan(conn)
        result = {
            "triggered_by": triggered_by,
            "dry_run": dry_run,
            "started_at": datetime.now(),
            "tables": [],
        }
        for item in plan:
            table_name = item["table_name"]
            days = int(item["days"])
            time_column = item["time_column"]
            if days <= 0 or not time_column:
                result["tables"].append({
                    "table_name": table_name,
                    "days": days,
                    "time_column": time_column,
                    "candidate_rows": 0,
                    "deleted_rows": 0,
                    "status": "skipped",
                })
                continue

            cutoff = datetime.now() - timedelta(days=days)
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE `{time_column}` < %s", (cutoff,))
                candidate_rows = int(cur.fetchone()["cnt"] or 0)

            deleted_rows = 0
            if not dry_run and candidate_rows > 0:
                while True:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"DELETE FROM `{table_name}` WHERE `{time_column}` < %s LIMIT {int(batch_size)}",
                            (cutoff,),
                        )
                        batch_deleted = int(cur.rowcount or 0)
                    conn.commit()
                    deleted_rows += batch_deleted
                    if batch_deleted < batch_size:
                        break

            result["tables"].append({
                "table_name": table_name,
                "days": days,
                "time_column": time_column,
                "candidate_rows": candidate_rows,
                "deleted_rows": deleted_rows,
                "status": "ok",
            })

        result["finished_at"] = datetime.now()
        result["report_path"] = _write_cleanup_report(result)
        return result
    finally:
        conn.close()
