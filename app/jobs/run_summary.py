from __future__ import annotations

import argparse
import os
import sys
from typing import Any

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.db import cursor, get_monitor_conn
from app.repositories.extra_repo import upsert_daily_stat, upsert_hourly_stat


def _server_code_map(conn) -> dict[int, str]:
    with cursor(conn) as cur:
        cur.execute("SELECT id, server_code FROM monitor_servers WHERE is_active = 1")
        return {int(row["id"]): row["server_code"] for row in cur.fetchall()}


def build_hourly(conn, hours: int) -> list[dict[str, Any]]:
    sql = """
    SELECT DATE_FORMAT(r.checked_at, '%%Y-%%m-%%d %%H:00:00') AS bucket_hour,
           r.server_id,
           MAX(s.server_code) AS server_code,
           MAX(r.seconds_behind_master) AS lag_max_sec,
           ROUND(AVG(CASE WHEN r.seconds_behind_master IS NULL THEN NULL ELSE r.seconds_behind_master END), 2) AS lag_avg_sec,
           SUM(CASE WHEN r.health_status IN ('critical', 'error') THEN 1 ELSE 0 END) AS error_count
      FROM monitor_replication_logs r
      LEFT JOIN monitor_servers s ON s.id = r.server_id
     WHERE r.checked_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
     GROUP BY DATE_FORMAT(r.checked_at, '%%Y-%%m-%%d %%H:00:00'), r.server_id
     ORDER BY bucket_hour ASC, r.server_id ASC
    """
    with cursor(conn) as cur:
        cur.execute(sql, (hours,))
        rows = list(cur.fetchall())
    for row in rows:
        upsert_hourly_stat(conn, row)
    return rows


def build_daily(conn, days: int) -> list[dict[str, Any]]:
    server_codes = _server_code_map(conn)
    rows: dict[tuple[str, int], dict[str, Any]] = {}

    def get_row(stat_date: str, server_id: int) -> dict[str, Any]:
        key = (stat_date, int(server_id))
        row = rows.get(key)
        if row is None:
            row = {
                "stat_date": stat_date,
                "server_id": int(server_id),
                "server_code": server_codes.get(int(server_id), ""),
                "replication_ok_count": 0,
                "replication_warn_count": 0,
                "replication_critical_count": 0,
                "replication_error_count": 0,
                "mismatch_count": 0,
                "incident_opened_count": 0,
                "incident_recovered_count": 0,
            }
            rows[key] = row
        return row

    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT DATE(checked_at) AS stat_date, server_id,
                   SUM(CASE WHEN health_status = 'ok' THEN 1 ELSE 0 END) AS replication_ok_count,
                   SUM(CASE WHEN health_status = 'warn' THEN 1 ELSE 0 END) AS replication_warn_count,
                   SUM(CASE WHEN health_status = 'critical' THEN 1 ELSE 0 END) AS replication_critical_count,
                   SUM(CASE WHEN health_status = 'error' THEN 1 ELSE 0 END) AS replication_error_count
              FROM monitor_replication_logs
             WHERE checked_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
             GROUP BY DATE(checked_at), server_id
            """,
            (days,),
        )
        for raw in cur.fetchall():
            row = get_row(str(raw["stat_date"]), int(raw["server_id"]))
            row["replication_ok_count"] = int(raw.get("replication_ok_count") or 0)
            row["replication_warn_count"] = int(raw.get("replication_warn_count") or 0)
            row["replication_critical_count"] = int(raw.get("replication_critical_count") or 0)
            row["replication_error_count"] = int(raw.get("replication_error_count") or 0)

        cur.execute(
            """
            SELECT DATE(checked_at) AS stat_date, server_id,
                   SUM(CASE WHEN result_status IN ('mismatch', 'error') THEN 1 ELSE 0 END) AS mismatch_count
              FROM monitor_table_signature_logs
             WHERE checked_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
             GROUP BY DATE(checked_at), server_id
            """,
            (days,),
        )
        for raw in cur.fetchall():
            row = get_row(str(raw["stat_date"]), int(raw["server_id"]))
            row["mismatch_count"] = int(raw.get("mismatch_count") or 0)

        cur.execute(
            """
            SELECT DATE(first_detected_at) AS stat_date, server_id, COUNT(*) AS incident_opened_count
              FROM monitor_incidents
             WHERE first_detected_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
               AND server_id IS NOT NULL
             GROUP BY DATE(first_detected_at), server_id
            """,
            (days,),
        )
        for raw in cur.fetchall():
            row = get_row(str(raw["stat_date"]), int(raw["server_id"]))
            row["incident_opened_count"] = int(raw.get("incident_opened_count") or 0)

        cur.execute(
            """
            SELECT DATE(recovered_at) AS stat_date, server_id, COUNT(*) AS incident_recovered_count
              FROM monitor_incidents
             WHERE recovered_at IS NOT NULL
               AND recovered_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
               AND server_id IS NOT NULL
             GROUP BY DATE(recovered_at), server_id
            """,
            (days,),
        )
        for raw in cur.fetchall():
            row = get_row(str(raw["stat_date"]), int(raw["server_id"]))
            row["incident_recovered_count"] = int(raw.get("incident_recovered_count") or 0)

    ordered = [rows[key] for key in sorted(rows.keys())]
    for row in ordered:
        upsert_daily_stat(conn, row)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    conn = get_monitor_conn()
    try:
        hourly_rows = build_hourly(conn, hours=max(1, args.hours))
        daily_rows = build_daily(conn, days=max(1, args.days))
    finally:
        conn.close()

    print(
        {
            "status": "ok",
            "hourly_rows": len(hourly_rows),
            "daily_rows": len(daily_rows),
            "alerts_sent": 0,
            "alerts_skipped": 0,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
