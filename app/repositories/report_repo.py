from __future__ import annotations

import json
from typing import Any

from app.db import cursor


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def create_report_export_request(
    conn,
    *,
    report_type: str,
    export_format: str,
    requested_by: str,
    period_days: int,
    payload: dict[str, Any] | None = None,
) -> tuple[int, int]:
    payload = dict(payload or {})
    payload.setdefault("report_type", report_type)
    payload.setdefault("export_format", export_format)
    payload.setdefault("period_days", period_days)

    with cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO monitor_job_runs (
                job_name, status, requested_by, payload_json
            ) VALUES ('report_export', 'queued', %s, %s)
            """,
            (requested_by, _json(payload)),
        )
        job_run_id = int(cur.lastrowid)
        cur.execute(
            """
            INSERT INTO monitor_report_exports (
                job_run_id, report_type, export_format, period_days,
                requested_by, status, request_payload_json
            ) VALUES (%s, %s, %s, %s, %s, 'queued', %s)
            """,
            (job_run_id, report_type, export_format, period_days, requested_by, _json(payload)),
        )
        report_id = int(cur.lastrowid)
    conn.commit()
    return job_run_id, report_id


def list_report_exports(conn, limit: int = 100):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, job_run_id, report_type, export_format, period_days,
                   requested_by, status, file_name, file_relpath,
                   file_size_bytes, sha256_hex, error_message,
                   generated_at, created_at, updated_at
              FROM monitor_report_exports
             ORDER BY id DESC
             LIMIT %s
            """,
            (limit,),
        )
        return list(cur.fetchall())


def get_report_export(conn, report_id: int):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, job_run_id, report_type, export_format, period_days,
                   requested_by, status, file_name, file_relpath,
                   file_size_bytes, sha256_hex, manifest_json,
                   error_message, generated_at, created_at, updated_at
              FROM monitor_report_exports
             WHERE id = %s
             LIMIT 1
            """,
            (report_id,),
        )
        return cur.fetchone()


def get_report_export_by_job_run(conn, job_run_id: int):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, job_run_id, report_type, export_format, period_days,
                   requested_by, status, file_name, file_relpath,
                   file_size_bytes, sha256_hex, manifest_json,
                   error_message, generated_at, created_at, updated_at
              FROM monitor_report_exports
             WHERE job_run_id = %s
             LIMIT 1
            """,
            (job_run_id,),
        )
        return cur.fetchone()


def mark_report_running(conn, report_id: int) -> None:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_report_exports
               SET status = 'running',
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (report_id,),
        )
    conn.commit()


def finish_report_success(
    conn,
    report_id: int,
    *,
    file_name: str,
    file_relpath: str,
    file_size_bytes: int,
    sha256_hex: str,
    manifest: dict[str, Any],
) -> None:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_report_exports
               SET status = 'success',
                   file_name = %s,
                   file_relpath = %s,
                   file_size_bytes = %s,
                   sha256_hex = %s,
                   manifest_json = %s,
                   error_message = NULL,
                   generated_at = CURRENT_TIMESTAMP(6),
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (file_name, file_relpath, file_size_bytes, sha256_hex, _json(manifest), report_id),
        )
    conn.commit()


def finish_report_failed(conn, report_id: int, error_message: str) -> None:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_report_exports
               SET status = 'failed',
                   error_message = %s,
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (error_message[:2000], report_id),
        )
    conn.commit()


def list_report_daily_rows(conn, days: int = 7):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT stat_date, server_id, server_code,
                   replication_ok_count, replication_warn_count,
                   replication_critical_count, replication_error_count,
                   mismatch_count, incident_opened_count, incident_recovered_count
              FROM monitor_daily_stats
             WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
             ORDER BY stat_date ASC, server_code ASC
            """,
            (days,),
        )
        return list(cur.fetchall())


def list_report_summary_rows(conn, days: int = 7):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT server_id, server_code,
                   SUM(replication_ok_count) AS replication_ok_count,
                   SUM(replication_warn_count) AS replication_warn_count,
                   SUM(replication_critical_count) AS replication_critical_count,
                   SUM(replication_error_count) AS replication_error_count,
                   SUM(mismatch_count) AS mismatch_count,
                   SUM(incident_opened_count) AS incident_opened_count,
                   SUM(incident_recovered_count) AS incident_recovered_count
              FROM monitor_daily_stats
             WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
             GROUP BY server_id, server_code
             ORDER BY server_code ASC
            """,
            (days,),
        )
        return list(cur.fetchall())


def finish_report_canceled(conn, report_id: int, error_message: str | None = None) -> None:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_report_exports
               SET status = 'canceled',
                   error_message = %s,
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            ((error_message or '')[:2000] or None, report_id),
        )
    conn.commit()
