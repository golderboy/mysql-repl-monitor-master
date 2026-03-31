from __future__ import annotations

import json
from typing import Any

from app.db import cursor


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def list_hourly_stats(conn, hours: int = 24, server_id: int | None = None):
    sql = """
    SELECT bucket_hour, server_id, server_code, lag_max_sec, lag_avg_sec, error_count, created_at
      FROM monitor_hourly_lag_stats
     WHERE bucket_hour >= DATE_SUB(DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:00:00'), INTERVAL %s HOUR)
    """
    args: list[Any] = [hours]
    if server_id is not None:
        sql += " AND server_id = %s"
        args.append(server_id)
    sql += " ORDER BY bucket_hour ASC, server_id ASC"
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def list_daily_stats(conn, days: int = 7, server_id: int | None = None):
    sql = """
    SELECT stat_date, server_id, server_code,
           replication_ok_count, replication_warn_count, replication_critical_count, replication_error_count,
           mismatch_count, incident_opened_count, incident_recovered_count, created_at
      FROM monitor_daily_stats
     WHERE stat_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
    """
    args: list[Any] = [days]
    if server_id is not None:
        sql += " AND server_id = %s"
        args.append(server_id)
    sql += " ORDER BY stat_date ASC, server_id ASC"
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def upsert_hourly_stat(conn, row: dict[str, Any]) -> None:
    sql = """
    INSERT INTO monitor_hourly_lag_stats (
        bucket_hour, server_id, server_code, lag_max_sec, lag_avg_sec, error_count
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        server_code = VALUES(server_code),
        lag_max_sec = VALUES(lag_max_sec),
        lag_avg_sec = VALUES(lag_avg_sec),
        error_count = VALUES(error_count),
        created_at = CURRENT_TIMESTAMP(6)
    """
    with cursor(conn) as cur:
        cur.execute(
            sql,
            (
                row["bucket_hour"],
                row["server_id"],
                row.get("server_code"),
                row.get("lag_max_sec"),
                row.get("lag_avg_sec"),
                row.get("error_count", 0),
            ),
        )
    conn.commit()


def upsert_daily_stat(conn, row: dict[str, Any]) -> None:
    sql = """
    INSERT INTO monitor_daily_stats (
        stat_date, server_id, server_code,
        replication_ok_count, replication_warn_count, replication_critical_count, replication_error_count,
        mismatch_count, incident_opened_count, incident_recovered_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        server_code = VALUES(server_code),
        replication_ok_count = VALUES(replication_ok_count),
        replication_warn_count = VALUES(replication_warn_count),
        replication_critical_count = VALUES(replication_critical_count),
        replication_error_count = VALUES(replication_error_count),
        mismatch_count = VALUES(mismatch_count),
        incident_opened_count = VALUES(incident_opened_count),
        incident_recovered_count = VALUES(incident_recovered_count),
        created_at = CURRENT_TIMESTAMP(6)
    """
    with cursor(conn) as cur:
        cur.execute(
            sql,
            (
                row["stat_date"],
                row["server_id"],
                row.get("server_code"),
                row.get("replication_ok_count", 0),
                row.get("replication_warn_count", 0),
                row.get("replication_critical_count", 0),
                row.get("replication_error_count", 0),
                row.get("mismatch_count", 0),
                row.get("incident_opened_count", 0),
                row.get("incident_recovered_count", 0),
            ),
        )
    conn.commit()


def queue_job_run(conn, job_name: str, requested_by: str, payload: dict[str, Any] | None = None, server_id: int | None = None) -> int:
    sql = """
    INSERT INTO monitor_job_runs (
        job_name, status, requested_by, server_id, payload_json
    ) VALUES (%s, 'queued', %s, %s, %s)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (job_name, requested_by, server_id, _json(payload or {})))
        run_id = int(cur.lastrowid)
    conn.commit()
    return run_id


def list_job_runs(conn, status: str | None = None, limit: int = 100):
    sql = """
    SELECT id, job_name, status, requested_by, server_id,
           requested_at, started_at, finished_at,
           progress_percent, cancel_requested, error_message,
           payload_json, result_json, updated_at
      FROM monitor_job_runs
     WHERE 1=1
    """
    args: list[Any] = []
    if status:
        sql += " AND status = %s"
        args.append(status)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def get_job_run_detail(conn, run_id: int):
    sql = """
    SELECT id, job_name, status, requested_by, server_id,
           requested_at, started_at, finished_at,
           progress_percent, cancel_requested, error_message,
           payload_json, result_json, updated_at
      FROM monitor_job_runs
     WHERE id = %s
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return cur.fetchone()


def cancel_job_run(conn, run_id: int) -> int:
    sql = """
    UPDATE monitor_job_runs
       SET cancel_requested = 1,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
       AND status IN ('queued', 'running')
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        affected = cur.rowcount
    conn.commit()
    return affected


def claim_next_job(conn):
    select_sql = """
    SELECT id, job_name, requested_by, server_id, payload_json, cancel_requested
      FROM monitor_job_runs
     WHERE status = 'queued'
     ORDER BY id ASC
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(select_sql)
        row = cur.fetchone()
        if not row:
            return None
        update_sql = """
        UPDATE monitor_job_runs
           SET status = 'running',
               started_at = CURRENT_TIMESTAMP(6),
               progress_percent = 5,
               updated_at = CURRENT_TIMESTAMP(6)
         WHERE id = %s AND status = 'queued'
        """
        cur.execute(update_sql, (row["id"],))
        if cur.rowcount != 1:
            conn.commit()
            return None
    conn.commit()
    return row


def update_job_progress(conn, run_id: int, progress_percent: int, result: dict[str, Any] | None = None) -> None:
    sql = """
    UPDATE monitor_job_runs
       SET progress_percent = %s,
           result_json = COALESCE(%s, result_json),
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (progress_percent, _json(result) if result is not None else None, run_id))
    conn.commit()


def finish_job_success(conn, run_id: int, result: dict[str, Any] | None = None) -> None:
    sql = """
    UPDATE monitor_job_runs
       SET status = 'success',
           finished_at = CURRENT_TIMESTAMP(6),
           progress_percent = 100,
           result_json = %s,
           error_message = NULL,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (_json(result or {}), run_id))
    conn.commit()


def finish_job_failed(conn, run_id: int, error_message: str, result: dict[str, Any] | None = None) -> None:
    sql = """
    UPDATE monitor_job_runs
       SET status = 'failed',
           finished_at = CURRENT_TIMESTAMP(6),
           progress_percent = progress_percent,
           result_json = %s,
           error_message = %s,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (_json(result or {}), error_message[:2000], run_id))
    conn.commit()


def finish_job_canceled(conn, run_id: int, result: dict[str, Any] | None = None) -> None:
    sql = """
    UPDATE monitor_job_runs
       SET status = 'canceled',
           finished_at = CURRENT_TIMESTAMP(6),
           result_json = %s,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (_json(result or {}), run_id))
    conn.commit()


def is_cancel_requested(conn, run_id: int) -> bool:
    sql = "SELECT cancel_requested FROM monitor_job_runs WHERE id = %s LIMIT 1"
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        row = cur.fetchone() or {}
    return bool(row.get("cancel_requested"))


def acquire_job_lock(conn, lock_name: str, run_id: int) -> bool:
    sql = """
    INSERT INTO monitor_job_locks (lock_name, run_id, acquired_at, heartbeat_at)
    VALUES (%s, %s, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
    ON DUPLICATE KEY UPDATE
        run_id = IF(run_id IS NULL, VALUES(run_id), run_id),
        acquired_at = IF(run_id IS NULL, VALUES(acquired_at), acquired_at),
        heartbeat_at = IF(run_id IS NULL, VALUES(heartbeat_at), heartbeat_at)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (lock_name, run_id))
        cur.execute("SELECT run_id FROM monitor_job_locks WHERE lock_name = %s", (lock_name,))
        row = cur.fetchone() or {}
    conn.commit()
    return int(row.get("run_id") or 0) == int(run_id)


def release_job_lock(conn, lock_name: str, run_id: int) -> None:
    sql = """
    UPDATE monitor_job_locks
       SET run_id = NULL,
           acquired_at = NULL,
           heartbeat_at = NULL,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE lock_name = %s AND run_id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (lock_name, run_id))
    conn.commit()


def add_alert_policy(conn, policy_name: str, issue_type: str, severity: str, channel: str, repeat_minutes: int, quiet_hours: Any, is_enabled: int) -> int:
    sql = """
    INSERT INTO monitor_alert_policies (
        policy_name, issue_type, severity, channel, repeat_minutes, quiet_hours_json, is_enabled
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (policy_name, issue_type, severity, channel, repeat_minutes, _json(quiet_hours), is_enabled))
        policy_id = int(cur.lastrowid)
    conn.commit()
    return policy_id


def add_alert_target(conn, policy_id: int, target_type: str, target_value: str, is_enabled: int) -> int:
    sql = """
    INSERT INTO monitor_alert_targets (
        policy_id, target_type, target_value, is_enabled
    ) VALUES (%s, %s, %s, %s)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (policy_id, target_type, target_value, is_enabled))
        target_id = int(cur.lastrowid)
    conn.commit()
    return target_id


def get_alert_overview(conn):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, policy_name, issue_type, severity, channel, repeat_minutes,
                   quiet_hours_json, is_enabled, created_at, updated_at
              FROM monitor_alert_policies
             ORDER BY id ASC
            """
        )
        policies = list(cur.fetchall())
        cur.execute(
            """
            SELECT id, policy_id, target_type, target_value, is_enabled, created_at, updated_at
              FROM monitor_alert_targets
             ORDER BY policy_id ASC, id ASC
            """
        )
        targets = list(cur.fetchall())
    return {"policies": policies, "targets": targets}


def add_web_audit_log(conn, actor: str, action: str, object_type: str, object_id: int | None, details: dict[str, Any] | None = None) -> int:
    sql = """
    INSERT INTO monitor_web_audit_logs (actor, action, object_type, object_id, details_json)
    VALUES (%s, %s, %s, %s, %s)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (actor, action, object_type, object_id, _json(details or {})))
        row_id = int(cur.lastrowid)
    conn.commit()
    return row_id
