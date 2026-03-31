from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app.services.event_hash_service import stable_hash

from app.db import cursor


def create_check_run(monitor_conn, run_type: str, triggered_by: str, trigger_user: Optional[str] = None) -> int:
    sql = '''
    INSERT INTO monitor_check_runs (run_type, triggered_by, trigger_user)
    VALUES (%s, %s, %s)
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (run_type, triggered_by, trigger_user))
        monitor_conn.commit()
        return int(cur.lastrowid)


def finish_check_run(monitor_conn, run_id: int, status: str, note: Optional[str] = None) -> None:
    sql = '''
    UPDATE monitor_check_runs
       SET finished_at = CURRENT_TIMESTAMP(6),
           status = %s,
           note = COALESCE(%s, note)
     WHERE id = %s
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (status, note, run_id))
        monitor_conn.commit()


def get_active_servers(monitor_conn) -> List[Dict[str, Any]]:
    sql = '''
    SELECT id, server_code, server_name, role, host, port, db_name, username,
           password_enc AS password_plain
      FROM monitor_servers
     WHERE is_active = 1
     ORDER BY sort_order, id
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def get_server_choices(web_conn) -> List[Dict[str, Any]]:
    sql = '''
    SELECT id, server_code, server_name, role
      FROM monitor_servers
     WHERE is_active = 1
     ORDER BY sort_order, id
    '''
    with cursor(web_conn) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def get_watchlist(monitor_conn) -> List[Dict[str, Any]]:
    sql = '''
    SELECT id, db_name, table_name, enabled, priority, compare_strategy,
           pk_column, updated_at_column, date_column, where_clause,
           tail_rows, signature_sql_override, note
      FROM monitor_table_watchlist
     WHERE enabled = 1
     ORDER BY priority ASC, table_name ASC
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def get_last_replication_event_hash(monitor_conn, server_id: int) -> str | None:
    sql = '''
    SELECT event_hash
      FROM monitor_replication_logs
     WHERE server_id = %s
     ORDER BY id DESC
     LIMIT 1
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (server_id,))
        row = cur.fetchone() or {}
        return row.get("event_hash")


def get_last_signature_event_hash(monitor_conn, server_id: int, db_name: str, table_name: str) -> str | None:
    sql = '''
    SELECT event_hash
      FROM monitor_table_signature_logs
     WHERE server_id = %s
       AND db_name = %s
       AND table_name = %s
     ORDER BY id DESC
     LIMIT 1
    '''
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (server_id, db_name, table_name))
        row = cur.fetchone() or {}
        return row.get("event_hash")


def insert_replication_log(
    monitor_conn,
    check_run_id: int,
    server_id: int,
    payload: Dict[str, Any],
    prev_event_hash: str | None = None,
    event_hash: str | None = None,
) -> int:
    sql = '''
    INSERT INTO monitor_replication_logs (
        check_run_id, server_id, is_connected,
        slave_io_running, slave_sql_running, seconds_behind_master,
        master_log_file, read_master_log_pos, exec_master_log_pos,
        relay_master_log_file, last_io_errno, last_io_error,
        last_sql_errno, last_sql_error, sql_running_state,
        health_status, prev_event_hash, event_hash
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s
    )
    '''
    values = (
        check_run_id,
        server_id,
        int(payload.get("is_connected", 0)),
        payload.get("slave_io_running"),
        payload.get("slave_sql_running"),
        payload.get("seconds_behind_master"),
        payload.get("master_log_file"),
        payload.get("read_master_log_pos"),
        payload.get("exec_master_log_pos"),
        payload.get("relay_master_log_file"),
        payload.get("last_io_errno"),
        payload.get("last_io_error"),
        payload.get("last_sql_errno"),
        payload.get("last_sql_error"),
        payload.get("sql_running_state"),
        payload.get("health_status", "error"),
        prev_event_hash if prev_event_hash is not None else payload.get("prev_event_hash"),
        event_hash or payload.get("event_hash") or stable_hash(payload),
    )
    with cursor(monitor_conn) as cur:
        cur.execute(sql, values)
        monitor_conn.commit()
        return int(cur.lastrowid)


def insert_signature_log(
    monitor_conn,
    check_run_id: int,
    server_id: int,
    db_name: str,
    table_name: str,
    compare_strategy: str,
    master_signature: Dict[str, Any],
    slave_signature: Dict[str, Any],
    result_status: str,
    diff_summary: str | None = None,
    error_message: str | None = None,
    prev_event_hash: str | None = None,
    event_hash: str | None = None,
) -> int:
    payload = {
        "master": master_signature,
        "slave": slave_signature,
    }
    sql = '''
    INSERT INTO monitor_table_signature_logs (
        check_run_id, server_id, db_name, table_name,
        compare_strategy, signature_json, signature_hash,
        result_status, diff_summary, error_message,
        prev_event_hash, event_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, SHA2(%s, 256), %s, %s, %s, %s, %s)
    '''
    signature_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (
                check_run_id,
                server_id,
                db_name,
                table_name,
                compare_strategy,
                signature_json,
                signature_json,
                result_status,
                diff_summary,
                error_message,
                prev_event_hash,
                event_hash or stable_hash({
                    "server_id": server_id,
                    "db_name": db_name,
                    "table_name": table_name,
                    "result_status": result_status,
                    "signature_json": signature_json,
                    "diff_summary": diff_summary,
                    "error_message": error_message,
                }),
            ),
        )
        monitor_conn.commit()
        return int(cur.lastrowid)


def fetch_recent_runs(web_conn, limit: int = 20):
    sql = '''
    SELECT id, run_type, started_at, finished_at, status, triggered_by, trigger_user, note
      FROM monitor_check_runs
     ORDER BY id DESC
     LIMIT %s
    '''
    with cursor(web_conn) as cur:
        cur.execute(sql, (limit,))
        return list(cur.fetchall())


def fetch_open_incidents(web_conn, limit: int = 50):
    sql = '''
    SELECT i.id, i.incident_code, i.issue_type, i.severity, i.current_status,
           i.first_detected_at, i.last_detected_at, s.server_code, i.db_name, i.table_name,
           i.system_summary
      FROM monitor_incidents i
      LEFT JOIN monitor_servers s ON s.id = i.server_id
     WHERE i.current_status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING', 'RECOVERED')
     ORDER BY i.last_detected_at DESC
     LIMIT %s
    '''
    with cursor(web_conn) as cur:
        cur.execute(sql, (limit,))
        return list(cur.fetchall())


def fetch_dashboard_summary(web_conn) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "open_incidents": 0,
        "critical_open": 0,
        "warning_open": 0,
        "recent_mismatches": 0,
        "last_run_status": None,
        "last_run_at": None,
    }
    with cursor(web_conn) as cur:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN current_status IN ('OPEN','ACKNOWLEDGED','INVESTIGATING') THEN 1 ELSE 0 END) AS open_incidents,
                SUM(CASE WHEN current_status IN ('OPEN','ACKNOWLEDGED','INVESTIGATING') AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_open,
                SUM(CASE WHEN current_status IN ('OPEN','ACKNOWLEDGED','INVESTIGATING') AND severity = 'WARNING' THEN 1 ELSE 0 END) AS warning_open
              FROM monitor_incidents
            """
        )
        row = cur.fetchone() or {}
        summary.update({
            "open_incidents": int(row.get("open_incidents") or 0),
            "critical_open": int(row.get("critical_open") or 0),
            "warning_open": int(row.get("warning_open") or 0),
        })

        cur.execute(
            """
            SELECT COUNT(*) AS recent_mismatches
              FROM monitor_table_signature_logs
             WHERE result_status = 'mismatch'
               AND checked_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """
        )
        row = cur.fetchone() or {}
        summary["recent_mismatches"] = int(row.get("recent_mismatches") or 0)

        cur.execute(
            """
            SELECT status, COALESCE(finished_at, started_at) AS last_run_at
              FROM monitor_check_runs
             ORDER BY id DESC
             LIMIT 1
            """
        )
        row = cur.fetchone() or {}
        summary["last_run_status"] = row.get("status")
        summary["last_run_at"] = row.get("last_run_at")
    return summary


def fetch_recent_mismatches(web_conn, limit: int = 100):
    sql = '''
    SELECT l.id, l.checked_at, l.db_name, l.table_name, l.compare_strategy,
           l.result_status, l.diff_summary, s.server_code
      FROM monitor_table_signature_logs l
      LEFT JOIN monitor_servers s ON s.id = l.server_id
     WHERE l.result_status IN ('mismatch', 'error')
     ORDER BY l.id DESC
     LIMIT %s
    '''
    with cursor(web_conn) as cur:
        cur.execute(sql, (limit,))
        return list(cur.fetchall())
