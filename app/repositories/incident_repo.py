from __future__ import annotations

from typing import Any, Dict, Optional

from app.db import cursor


def get_incident_by_code(monitor_conn, incident_code: str) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT *
      FROM monitor_incidents
     WHERE incident_code = %s
     LIMIT 1
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (incident_code,))
        return cur.fetchone()


def get_incident_detail(conn, incident_id: int) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT i.*, s.server_code, s.server_name, s.role AS server_role
      FROM monitor_incidents i
      LEFT JOIN monitor_servers s ON s.id = i.server_id
     WHERE i.id = %s
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(sql, (incident_id,))
        return cur.fetchone()


def list_incidents(
    conn,
    status: str | None = None,
    issue_type: str | None = None,
    severity: str | None = None,
    server_id: int | None = None,
    keyword: str | None = None,
    limit: int = 200,
):
    sql = """
    SELECT i.id, i.incident_code, i.issue_type, i.severity, i.current_status,
           i.first_detected_at, i.last_detected_at, i.recovered_at, i.closed_at,
           i.occurrence_count, i.system_summary, i.owner,
           s.server_code, s.server_name
      FROM monitor_incidents i
      LEFT JOIN monitor_servers s ON s.id = i.server_id
     WHERE 1=1
    """
    args: list[Any] = []
    if status:
        sql += " AND i.current_status = %s"
        args.append(status)
    if issue_type:
        sql += " AND i.issue_type = %s"
        args.append(issue_type)
    if severity:
        sql += " AND i.severity = %s"
        args.append(severity)
    if server_id:
        sql += " AND i.server_id = %s"
        args.append(server_id)
    if keyword:
        sql += " AND (i.incident_code LIKE %s OR i.system_summary LIKE %s OR COALESCE(i.db_name,'') LIKE %s OR COALESCE(i.table_name,'') LIKE %s)"
        like = f"%{keyword}%"
        args.extend([like, like, like, like])
    sql += " ORDER BY i.last_detected_at DESC, i.id DESC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def get_incident_events(conn, incident_id: int, limit: int = 200):
    sql = """
    SELECT id, incident_id, event_time, event_type, old_status, new_status,
           message, created_by, created_at
      FROM monitor_incident_events
     WHERE incident_id = %s
     ORDER BY id DESC
     LIMIT %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (incident_id, limit))
        return list(cur.fetchall())


def get_incident_notes(conn, incident_id: int, limit: int = 100):
    sql = """
    SELECT id, incident_id, note_type, note_text, created_by, created_at, updated_at
      FROM monitor_incident_notes
     WHERE incident_id = %s
     ORDER BY id DESC
     LIMIT %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (incident_id, limit))
        return list(cur.fetchall())


def create_incident(
    monitor_conn,
    incident_code: str,
    issue_type: str,
    severity: str,
    server_id: int | None,
    db_name: str | None,
    table_name: str | None,
    system_summary: str,
) -> int:
    sql = """
    INSERT INTO monitor_incidents (
        incident_code, issue_type, severity, server_id, db_name, table_name,
        first_detected_at, last_detected_at, current_status, system_summary
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 'OPEN', %s
    )
    """
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (incident_code, issue_type, severity, server_id, db_name, table_name, system_summary),
        )
        monitor_conn.commit()
        return int(cur.lastrowid)


def touch_incident_detected(monitor_conn, incident_id: int, severity: str, system_summary: str) -> None:
    sql = """
    UPDATE monitor_incidents
       SET last_detected_at = CURRENT_TIMESTAMP(6),
           occurrence_count = occurrence_count + 1,
           severity = %s,
           system_summary = %s,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (severity, system_summary, incident_id))
        monitor_conn.commit()


def recover_incident(monitor_conn, incident_id: int, system_summary: str) -> None:
    sql = """
    UPDATE monitor_incidents
       SET current_status = 'RECOVERED',
           recovered_at = CURRENT_TIMESTAMP(6),
           system_summary = %s,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
       AND current_status IN ('OPEN','ACKNOWLEDGED','INVESTIGATING')
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (system_summary, incident_id))
        monitor_conn.commit()


def add_incident_event(
    monitor_conn,
    incident_id: int,
    event_type: str,
    message: str,
    event_hash: str,
    prev_event_hash: str | None = None,
    created_by: str = "system",
    old_status: Optional[str] = None,
    new_status: Optional[str] = None,
) -> int:
    sql = """
    INSERT INTO monitor_incident_events (
        incident_id, event_type, old_status, new_status, message, created_by,
        prev_event_hash, event_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (incident_id, event_type, old_status, new_status, message, created_by, prev_event_hash, event_hash),
        )
        monitor_conn.commit()
        return int(cur.lastrowid)


def get_last_event_hash(monitor_conn, incident_id: int) -> str | None:
    sql = """
    SELECT event_hash
      FROM monitor_incident_events
     WHERE incident_id = %s
     ORDER BY id DESC
     LIMIT 1
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (incident_id,))
        row = cur.fetchone()
        return row["event_hash"] if row else None


def count_recent_signature_mismatches(
    monitor_conn,
    server_id: int,
    db_name: str,
    table_name: str,
    rounds: int,
) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
      FROM (
        SELECT result_status
          FROM monitor_table_signature_logs
         WHERE server_id = %s
           AND db_name = %s
           AND table_name = %s
         ORDER BY id DESC
         LIMIT %s
      ) t
     WHERE result_status = 'mismatch'
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (server_id, db_name, table_name, rounds))
        row = cur.fetchone()
        return int(row["cnt"] or 0)


def add_incident_note(conn, incident_id: int, note_type: str, note_text: str, created_by: str) -> int:
    sql = """
    INSERT INTO monitor_incident_notes (incident_id, note_type, note_text, created_by)
    VALUES (%s, %s, %s, %s)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (incident_id, note_type, note_text, created_by))
        conn.commit()
        return int(cur.lastrowid)


def update_incident_fields(
    conn,
    incident_id: int,
    current_status: str | None = None,
    owner: str | None = None,
    root_cause: str | None = None,
    corrective_action: str | None = None,
    summary_result: str | None = None,
) -> None:
    updates = []
    args: list[Any] = []
    if current_status is not None:
        updates.append("current_status = %s")
        args.append(current_status)
        if current_status == 'RECOVERED':
            updates.append("recovered_at = COALESCE(recovered_at, CURRENT_TIMESTAMP(6))")
        if current_status == 'CLOSED':
            updates.append("closed_at = CURRENT_TIMESTAMP(6)")
    if owner is not None:
        updates.append("owner = %s")
        args.append(owner)
    if root_cause is not None:
        updates.append("root_cause = %s")
        args.append(root_cause)
    if corrective_action is not None:
        updates.append("corrective_action = %s")
        args.append(corrective_action)
    if summary_result is not None:
        updates.append("summary_result = %s")
        args.append(summary_result)
    updates.append("updated_at = CURRENT_TIMESTAMP(6)")
    sql = f"UPDATE monitor_incidents SET {', '.join(updates)} WHERE id = %s"
    args.append(incident_id)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        conn.commit()

def count_recent_signature_ok(
    monitor_conn,
    server_id: int,
    db_name: str,
    table_name: str,
    rounds: int,
) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
      FROM (
        SELECT result_status
          FROM monitor_table_signature_logs
         WHERE server_id = %s
           AND db_name = %s
           AND table_name = %s
         ORDER BY id DESC
         LIMIT %s
      ) t
     WHERE result_status = 'match'
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (server_id, db_name, table_name, rounds))
        row = cur.fetchone()
        return int(row["cnt"] or 0)


def count_recent_replication_ok(monitor_conn, server_id: int, rounds: int) -> int:
    sql = """
    SELECT COUNT(*) AS cnt
      FROM (
        SELECT health_status
          FROM monitor_replication_logs
         WHERE server_id = %s
         ORDER BY id DESC
         LIMIT %s
      ) t
     WHERE health_status = 'ok'
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (server_id, rounds))
        row = cur.fetchone()
        return int(row["cnt"] or 0)
