from __future__ import annotations

from typing import Any

from app.db import cursor


def create_schema_run(monitor_conn, target_server_id: int, triggered_by: str) -> int:
    sql = """
    INSERT INTO monitor_schema_runs (triggered_by, target_server_id, status)
    VALUES (%s, %s, 'running')
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (triggered_by, target_server_id))
        monitor_conn.commit()
        return int(cur.lastrowid)


def finish_schema_run(monitor_conn, run_id: int, status: str, summary: str | None = None) -> None:
    sql = """
    UPDATE monitor_schema_runs
       SET finished_at = CURRENT_TIMESTAMP(6),
           status = %s,
           summary = %s
     WHERE id = %s
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (status, summary, run_id))
        monitor_conn.commit()


def insert_schema_diff(
    monitor_conn,
    schema_run_id: int,
    server_id: int,
    db_name: str,
    table_name: str,
    diff_type: str,
    object_type: str,
    object_name: str | None,
    master_value: str | None,
    slave_value: str | None,
    diff_summary: str | None,
    prev_event_hash: str | None,
    event_hash: str,
) -> int:
    sql = """
    INSERT INTO monitor_schema_diffs (
        schema_run_id, server_id, db_name, table_name,
        diff_type, object_type, object_name,
        master_value, slave_value, diff_summary,
        prev_event_hash, event_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (
                schema_run_id,
                server_id,
                db_name,
                table_name,
                diff_type,
                object_type,
                object_name,
                master_value,
                slave_value,
                diff_summary,
                prev_event_hash,
                event_hash,
            ),
        )
        monitor_conn.commit()
        return int(cur.lastrowid)


def list_schema_runs(conn, limit: int = 100):
    sql = """
    SELECT r.id, r.started_at, r.finished_at, r.triggered_by, r.target_server_id,
           r.status, r.summary, s.server_code, s.server_name,
           COUNT(d.id) AS diff_count
      FROM monitor_schema_runs r
      JOIN monitor_servers s ON s.id = r.target_server_id
      LEFT JOIN monitor_schema_diffs d ON d.schema_run_id = r.id
     GROUP BY r.id, r.started_at, r.finished_at, r.triggered_by, r.target_server_id,
              r.status, r.summary, s.server_code, s.server_name
     ORDER BY r.id DESC
     LIMIT %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (limit,))
        return list(cur.fetchall())


def get_schema_run_detail(conn, run_id: int):
    sql = """
    SELECT r.id, r.started_at, r.finished_at, r.triggered_by, r.target_server_id,
           r.status, r.summary, s.server_code, s.server_name, s.db_name,
           COUNT(d.id) AS diff_count
      FROM monitor_schema_runs r
      JOIN monitor_servers s ON s.id = r.target_server_id
      LEFT JOIN monitor_schema_diffs d ON d.schema_run_id = r.id
     WHERE r.id = %s
     GROUP BY r.id, r.started_at, r.finished_at, r.triggered_by, r.target_server_id,
              r.status, r.summary, s.server_code, s.server_name, s.db_name
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return cur.fetchone()


def list_schema_diffs(
    conn,
    run_id: int,
    table_name: str | None = None,
    diff_type: str | None = None,
    limit: int = 1000,
):
    sql = """
    SELECT id, schema_run_id, server_id, db_name, table_name, diff_type,
           object_type, object_name, master_value, slave_value,
           diff_summary, created_at
      FROM monitor_schema_diffs
     WHERE schema_run_id = %s
    """
    args: list[Any] = [run_id]
    if table_name:
        sql += " AND table_name = %s"
        args.append(table_name)
    if diff_type:
        sql += " AND diff_type = %s"
        args.append(diff_type)
    sql += " ORDER BY table_name ASC, id ASC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def list_schema_tables(conn, run_id: int):
    sql = """
    SELECT table_name, COUNT(*) AS diff_count
      FROM monitor_schema_diffs
     WHERE schema_run_id = %s
     GROUP BY table_name
     ORDER BY table_name ASC
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return list(cur.fetchall())


def list_schema_diff_types(conn, run_id: int):
    sql = """
    SELECT diff_type, COUNT(*) AS diff_count
      FROM monitor_schema_diffs
     WHERE schema_run_id = %s
     GROUP BY diff_type
     ORDER BY diff_type ASC
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return list(cur.fetchall())
