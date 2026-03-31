from __future__ import annotations

from typing import Any

from app.db import cursor


def create_deep_run(
    monitor_conn,
    server_id: int,
    db_name: str,
    table_name: str,
    pk_column: str,
    compare_scope: str | None,
    chunk_size: int,
    triggered_by: str,
) -> int:
    sql = """
    INSERT INTO monitor_deep_compare_runs (
        triggered_by, server_id, db_name, table_name,
        pk_column, compare_scope, chunk_size, status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'running')
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (triggered_by, server_id, db_name, table_name, pk_column, compare_scope, chunk_size))
        monitor_conn.commit()
        return int(cur.lastrowid)


def finish_deep_run(monitor_conn, run_id: int, status: str, summary: str | None = None) -> None:
    sql = """
    UPDATE monitor_deep_compare_runs
       SET finished_at = CURRENT_TIMESTAMP(6),
           status = %s,
           summary = %s
     WHERE id = %s
    """
    with cursor(monitor_conn) as cur:
        cur.execute(sql, (status, summary, run_id))
        monitor_conn.commit()


def insert_deep_result(
    monitor_conn,
    deep_run_id: int,
    chunk_no: int,
    pk_start: str | None,
    pk_end: str | None,
    master_hash: str | None,
    slave_hash: str | None,
    master_count: int | None,
    slave_count: int | None,
    result_status: str,
    diff_summary: str | None,
    prev_event_hash: str | None,
    event_hash: str,
) -> int:
    sql = """
    INSERT INTO monitor_deep_compare_results (
        deep_run_id, chunk_no, pk_start, pk_end,
        master_hash, slave_hash, master_count, slave_count,
        result_status, diff_summary, prev_event_hash, event_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (
                deep_run_id,
                chunk_no,
                pk_start,
                pk_end,
                master_hash,
                slave_hash,
                master_count,
                slave_count,
                result_status,
                diff_summary,
                prev_event_hash,
                event_hash,
            ),
        )
        monitor_conn.commit()
        return int(cur.lastrowid)


def list_deep_runs(conn, status: str | None = None, limit: int = 100):
    sql = """
    SELECT r.id, r.started_at, r.finished_at, r.triggered_by,
           r.server_id, r.db_name, r.table_name, r.pk_column,
           r.compare_scope, r.chunk_size, r.status, r.summary,
           s.server_code, s.server_name,
           COUNT(d.id) AS chunk_count,
           SUM(CASE WHEN d.result_status = 'mismatch' THEN 1 ELSE 0 END) AS mismatch_count,
           SUM(CASE WHEN d.result_status = 'error' THEN 1 ELSE 0 END) AS error_count
      FROM monitor_deep_compare_runs r
      JOIN monitor_servers s ON s.id = r.server_id
      LEFT JOIN monitor_deep_compare_results d ON d.deep_run_id = r.id
    """
    args: list[Any] = []
    if status:
        sql += " WHERE r.status = %s"
        args.append(status)
    sql += """
     GROUP BY r.id, r.started_at, r.finished_at, r.triggered_by,
              r.server_id, r.db_name, r.table_name, r.pk_column,
              r.compare_scope, r.chunk_size, r.status, r.summary,
              s.server_code, s.server_name
     ORDER BY r.id DESC
     LIMIT %s
    """
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def get_deep_run_detail(conn, run_id: int):
    sql = """
    SELECT r.id, r.started_at, r.finished_at, r.triggered_by,
           r.server_id, r.db_name, r.table_name, r.pk_column,
           r.compare_scope, r.chunk_size, r.status, r.summary,
           s.server_code, s.server_name,
           COUNT(d.id) AS chunk_count,
           SUM(CASE WHEN d.result_status = 'mismatch' THEN 1 ELSE 0 END) AS mismatch_count,
           SUM(CASE WHEN d.result_status = 'error' THEN 1 ELSE 0 END) AS error_count
      FROM monitor_deep_compare_runs r
      JOIN monitor_servers s ON s.id = r.server_id
      LEFT JOIN monitor_deep_compare_results d ON d.deep_run_id = r.id
     WHERE r.id = %s
     GROUP BY r.id, r.started_at, r.finished_at, r.triggered_by,
              r.server_id, r.db_name, r.table_name, r.pk_column,
              r.compare_scope, r.chunk_size, r.status, r.summary,
              s.server_code, s.server_name
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return cur.fetchone()


def list_deep_results(conn, run_id: int, result_status: str | None = None, limit: int = 5000):
    sql = """
    SELECT id, deep_run_id, chunk_no, pk_start, pk_end, master_hash, slave_hash,
           master_count, slave_count, result_status, diff_summary, created_at
      FROM monitor_deep_compare_results
     WHERE deep_run_id = %s
    """
    args: list[Any] = [run_id]
    if result_status:
        sql += " AND result_status = %s"
        args.append(result_status)
    sql += " ORDER BY chunk_no ASC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())


def list_deep_result_statuses(conn, run_id: int):
    sql = """
    SELECT result_status, COUNT(*) AS chunk_count
      FROM monitor_deep_compare_results
     WHERE deep_run_id = %s
     GROUP BY result_status
     ORDER BY result_status ASC
    """
    with cursor(conn) as cur:
        cur.execute(sql, (run_id,))
        return list(cur.fetchall())
