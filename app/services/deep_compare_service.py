from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from app.db import cursor, get_prod_conn
from app.repositories.deep_repo import create_deep_run, finish_deep_run, insert_deep_result
from app.repositories.monitor_repo import get_active_servers
from app.services.event_hash_service import stable_hash

IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _ident(value: str) -> str:
    if not IDENT_RE.match(value):
        raise ValueError(f"Invalid identifier: {value}")
    return f"`{value}`"


def _json_hash(rows: list[dict[str, Any]]) -> str:
    raw = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _find_servers(monitor_conn, target_server_id: int):
    servers = get_active_servers(monitor_conn)
    master = next((x for x in servers if x["role"] == "MASTER"), None)
    target = next((x for x in servers if x["id"] == target_server_id and x["role"] == "SLAVE"), None)
    if not master:
        raise ValueError("MASTER reference not found in monitor_servers")
    if not target:
        raise ValueError("Target SLAVE not found in monitor_servers")
    return master, target


def _get_columns(conn, db_name: str, table_name: str) -> list[str]:
    sql = """
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = %s
       AND table_name = %s
     ORDER BY ordinal_position
    """
    with cursor(conn) as cur:
        cur.execute(sql, (db_name, table_name))
        rows = cur.fetchall()
    return [row["column_name"] for row in rows]


def _fetch_rows(conn, db_name: str, table_name: str, pk_column: str, columns: list[str], compare_scope: str | None, limit: int, offset: int) -> list[dict[str, Any]]:
    select_cols = ", ".join(_ident(col) for col in columns)
    sql = f"SELECT {select_cols} FROM {_ident(db_name)}.{_ident(table_name)}"
    if compare_scope:
        sql += f" WHERE ({compare_scope})"
    sql += f" ORDER BY {_ident(pk_column)} LIMIT %s OFFSET %s"
    with cursor(conn) as cur:
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({col: row.get(col) for col in columns})
    return normalized


def _row_pk_range(rows: list[dict[str, Any]], pk_column: str) -> tuple[str | None, str | None]:
    if not rows:
        return None, None
    return str(rows[0].get(pk_column)), str(rows[-1].get(pk_column))


def _diff_summary(master_rows: list[dict[str, Any]], slave_rows: list[dict[str, Any]], pk_column: str) -> str:
    if len(master_rows) != len(slave_rows):
        return f"Row count differs in chunk: master={len(master_rows)} slave={len(slave_rows)}"
    for idx, (m_row, s_row) in enumerate(zip(master_rows, slave_rows), start=1):
        if m_row != s_row:
            return f"First differing row at chunk offset {idx}, pk master={m_row.get(pk_column)} slave={s_row.get(pk_column)}"
    return "Unknown mismatch"


def execute_deep_compare(
    monitor_conn,
    target_server_id: int,
    db_name: str,
    table_name: str,
    pk_column: str,
    compare_scope: str | None,
    chunk_size: int,
    triggered_by: str,
) -> int:
    master_srv, target_srv = _find_servers(monitor_conn, target_server_id)
    run_id = create_deep_run(
        monitor_conn,
        server_id=target_srv["id"],
        db_name=db_name,
        table_name=table_name,
        pk_column=pk_column,
        compare_scope=compare_scope,
        chunk_size=chunk_size,
        triggered_by=triggered_by,
    )

    master_conn = None
    slave_conn = None
    try:
        master_conn = get_prod_conn(master_srv)
        slave_conn = get_prod_conn(target_srv)

        master_cols = _get_columns(master_conn, db_name, table_name)
        slave_cols = _get_columns(slave_conn, db_name, table_name)
        if not master_cols:
            raise ValueError(f"Table not found on master: {db_name}.{table_name}")
        if not slave_cols:
            raise ValueError(f"Table not found on slave: {db_name}.{table_name}")
        if master_cols != slave_cols:
            raise ValueError("Schema differs; please run schema compare first")
        if pk_column not in master_cols:
            raise ValueError(f"pk_column not found: {pk_column}")

        offset = 0
        chunk_no = 0
        mismatch_count = 0
        error_count = 0
        columns = master_cols

        while True:
            chunk_no += 1
            master_rows = _fetch_rows(master_conn, db_name, table_name, pk_column, columns, compare_scope, chunk_size, offset)
            slave_rows = _fetch_rows(slave_conn, db_name, table_name, pk_column, columns, compare_scope, chunk_size, offset)
            if not master_rows and not slave_rows:
                chunk_no -= 1
                break

            master_hash = _json_hash(master_rows) if master_rows else None
            slave_hash = _json_hash(slave_rows) if slave_rows else None
            pk_start_m, pk_end_m = _row_pk_range(master_rows, pk_column)
            pk_start_s, pk_end_s = _row_pk_range(slave_rows, pk_column)
            pk_start = pk_start_m or pk_start_s
            pk_end = pk_end_m or pk_end_s

            result_status = "match"
            diff_summary = None
            if master_rows != slave_rows:
                result_status = "mismatch"
                mismatch_count += 1
                diff_summary = _diff_summary(master_rows, slave_rows, pk_column)

            event_hash = stable_hash({
                "deep_run_id": run_id,
                "chunk_no": chunk_no,
                "pk_start": pk_start,
                "pk_end": pk_end,
                "master_hash": master_hash,
                "slave_hash": slave_hash,
                "master_count": len(master_rows),
                "slave_count": len(slave_rows),
                "result_status": result_status,
                "diff_summary": diff_summary,
            })
            insert_deep_result(
                monitor_conn,
                deep_run_id=run_id,
                chunk_no=chunk_no,
                pk_start=pk_start,
                pk_end=pk_end,
                master_hash=master_hash,
                slave_hash=slave_hash,
                master_count=len(master_rows),
                slave_count=len(slave_rows),
                result_status=result_status,
                diff_summary=diff_summary,
                prev_event_hash=None,
                event_hash=event_hash,
            )
            offset += chunk_size

        final_status = "success" if mismatch_count == 0 and error_count == 0 else "partial"
        summary = f"Deep compare finished: chunks={chunk_no}, mismatches={mismatch_count}, errors={error_count}"
        finish_deep_run(monitor_conn, run_id, final_status, summary)
        return run_id
    except Exception as exc:
        finish_deep_run(monitor_conn, run_id, "error", f"Deep compare failed: {exc}")
        raise
    finally:
        if master_conn:
            master_conn.close()
        if slave_conn:
            slave_conn.close()
