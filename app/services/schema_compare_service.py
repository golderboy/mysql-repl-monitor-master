from __future__ import annotations

import json
from typing import Any

from app.db import cursor, get_prod_conn
from app.repositories.monitor_repo import get_active_servers
from app.repositories.schema_repo import create_schema_run, finish_schema_run, insert_schema_diff
from app.services.event_hash_service import stable_hash


TABLE_SQL = """
SELECT table_name, engine, table_collation, create_options, table_comment
  FROM information_schema.tables
 WHERE table_schema = %s
   AND table_type = 'BASE TABLE'
 ORDER BY table_name
"""

COLUMN_SQL = """
SELECT table_name, column_name, ordinal_position, column_type, is_nullable,
       column_default, extra, column_key, character_set_name, collation_name
  FROM information_schema.columns
 WHERE table_schema = %s
 ORDER BY table_name, ordinal_position
"""

INDEX_SQL = """
SELECT table_name, index_name, non_unique, index_type, seq_in_index,
       column_name, sub_part, collation
  FROM information_schema.statistics
 WHERE table_schema = %s
 ORDER BY table_name, index_name, seq_in_index
"""


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _fetch_tables(conn, db_name: str) -> dict[str, dict[str, Any]]:
    with cursor(conn) as cur:
        cur.execute(TABLE_SQL, (db_name,))
        rows = cur.fetchall()
    return {
        row["table_name"]: {
            "engine": row["engine"],
            "table_collation": row["table_collation"],
            "create_options": row["create_options"],
            "table_comment": row["table_comment"],
        }
        for row in rows
    }


def _fetch_columns(conn, db_name: str) -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = {}
    with cursor(conn) as cur:
        cur.execute(COLUMN_SQL, (db_name,))
        rows = cur.fetchall()
    for row in rows:
        table_map = result.setdefault(row["table_name"], {})
        table_map[row["column_name"]] = {
            "ordinal_position": row["ordinal_position"],
            "column_type": row["column_type"],
            "is_nullable": row["is_nullable"],
            "column_default": row["column_default"],
            "extra": row["extra"],
            "column_key": row["column_key"],
            "character_set_name": row["character_set_name"],
            "collation_name": row["collation_name"],
        }
    return result


def _fetch_indexes(conn, db_name: str) -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = {}
    with cursor(conn) as cur:
        cur.execute(INDEX_SQL, (db_name,))
        rows = cur.fetchall()
    for row in rows:
        table_map = result.setdefault(row["table_name"], {})
        index_map = table_map.setdefault(
            row["index_name"],
            {
                "non_unique": row["non_unique"],
                "index_type": row["index_type"],
                "columns": [],
            },
        )
        index_map["columns"].append(
            {
                "seq_in_index": row["seq_in_index"],
                "column_name": row["column_name"],
                "sub_part": row["sub_part"],
                "collation": row["collation"],
            }
        )
    return result


def _insert_diff(
    monitor_conn,
    run_id: int,
    server_id: int,
    db_name: str,
    table_name: str,
    diff_type: str,
    object_type: str,
    object_name: str | None,
    master_value: Any,
    slave_value: Any,
    diff_summary: str,
) -> None:
    event_hash = stable_hash(
        {
            "schema_run_id": run_id,
            "server_id": server_id,
            "db_name": db_name,
            "table_name": table_name,
            "diff_type": diff_type,
            "object_type": object_type,
            "object_name": object_name,
            "master_value": master_value,
            "slave_value": slave_value,
            "diff_summary": diff_summary,
        }
    )
    insert_schema_diff(
        monitor_conn=monitor_conn,
        schema_run_id=run_id,
        server_id=server_id,
        db_name=db_name,
        table_name=table_name,
        diff_type=diff_type,
        object_type=object_type,
        object_name=object_name,
        master_value=_json(master_value),
        slave_value=_json(slave_value),
        diff_summary=diff_summary,
        prev_event_hash=None,
        event_hash=event_hash,
    )


def _compare_tables(monitor_conn, run_id: int, server_id: int, db_name: str, master_tables, slave_tables) -> int:
    diff_count = 0
    master_names = set(master_tables)
    slave_names = set(slave_tables)

    for table_name in sorted(master_names - slave_names):
        diff_count += 1
        _insert_diff(
            monitor_conn,
            run_id,
            server_id,
            db_name,
            table_name,
            "table_missing",
            "table",
            table_name,
            master_tables[table_name],
            None,
            f"Table {table_name} exists on master only",
        )

    for table_name in sorted(slave_names - master_names):
        diff_count += 1
        _insert_diff(
            monitor_conn,
            run_id,
            server_id,
            db_name,
            table_name,
            "table_missing",
            "table",
            table_name,
            None,
            slave_tables[table_name],
            f"Table {table_name} exists on slave only",
        )

    for table_name in sorted(master_names & slave_names):
        if master_tables[table_name] != slave_tables[table_name]:
            diff_count += 1
            _insert_diff(
                monitor_conn,
                run_id,
                server_id,
                db_name,
                table_name,
                "table_definition",
                "table",
                table_name,
                master_tables[table_name],
                slave_tables[table_name],
                f"Table definition differs for {table_name}",
            )

    return diff_count


def _compare_columns(monitor_conn, run_id: int, server_id: int, db_name: str, master_columns, slave_columns) -> int:
    diff_count = 0
    common_tables = set(master_columns) | set(slave_columns)

    for table_name in sorted(common_tables):
        master_map = master_columns.get(table_name, {})
        slave_map = slave_columns.get(table_name, {})
        master_names = set(master_map)
        slave_names = set(slave_map)

        for column_name in sorted(master_names - slave_names):
            diff_count += 1
            _insert_diff(
                monitor_conn,
                run_id,
                server_id,
                db_name,
                table_name,
                "column_missing",
                "column",
                f"{table_name}.{column_name}",
                master_map[column_name],
                None,
                f"Column {table_name}.{column_name} exists on master only",
            )

        for column_name in sorted(slave_names - master_names):
            diff_count += 1
            _insert_diff(
                monitor_conn,
                run_id,
                server_id,
                db_name,
                table_name,
                "column_missing",
                "column",
                f"{table_name}.{column_name}",
                None,
                slave_map[column_name],
                f"Column {table_name}.{column_name} exists on slave only",
            )

        for column_name in sorted(master_names & slave_names):
            if master_map[column_name] != slave_map[column_name]:
                diff_count += 1
                _insert_diff(
                    monitor_conn,
                    run_id,
                    server_id,
                    db_name,
                    table_name,
                    "column_definition",
                    "column",
                    f"{table_name}.{column_name}",
                    master_map[column_name],
                    slave_map[column_name],
                    f"Column definition differs for {table_name}.{column_name}",
                )

    return diff_count


def _compare_indexes(monitor_conn, run_id: int, server_id: int, db_name: str, master_indexes, slave_indexes) -> int:
    diff_count = 0
    common_tables = set(master_indexes) | set(slave_indexes)

    for table_name in sorted(common_tables):
        master_map = master_indexes.get(table_name, {})
        slave_map = slave_indexes.get(table_name, {})
        master_names = set(master_map)
        slave_names = set(slave_map)

        for index_name in sorted(master_names - slave_names):
            diff_count += 1
            _insert_diff(
                monitor_conn,
                run_id,
                server_id,
                db_name,
                table_name,
                "index_missing",
                "index",
                f"{table_name}.{index_name}",
                master_map[index_name],
                None,
                f"Index {table_name}.{index_name} exists on master only",
            )

        for index_name in sorted(slave_names - master_names):
            diff_count += 1
            _insert_diff(
                monitor_conn,
                run_id,
                server_id,
                db_name,
                table_name,
                "index_missing",
                "index",
                f"{table_name}.{index_name}",
                None,
                slave_map[index_name],
                f"Index {table_name}.{index_name} exists on slave only",
            )

        for index_name in sorted(master_names & slave_names):
            if master_map[index_name] != slave_map[index_name]:
                diff_count += 1
                _insert_diff(
                    monitor_conn,
                    run_id,
                    server_id,
                    db_name,
                    table_name,
                    "index_definition",
                    "index",
                    f"{table_name}.{index_name}",
                    master_map[index_name],
                    slave_map[index_name],
                    f"Index definition differs for {table_name}.{index_name}",
                )

    return diff_count


def execute_schema_compare(monitor_conn, target_server_id: int, triggered_by: str) -> int:
    servers = get_active_servers(monitor_conn)
    master = next((x for x in servers if x["role"] == "MASTER"), None)
    target = next((x for x in servers if int(x["id"]) == int(target_server_id)), None)

    if not master:
        raise ValueError("No active MASTER reference configured")
    if not target:
        raise ValueError("Target server not found")
    if master["id"] == target["id"]:
        raise ValueError("Target server must not be the reference MASTER")

    run_id = create_schema_run(monitor_conn, target_server_id=target_server_id, triggered_by=triggered_by)
    master_conn = None
    slave_conn = None
    try:
        master_conn = get_prod_conn(master)
        slave_conn = get_prod_conn(target)
        db_name = target["db_name"]

        master_tables = _fetch_tables(master_conn, db_name)
        slave_tables = _fetch_tables(slave_conn, db_name)
        master_columns = _fetch_columns(master_conn, db_name)
        slave_columns = _fetch_columns(slave_conn, db_name)
        master_indexes = _fetch_indexes(master_conn, db_name)
        slave_indexes = _fetch_indexes(slave_conn, db_name)

        diff_count = 0
        diff_count += _compare_tables(monitor_conn, run_id, target["id"], db_name, master_tables, slave_tables)
        diff_count += _compare_columns(monitor_conn, run_id, target["id"], db_name, master_columns, slave_columns)
        diff_count += _compare_indexes(monitor_conn, run_id, target["id"], db_name, master_indexes, slave_indexes)

        compared_tables = len(set(master_tables) | set(slave_tables))
        summary = (
            f"Schema compare completed for {target['server_code']} ({db_name}) | "
            f"tables={compared_tables}, diffs={diff_count}"
        )
        finish_schema_run(monitor_conn, run_id, status="success", summary=summary)
        return run_id
    except Exception as exc:
        finish_schema_run(monitor_conn, run_id, status="error", summary=str(exc))
        raise
    finally:
        if master_conn:
            master_conn.close()
        if slave_conn:
            slave_conn.close()
