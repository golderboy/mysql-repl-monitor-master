from __future__ import annotations

from typing import Optional

from app.db import cursor


def get_setting(conn, key: str, default: Optional[str] = None) -> Optional[str]:
    sql = "SELECT setting_value FROM monitor_settings WHERE setting_key = %s"
    with cursor(conn) as cur:
        cur.execute(sql, (key,))
        row = cur.fetchone()
        if row:
            return row["setting_value"]
    return default


def list_settings(conn):
    sql = "SELECT setting_key, setting_value, updated_at FROM monitor_settings ORDER BY setting_key"
    with cursor(conn) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def upsert_setting(conn, key: str, value: str) -> None:
    sql = """
    INSERT INTO monitor_settings (setting_key, setting_value)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = CURRENT_TIMESTAMP(6)
    """
    with cursor(conn) as cur:
        cur.execute(sql, (key, value))
        conn.commit()


def list_watchlist(conn):
    sql = """
    SELECT id, db_name, table_name, enabled, priority, compare_strategy,
           pk_column, date_column, tail_rows, note, updated_at
      FROM monitor_table_watchlist
     ORDER BY priority ASC, table_name ASC
    """
    with cursor(conn) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def update_watchlist_item(
    conn,
    watch_id: int,
    enabled: int,
    priority: int,
    compare_strategy: str,
    note: str | None,
) -> None:
    sql = """
    UPDATE monitor_table_watchlist
       SET enabled = %s,
           priority = %s,
           compare_strategy = %s,
           note = %s,
           updated_at = CURRENT_TIMESTAMP(6)
     WHERE id = %s
    """
    with cursor(conn) as cur:
        cur.execute(sql, (enabled, priority, compare_strategy, note, watch_id))
        conn.commit()
