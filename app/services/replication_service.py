from __future__ import annotations

from typing import Any, Dict

from app.config import settings
from app.db import cursor


def fetch_replication_status(slave_conn) -> Dict[str, Any]:
    '''
    รองรับ MariaDB/MySQL ผ่าน SHOW REPLICA STATUS หรือ SHOW SLAVE STATUS
    '''
    with cursor(slave_conn) as cur:
        try:
            cur.execute("SHOW REPLICA STATUS")
        except Exception:
            cur.execute("SHOW SLAVE STATUS")
        row = cur.fetchone()

    if not row:
        return {
            "is_connected": 0,
            "health_status": "error",
            "last_io_error": "No replica/slave status row returned",
        }

    seconds_behind = row.get("Seconds_Behind_Master")
    io_running = row.get("Slave_IO_Running") or row.get("Replica_IO_Running")
    sql_running = row.get("Slave_SQL_Running") or row.get("Replica_SQL_Running")

    health_status = "ok"
    if io_running != "Yes" or sql_running != "Yes":
        health_status = "critical"
    elif seconds_behind is not None and seconds_behind >= settings.lag_critical_sec:
        health_status = "critical"
    elif seconds_behind is not None and seconds_behind >= settings.lag_warning_sec:
        health_status = "warn"

    return {
        "is_connected": 1,
        "slave_io_running": io_running,
        "slave_sql_running": sql_running,
        "seconds_behind_master": seconds_behind,
        "master_log_file": row.get("Master_Log_File"),
        "read_master_log_pos": row.get("Read_Master_Log_Pos"),
        "exec_master_log_pos": row.get("Exec_Master_Log_Pos"),
        "relay_master_log_file": row.get("Relay_Master_Log_File"),
        "last_io_errno": row.get("Last_IO_Errno"),
        "last_io_error": row.get("Last_IO_Error"),
        "last_sql_errno": row.get("Last_SQL_Errno"),
        "last_sql_error": row.get("Last_SQL_Error"),
        "sql_running_state": row.get("Slave_SQL_Running_State"),
        "health_status": health_status,
    }
