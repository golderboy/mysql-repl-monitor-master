from __future__ import annotations

from app.db import get_monitor_conn, cursor


RETENTION_SQL = [
    ("monitor_replication_logs", 180),
    ("monitor_table_signature_logs", 180),
    ("monitor_schema_diffs", 365),
    ("monitor_deep_compare_results", 60),
]


def main() -> None:
    conn = get_monitor_conn()
    try:
        with cursor(conn) as cur:
            for table_name, days in RETENTION_SQL:
                cur.execute(
                    f"DELETE FROM {table_name} WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                    (days,),
                )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
