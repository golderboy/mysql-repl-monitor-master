from __future__ import annotations

import argparse
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.db import get_monitor_conn
from app.services.deep_compare_service import execute_deep_compare


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deep compare as a one-shot job")
    parser.add_argument("--target-server-id", type=int, required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--pk-column", required=True)
    parser.add_argument("--compare-scope", default=None)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--triggered-by", default="manual")
    args = parser.parse_args()

    if args.chunk_size <= 0:
        raise SystemExit("chunk-size must be > 0")

    conn = get_monitor_conn()
    try:
        run_id = execute_deep_compare(
            conn,
            target_server_id=args.target_server_id,
            db_name=args.db_name,
            table_name=args.table_name,
            pk_column=args.pk_column,
            compare_scope=args.compare_scope,
            chunk_size=args.chunk_size,
            triggered_by=args.triggered_by,
        )
    finally:
        conn.close()

    print({
        "status": "ok",
        "deep_run_id": run_id,
        "target_server_id": args.target_server_id,
        "db_name": args.db_name,
        "table_name": args.table_name,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
