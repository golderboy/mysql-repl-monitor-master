from __future__ import annotations

import argparse
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.db import get_monitor_conn
from app.services.schema_compare_service import execute_schema_compare


def main() -> int:
    parser = argparse.ArgumentParser(description="Run schema compare as a one-shot job")
    parser.add_argument("--target-server-id", type=int, required=True)
    parser.add_argument("--triggered-by", default="manual")
    args = parser.parse_args()

    conn = get_monitor_conn()
    try:
        run_id = execute_schema_compare(
            conn,
            target_server_id=args.target_server_id,
            triggered_by=args.triggered_by,
        )
    finally:
        conn.close()

    print({"status": "ok", "schema_run_id": run_id, "target_server_id": args.target_server_id})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
