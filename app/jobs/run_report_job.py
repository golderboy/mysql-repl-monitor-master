from __future__ import annotations

import argparse
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.db import get_monitor_conn
from app.services.report_service import generate_report_export, normalize_period_days


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-type", choices=["daily", "weekly", "monthly"], required=True)
    parser.add_argument("--export-format", choices=["csv", "pdf"], default="csv")
    parser.add_argument("--period-days", type=int, default=0)
    parser.add_argument("--requested-by", default="system")
    parser.add_argument("--report-id", type=int, default=0)
    parser.add_argument("--job-run-id", type=int, default=0)
    parser.add_argument("--report-view", choices=["daily", "admin", "executive", "hub"], default=None)
    args = parser.parse_args()

    report_id = int(args.report_id or 0)
    job_run_id = int(args.job_run_id or 0)
    if report_id <= 0:
        raise SystemExit("report-id is required and must be > 0")
    if job_run_id <= 0:
        raise SystemExit("job-run-id is required and must be > 0")

    conn = get_monitor_conn()
    try:
        result = generate_report_export(
            conn,
            report_type=args.report_type,
            export_format=args.export_format,
            period_days=normalize_period_days(args.report_type, args.period_days),
            requested_by=args.requested_by,
            report_id=report_id,
            job_run_id=job_run_id,
            report_view=args.report_view,
        )
        print({
            "status": "ok",
            "report_id": report_id,
            "job_run_id": job_run_id,
            "file_name": result.file_name,
            "file_relpath": result.file_relpath,
        })
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
