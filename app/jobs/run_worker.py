from __future__ import annotations

import argparse
import json
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.db import get_monitor_conn
from app.repositories.extra_repo import (
    acquire_job_lock,
    claim_next_job,
    finish_job_canceled,
    finish_job_failed,
    finish_job_success,
    is_cancel_requested,
    release_job_lock,
    update_job_progress,
)
from app.services.deep_compare_service import execute_deep_compare
from app.services.schema_compare_service import execute_schema_compare
from app.jobs.run_summary import build_daily, build_hourly
from app.repositories.report_repo import get_report_export_by_job_run, mark_report_running, finish_report_success, finish_report_failed, finish_report_canceled
from app.services.report_service import generate_report_export


LOCK_NAMES = {
    "summary": "summary",
    "schema_compare": "schema_compare",
    "deep_compare": "deep_compare",
    "report_export": "report_export",
}


def _payload(row: dict) -> dict:
    raw = row.get("payload_json")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _run_summary(conn, run_id: int, payload: dict) -> dict:
    days = int(payload.get("days") or 7)
    hours = int(payload.get("hours") or 24)
    update_job_progress(conn, run_id, 20, {"step": "hourly"})
    hourly_rows = build_hourly(conn, hours=max(1, hours))
    if is_cancel_requested(conn, run_id):
        finish_job_canceled(conn, run_id, {"step": "after_hourly"})
        return {"status": "canceled"}
    update_job_progress(conn, run_id, 70, {"step": "daily"})
    daily_rows = build_daily(conn, days=max(1, days))
    return {"hourly_rows": len(hourly_rows), "daily_rows": len(daily_rows)}


def _run_schema_compare(conn, run_id: int, requested_by: str, payload: dict) -> dict:
    target_server_id = int(payload.get("target_server_id") or payload.get("server_id") or 0)
    if target_server_id <= 0:
        raise ValueError("target_server_id is required")
    update_job_progress(conn, run_id, 20, {"step": "schema_compare"})
    schema_run_id = execute_schema_compare(conn, target_server_id=target_server_id, triggered_by=requested_by)
    return {"schema_run_id": schema_run_id}


def _run_deep_compare(conn, run_id: int, requested_by: str, payload: dict) -> dict:
    target_server_id = int(payload.get("target_server_id") or payload.get("server_id") or 0)
    db_name = (payload.get("db_name") or "").strip()
    table_name = (payload.get("table_name") or "").strip()
    pk_column = (payload.get("pk_column") or "").strip()
    compare_scope = (payload.get("compare_scope") or "").strip() or None
    chunk_size = int(payload.get("chunk_size") or 1000)
    if target_server_id <= 0 or not db_name or not table_name or not pk_column:
        raise ValueError("target_server_id, db_name, table_name and pk_column are required")
    update_job_progress(conn, run_id, 20, {"step": "deep_compare"})
    deep_run_id = execute_deep_compare(
        conn,
        target_server_id=target_server_id,
        db_name=db_name,
        table_name=table_name,
        pk_column=pk_column,
        compare_scope=compare_scope,
        chunk_size=chunk_size,
        triggered_by=requested_by,
    )
    return {"deep_run_id": deep_run_id}




def _run_report_export(conn, run_id: int, requested_by: str, payload: dict) -> dict:
    report_row = get_report_export_by_job_run(conn, run_id)
    if not report_row:
        raise ValueError("report export request not found")
    report_id = int(report_row["id"])
    report_type = (payload.get("report_type") or report_row.get("report_type") or "daily").strip()
    export_format = (payload.get("export_format") or report_row.get("export_format") or "csv").strip().lower()
    period_days = int(payload.get("period_days") or report_row.get("period_days") or 0)
    mark_report_running(conn, report_id)
    update_job_progress(conn, run_id, 25, {"step": "build_report", "report_id": report_id})
    generated = generate_report_export(
        conn,
        report_type=report_type,
        export_format=export_format,
        period_days=period_days,
        requested_by=requested_by,
        report_id=report_id,
        job_run_id=run_id,
        report_view=payload.get("report_view"),
    )
    finish_report_success(
        conn,
        report_id,
        file_name=generated.file_name,
        file_relpath=generated.file_relpath,
        file_size_bytes=generated.file_size_bytes,
        sha256_hex=generated.sha256_hex,
        manifest=generated.manifest,
    )
    return {
        "report_id": report_id,
        "file_name": generated.file_name,
        "file_relpath": generated.file_relpath,
        "export_format": export_format,
    }

def process_one(conn) -> bool:
    row = claim_next_job(conn)
    if not row:
        return False

    run_id = int(row["id"])
    job_name = row["job_name"]
    requested_by = row.get("requested_by") or "system"
    payload = _payload(row)
    lock_name = LOCK_NAMES.get(job_name)

    if is_cancel_requested(conn, run_id):
        if job_name == "report_export":
            report_row = get_report_export_by_job_run(conn, run_id)
            if report_row:
                finish_report_canceled(conn, int(report_row["id"]), "canceled before start")
        finish_job_canceled(conn, run_id, {"step": "before_start"})
        return True

    if not lock_name:
        finish_job_failed(conn, run_id, f"unsupported job_name: {job_name}")
        return True

    if not acquire_job_lock(conn, lock_name, run_id):
        if job_name == "report_export":
            report_row = get_report_export_by_job_run(conn, run_id)
            if report_row:
                finish_report_failed(conn, int(report_row["id"]), f"lock busy: {lock_name}")
        finish_job_failed(conn, run_id, f"lock busy: {lock_name}")
        return True

    try:
        if job_name == "summary":
            result = _run_summary(conn, run_id, payload)
        elif job_name == "schema_compare":
            result = _run_schema_compare(conn, run_id, requested_by, payload)
        elif job_name == "deep_compare":
            result = _run_deep_compare(conn, run_id, requested_by, payload)
        elif job_name == "report_export":
            result = _run_report_export(conn, run_id, requested_by, payload)
        else:
            raise ValueError(f"unsupported job_name: {job_name}")
        if result.get("status") == "canceled":
            return True
        finish_job_success(conn, run_id, result)
    except Exception as exc:
        if job_name == "report_export":
            report_row = get_report_export_by_job_run(conn, run_id)
            if report_row:
                finish_report_failed(conn, int(report_row["id"]), str(exc))
        finish_job_failed(conn, run_id, str(exc))
    finally:
        release_job_lock(conn, lock_name, run_id)
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-jobs", type=int, default=10)
    args = parser.parse_args()

    conn = get_monitor_conn()
    try:
        processed = 0
        max_jobs = max(1, args.max_jobs)
        while processed < max_jobs and process_one(conn):
            processed += 1
    finally:
        conn.close()

    print({"status": "ok", "processed_jobs": processed})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
