from __future__ import annotations

import hashlib
import json
import os
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.repositories.maintenance_repo import get_setting_map
from app.services.retention_service import get_export_root, get_maintenance_conn

DEFAULT_EXPORT_LOOKBACK_DAYS = 1
EXPORT_TABLES: dict[str, list[str]] = {
    "monitor_check_runs": ["started_at", "created_at"],
    "monitor_incidents": ["updated_at", "created_at", "first_detected_at"],
    "monitor_incident_events": ["created_at"],
    "monitor_incident_notes": ["created_at"],
    "monitor_replication_logs": ["checked_at", "created_at"],
    "monitor_table_signature_logs": ["checked_at", "created_at"],
    "monitor_schema_runs": ["started_at", "created_at"],
    "monitor_schema_diffs": ["created_at"],
    "monitor_deep_compare_runs": ["started_at", "created_at"],
    "monitor_deep_compare_results": ["created_at"],
    "monitor_telegram_logs": ["sent_at", "created_at"],
}


def _detect_timestamp_column(conn, table_name: str, candidates: list[str]) -> str | None:
    sql = """
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name,))
        existing = {row["column_name"] for row in cur.fetchall()}
    for col in candidates:
        if col in existing:
            return col
    return None


def get_export_config(read_conn=None) -> dict[str, Any]:
    owns_conn = False
    conn = read_conn
    if conn is None:
        conn = get_maintenance_conn()
        owns_conn = True
    try:
        settings_map = get_setting_map(conn)
        return {
            "export_dir": settings_map.get("export.dir", get_export_root()),
            "lookback_days": int(
                settings_map.get(
                    "export.lookback_days",
                    os.getenv("EVIDENCE_EXPORT_LOOKBACK_DAYS", DEFAULT_EXPORT_LOOKBACK_DAYS),
                )
                or DEFAULT_EXPORT_LOOKBACK_DAYS
            ),
        }
    finally:
        if owns_conn:
            conn.close()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_evidence_export(triggered_by: str = "system", lookback_days: int | None = None) -> dict[str, Any]:
    conn = get_maintenance_conn()
    try:
        cfg = get_export_config(conn)
        export_root = Path(cfg["export_dir"])
        export_root.mkdir(parents=True, exist_ok=True)
        lookback_days = int(lookback_days if lookback_days is not None else cfg["lookback_days"])
        cutoff = datetime.now() - timedelta(days=lookback_days)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        working_dir = export_root / f"evidence_export_{ts}"
        working_dir.mkdir(parents=True, exist_ok=True)

        manifest: dict[str, Any] = {
            "triggered_by": triggered_by,
            "started_at": datetime.now(),
            "lookback_days": lookback_days,
            "cutoff": cutoff,
            "tables": [],
        }

        for table_name, candidates in EXPORT_TABLES.items():
            time_column = _detect_timestamp_column(conn, table_name, candidates)
            outfile = working_dir / f"{table_name}.jsonl"
            rows_written = 0
            hasher = hashlib.sha256()

            if time_column:
                sql = f"SELECT * FROM `{table_name}` WHERE `{time_column}` >= %s ORDER BY `{time_column}` ASC"
                params = (cutoff,)
            else:
                sql = f"SELECT * FROM `{table_name}` ORDER BY 1"
                params = ()

            with conn.cursor() as cur, outfile.open("w", encoding="utf-8") as fh:
                cur.execute(sql, params)
                while True:
                    batch = cur.fetchmany(1000)
                    if not batch:
                        break
                    for row in batch:
                        line = json.dumps(row, ensure_ascii=False, default=str)
                        fh.write(line + "\n")
                        hasher.update((line + "\n").encode("utf-8"))
                        rows_written += 1

            manifest["tables"].append(
                {
                    "table_name": table_name,
                    "time_column": time_column,
                    "rows_written": rows_written,
                    "jsonl_sha256": hasher.hexdigest(),
                    "file_name": outfile.name,
                }
            )

        manifest_path = working_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        archive_path = export_root / f"evidence_export_{ts}.zip"
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for child in sorted(working_dir.iterdir()):
                zf.write(child, arcname=child.name)

        archive_sha256 = _sha256_file(archive_path)
        sha_path = Path(str(archive_path) + ".sha256")
        sha_path.write_text(f"{archive_sha256}  {archive_path.name}\n", encoding="utf-8")

        for child in working_dir.iterdir():
            child.unlink()
        working_dir.rmdir()

        return {
            "archive_name": archive_path.name,
            "archive_path": str(archive_path),
            "archive_sha256": archive_sha256,
            "lookback_days": lookback_days,
            "triggered_by": triggered_by,
            "table_count": len(manifest["tables"]),
        }
    finally:
        conn.close()
