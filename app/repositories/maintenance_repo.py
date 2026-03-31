from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from app.db import cursor


def get_setting_map(conn) -> dict[str, str]:
    sql = "SELECT setting_key, setting_value FROM monitor_settings ORDER BY setting_key"
    with cursor(conn) as cur:
        cur.execute(sql)
        return {row["setting_key"]: row["setting_value"] for row in cur.fetchall()}


def list_export_archives(export_root: str, limit: int = 50) -> list[dict[str, Any]]:
    root = Path(export_root)
    if not root.exists():
        return []

    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        stat = path.stat()
        sha256_file = path.with_suffix(path.suffix + ".sha256")
        sha256_value = None
        if sha256_file.exists():
            try:
                sha256_value = sha256_file.read_text(encoding="utf-8").strip().split()[0]
            except Exception:
                sha256_value = None
        rows.append(
            {
                "archive_name": path.name,
                "archive_path": str(path),
                "archive_size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime),
                "sha256": sha256_value,
            }
        )
    return rows
