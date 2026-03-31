from __future__ import annotations

from app.services.evidence_export_service import run_evidence_export
from app.services.retention_service import run_retention_cleanup


def main() -> None:
    cleanup_result = run_retention_cleanup(triggered_by="system", dry_run=False)
    export_result = run_evidence_export(triggered_by="system")
    deleted_total = sum(int(x.get("deleted_rows") or 0) for x in cleanup_result["tables"])
    print(f"cleanup_deleted_total={deleted_total}")
    print(f"export_archive={export_result['archive_name']}")


if __name__ == "__main__":
    main()
