from __future__ import annotations

from app.db import get_monitor_conn, get_prod_conn
from app.repositories.monitor_repo import (
    create_check_run,
    finish_check_run,
    get_active_servers,
    get_last_replication_event_hash,
    get_last_signature_event_hash,
    get_watchlist,
    insert_replication_log,
    insert_signature_log,
)
from app.services.replication_service import fetch_replication_status
from app.services.signature_service import build_signature, compare_signature
from app.services.incident_service import process_replication_event, process_signature_event
from app.services.telegram_service import flush_notifications
from app.services.event_hash_service import stable_hash



def main() -> None:
    monitor_conn = get_monitor_conn()
    run_id = create_check_run(monitor_conn, run_type="scheduled", triggered_by="system")
    run_status = "success"
    master_conn = None

    try:
        servers = get_active_servers(monitor_conn)
        watchlist = get_watchlist(monitor_conn)

        master = next((x for x in servers if x["role"] == "MASTER"), None)
        slaves = [x for x in servers if x["role"] == "SLAVE"]

        if not master:
            raise RuntimeError("No active server with role=MASTER found in monitor_servers")
        if not slaves:
            raise RuntimeError("No active server with role=SLAVE found in monitor_servers")

        master_conn = get_prod_conn(master)

        for slave in slaves:
            slave_conn = None
            try:
                slave_conn = get_prod_conn(slave)

                repl = fetch_replication_status(slave_conn)
                repl_prev_hash = get_last_replication_event_hash(monitor_conn, slave["id"])
                repl_event_hash = stable_hash({
                    "check_run_id": run_id,
                    "server_id": slave["id"],
                    "payload": repl,
                    "prev_event_hash": repl_prev_hash,
                })
                repl_log_id = insert_replication_log(
                    monitor_conn=monitor_conn,
                    check_run_id=run_id,
                    server_id=slave["id"],
                    payload=repl,
                    prev_event_hash=repl_prev_hash,
                    event_hash=repl_event_hash,
                )

                process_replication_event(
                    monitor_conn=monitor_conn,
                    server=slave,
                    replication_payload=repl,
                    evidence_id=repl_log_id,
                )

                for item in watchlist:
                    try:
                        master_sig = build_signature(master_conn, item)
                        slave_sig = build_signature(slave_conn, item)
                        result_status, diff_summary = compare_signature(master_sig, slave_sig)
                    except Exception as sig_exc:
                        master_sig = {}
                        slave_sig = {}
                        result_status = "error"
                        diff_summary = str(sig_exc)

                    sig_prev_hash = get_last_signature_event_hash(
                        monitor_conn,
                        slave["id"],
                        item["db_name"],
                        item["table_name"],
                    )
                    sig_event_hash = stable_hash({
                        "check_run_id": run_id,
                        "server_id": slave["id"],
                        "db_name": item["db_name"],
                        "table_name": item["table_name"],
                        "compare_strategy": item["compare_strategy"],
                        "master_signature": master_sig,
                        "slave_signature": slave_sig,
                        "result_status": result_status,
                        "diff_summary": diff_summary,
                        "prev_event_hash": sig_prev_hash,
                    })
                    sig_log_id = insert_signature_log(
                        monitor_conn=monitor_conn,
                        check_run_id=run_id,
                        server_id=slave["id"],
                        db_name=item["db_name"],
                        table_name=item["table_name"],
                        compare_strategy=item["compare_strategy"],
                        master_signature=master_sig,
                        slave_signature=slave_sig,
                        result_status=result_status,
                        diff_summary=diff_summary,
                        error_message=diff_summary if result_status == "error" else None,
                        prev_event_hash=sig_prev_hash,
                        event_hash=sig_event_hash,
                    )

                    process_signature_event(
                        monitor_conn=monitor_conn,
                        server=slave,
                        watch_item=item,
                        master_signature=master_sig,
                        slave_signature=slave_sig,
                        result_status=result_status,
                        evidence_id=sig_log_id,
                        diff_summary=diff_summary,
                    )

            except Exception as exc:
                run_status = "partial"
                process_replication_event(
                    monitor_conn=monitor_conn,
                    server=slave,
                    replication_payload={
                        "is_connected": 0,
                        "health_status": "error",
                        "last_io_error": str(exc),
                    },
                    evidence_id=None,
                )
            finally:
                if slave_conn:
                    slave_conn.close()

        flush_notifications(monitor_conn)

    except Exception:
        run_status = "error"
        raise
    finally:
        if master_conn:
            master_conn.close()
        finish_check_run(monitor_conn, run_id=run_id, status=run_status)
        monitor_conn.close()


if __name__ == "__main__":
    main()
