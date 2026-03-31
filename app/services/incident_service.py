from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from app.config import settings
from app.repositories.incident_repo import (
    add_incident_event,
    count_recent_replication_ok,
    count_recent_signature_mismatches,
    count_recent_signature_ok,
    create_incident,
    get_incident_by_code,
    get_last_event_hash,
    recover_incident,
    touch_incident_detected,
)


SEVERITY_ORDER = {
    "INFO": 10,
    "WARNING": 20,
    "CRITICAL": 30,
}


def _severity_rank(value: str | None) -> int:
    if not value:
        return 0
    return SEVERITY_ORDER.get(value.upper(), 0)



def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()



def _open_or_update_incident(
    monitor_conn,
    incident_code: str,
    issue_type: str,
    severity: str,
    server_id: int | None,
    db_name: str | None,
    table_name: str | None,
    summary: str,
    detected_message: str,
) -> int:
    incident = get_incident_by_code(monitor_conn, incident_code)
    if not incident:
        incident_id = create_incident(
            monitor_conn=monitor_conn,
            incident_code=incident_code,
            issue_type=issue_type,
            severity=severity,
            server_id=server_id,
            db_name=db_name,
            table_name=table_name,
            system_summary=summary,
        )
        event_hash = _stable_hash({
            "incident_code": incident_code,
            "event_type": "detected",
            "severity": severity,
            "message": detected_message,
        })
        add_incident_event(
            monitor_conn=monitor_conn,
            incident_id=incident_id,
            event_type="detected",
            message=detected_message,
            prev_event_hash=None,
            event_hash=event_hash,
            old_status=None,
            new_status="OPEN",
        )
        return incident_id

    old_status = incident["current_status"]
    old_severity = incident["severity"]
    incident_id = int(incident["id"])

    touch_incident_detected(
        monitor_conn=monitor_conn,
        incident_id=incident_id,
        severity=severity,
        system_summary=summary,
    )

    should_add_detected = old_status in ("RECOVERED", "CLOSED")
    should_add_escalated = _severity_rank(severity) > _severity_rank(old_severity)

    if should_add_detected or should_add_escalated:
        prev_hash = get_last_event_hash(monitor_conn, incident_id)
        event_type = "detected" if should_add_detected else "escalated"
        payload = {
            "incident_code": incident_code,
            "event_type": event_type,
            "old_status": old_status,
            "new_status": "OPEN" if should_add_detected else old_status,
            "old_severity": old_severity,
            "new_severity": severity,
            "message": detected_message,
        }
        event_hash = _stable_hash(payload)
        add_incident_event(
            monitor_conn=monitor_conn,
            incident_id=incident_id,
            event_type=event_type,
            old_status=old_status,
            new_status="OPEN" if should_add_detected else old_status,
            message=detected_message,
            prev_event_hash=prev_hash,
            event_hash=event_hash,
        )

    return incident_id



def _recover_if_needed(
    monitor_conn,
    incident_code: str,
    summary: str,
    recover_message: str,
) -> None:
    incident = get_incident_by_code(monitor_conn, incident_code)
    if not incident:
        return
    if incident["current_status"] not in ("OPEN", "ACKNOWLEDGED", "INVESTIGATING"):
        return

    incident_id = int(incident["id"])
    prev_hash = get_last_event_hash(monitor_conn, incident_id)
    event_hash = _stable_hash({
        "incident_code": incident_code,
        "event_type": "recovered",
        "message": recover_message,
    })

    recover_incident(monitor_conn, incident_id, summary)
    add_incident_event(
        monitor_conn=monitor_conn,
        incident_id=incident_id,
        event_type="recovered",
        old_status=incident["current_status"],
        new_status="RECOVERED",
        message=recover_message,
        prev_event_hash=prev_hash,
        event_hash=event_hash,
    )



def process_replication_event(
    monitor_conn,
    server: Dict[str, Any],
    replication_payload: Dict[str, Any],
    evidence_id: int | None,
) -> None:
    incident_code = f"REPL-{server['server_code']}"
    health = replication_payload.get("health_status", "error")

    if health == "ok":
        ok_rounds = count_recent_replication_ok(
            monitor_conn=monitor_conn,
            server_id=server["id"],
            rounds=settings.recover_after_consecutive_ok,
        )
        if ok_rounds >= settings.recover_after_consecutive_ok:
            _recover_if_needed(
                monitor_conn=monitor_conn,
                incident_code=incident_code,
                summary=f"Replication recovered on {server['server_code']}",
                recover_message=f"Replication recovered on {server['server_code']}; evidence_id={evidence_id}",
            )
        return

    severity = "CRITICAL" if health in {"critical", "error"} else "WARNING"
    summary = (
        f"Replication issue on {server['server_code']} "
        f"(IO={replication_payload.get('slave_io_running')}, "
        f"SQL={replication_payload.get('slave_sql_running')}, "
        f"Lag={replication_payload.get('seconds_behind_master')})"
    )
    _open_or_update_incident(
        monitor_conn=monitor_conn,
        incident_code=incident_code,
        issue_type="replication_status",
        severity=severity,
        server_id=server["id"],
        db_name=None,
        table_name=None,
        summary=summary,
        detected_message=f"{summary}; evidence_id={evidence_id}",
    )



def process_signature_event(
    monitor_conn,
    server: Dict[str, Any],
    watch_item: Dict[str, Any],
    master_signature: Dict[str, Any],
    slave_signature: Dict[str, Any],
    result_status: str,
    evidence_id: int | None,
    diff_summary: str | None = None,
) -> None:
    incident_code = f"SIG-{server['server_code']}-{watch_item['db_name']}-{watch_item['table_name']}"
    summary = f"Signature mismatch: {watch_item['db_name']}.{watch_item['table_name']} on {server['server_code']}"

    if result_status == "match":
        ok_rounds = count_recent_signature_ok(
            monitor_conn=monitor_conn,
            server_id=server["id"],
            db_name=watch_item["db_name"],
            table_name=watch_item["table_name"],
            rounds=settings.recover_after_consecutive_ok,
        )
        if ok_rounds >= settings.recover_after_consecutive_ok:
            _recover_if_needed(
                monitor_conn=monitor_conn,
                incident_code=incident_code,
                summary=f"Signature recovered: {watch_item['db_name']}.{watch_item['table_name']} on {server['server_code']}",
                recover_message=(
                    f"Signature recovered: {watch_item['db_name']}.{watch_item['table_name']} "
                    f"on {server['server_code']}; evidence_id={evidence_id}"
                ),
            )
        return

    if result_status == "error":
        _open_or_update_incident(
            monitor_conn=monitor_conn,
            incident_code=incident_code,
            issue_type="table_signature",
            severity="CRITICAL",
            server_id=server["id"],
            db_name=watch_item["db_name"],
            table_name=watch_item["table_name"],
            summary=summary if not diff_summary else f"{summary} | {diff_summary}",
            detected_message=f"{summary}; evidence_id={evidence_id}; diff={diff_summary}",
        )
        return

    threshold = settings.signature_consecutive_mismatch_to_open
    cnt = count_recent_signature_mismatches(
        monitor_conn=monitor_conn,
        server_id=server["id"],
        db_name=watch_item["db_name"],
        table_name=watch_item["table_name"],
        rounds=threshold,
    )
    if cnt < threshold:
        return

    _open_or_update_incident(
        monitor_conn=monitor_conn,
        incident_code=incident_code,
        issue_type="table_signature",
        severity="WARNING",
        server_id=server["id"],
        db_name=watch_item["db_name"],
        table_name=watch_item["table_name"],
        summary=summary if not diff_summary else f"{summary} | {diff_summary}",
        detected_message=f"{summary}; evidence_id={evidence_id}; diff={diff_summary}",
    )
