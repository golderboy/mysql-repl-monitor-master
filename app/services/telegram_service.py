from __future__ import annotations

import hashlib
import json

import requests

from app.config import settings
from app.db import cursor



def _stable_hash(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()



def send_message(message: str) -> tuple[bool, str | None, str | None]:
    if not settings.telegram_enabled:
        return False, None, "Telegram disabled"
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": settings.telegram_chat_id,
        "text": message,
    }
    resp = requests.post(url, json=payload, timeout=15)
    if resp.ok:
        data = resp.json()
        message_id = str(data.get("result", {}).get("message_id"))
        return True, message_id, None
    return False, None, resp.text



def write_telegram_log(
    monitor_conn,
    incident_id: int | None,
    message_text: str,
    success: bool,
    telegram_message_id: str | None,
    error_message: str | None,
    prev_event_hash: str | None = None,
) -> None:
    event_hash = _stable_hash({
        "incident_id": incident_id,
        "message_text": message_text,
        "send_status": "success" if success else "failed",
        "telegram_message_id": telegram_message_id,
        "error_message": error_message,
    })
    sql = """
    INSERT INTO monitor_telegram_logs (
        incident_id, chat_id, message_text, telegram_message_id,
        send_status, error_message, prev_event_hash, event_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with cursor(monitor_conn) as cur:
        cur.execute(
            sql,
            (
                incident_id,
                settings.telegram_chat_id,
                message_text,
                telegram_message_id,
                "success" if success else "failed",
                error_message,
                prev_event_hash,
                event_hash,
            ),
        )
        monitor_conn.commit()



def flush_notifications(monitor_conn) -> None:
    if not settings.telegram_enabled:
        return

    sql = """
    SELECT e.id,
           e.incident_id,
           e.event_type,
           e.message,
           e.event_hash,
           i.incident_code,
           i.severity,
           s.server_code
      FROM monitor_incident_events e
      JOIN monitor_incidents i ON i.id = e.incident_id
      LEFT JOIN monitor_servers s ON s.id = i.server_id
      LEFT JOIN monitor_telegram_logs t
        ON t.prev_event_hash = e.event_hash
     WHERE e.event_type IN ('detected','escalated','recovered')
       AND t.id IS NULL
     ORDER BY e.id ASC
    """

    with cursor(monitor_conn) as cur:
        cur.execute(sql)
        rows = list(cur.fetchall())

    for row in rows:
        text = (
            f"[{row['event_type'].upper()}] {row['incident_code']}\n"
            f"Severity: {row['severity']}\n"
            f"Server: {row.get('server_code') or '-'}\n"
            f"{row['message']}"
        )
        ok, telegram_message_id, err = send_message(text)
        write_telegram_log(
            monitor_conn=monitor_conn,
            incident_id=row["incident_id"],
            message_text=text,
            success=ok,
            telegram_message_id=telegram_message_id,
            error_message=err,
            prev_event_hash=row["event_hash"],
        )
