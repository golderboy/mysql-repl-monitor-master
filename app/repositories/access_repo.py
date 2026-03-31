from __future__ import annotations

import json
from typing import Any

from app.db import cursor


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def get_auth_context(conn, username: str):
    sql = """
    SELECT id, username, display_name, password_hash, is_active, require_password_change,
           failed_login_count, locked_until, password_changed_at
      FROM monitor_users
     WHERE username = %s
     LIMIT 1
    """
    with cursor(conn) as cur:
        cur.execute(sql, (username,))
        user = cur.fetchone()
        if not user:
            return None
        cur.execute(
            """
            SELECT r.role_code
              FROM monitor_user_roles ur
              INNER JOIN monitor_roles r ON r.id = ur.role_id
             WHERE ur.user_id = %s
             ORDER BY r.role_code ASC
            """,
            (user["id"],),
        )
        roles = [row["role_code"] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT p.permission_code
              FROM monitor_user_roles ur
              INNER JOIN monitor_role_permissions rp ON rp.role_id = ur.role_id
              INNER JOIN monitor_permissions p ON p.id = rp.permission_id
             WHERE ur.user_id = %s
             ORDER BY p.permission_code ASC
            """,
            (user["id"],),
        )
        permissions = [row["permission_code"] for row in cur.fetchall()]
    user["roles"] = roles
    user["permissions"] = permissions
    return user


def touch_last_login(conn, user_id: int) -> None:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_users
               SET last_login_at = CURRENT_TIMESTAMP(6),
                   failed_login_count = 0,
                   locked_until = NULL,
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (user_id,),
        )
    conn.commit()


def register_failed_login(conn, user_id: int, threshold: int, lockout_minutes: int):
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_users
               SET failed_login_count = COALESCE(failed_login_count, 0) + 1,
                   locked_until = CASE
                       WHEN COALESCE(failed_login_count, 0) + 1 >= %s
                       THEN DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL %s MINUTE)
                       ELSE locked_until
                   END,
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (threshold, lockout_minutes, user_id),
        )
        cur.execute(
            "SELECT failed_login_count, locked_until FROM monitor_users WHERE id = %s LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone() or {}
    conn.commit()
    return row


def list_users(conn):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT u.id, u.username, u.display_name, u.is_active, u.require_password_change,
                   u.last_login_at, u.failed_login_count, u.locked_until, u.password_changed_at,
                   u.created_at, u.updated_at,
                   GROUP_CONCAT(r.role_code ORDER BY r.role_code SEPARATOR ',') AS role_codes
              FROM monitor_users u
              LEFT JOIN monitor_user_roles ur ON ur.user_id = u.id
              LEFT JOIN monitor_roles r ON r.id = ur.role_id
             GROUP BY u.id, u.username, u.display_name, u.is_active, u.require_password_change,
                      u.last_login_at, u.failed_login_count, u.locked_until, u.password_changed_at,
                      u.created_at, u.updated_at
             ORDER BY u.username ASC
            """
        )
        return list(cur.fetchall())


def list_roles(conn):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT r.id, r.role_code, r.role_name, r.role_description,
                   GROUP_CONCAT(p.permission_code ORDER BY p.permission_code SEPARATOR ',') AS permission_codes
              FROM monitor_roles r
              LEFT JOIN monitor_role_permissions rp ON rp.role_id = r.id
              LEFT JOIN monitor_permissions p ON p.id = rp.permission_id
             GROUP BY r.id, r.role_code, r.role_name, r.role_description
             ORDER BY r.role_code ASC
            """
        )
        return list(cur.fetchall())


def create_user(conn, username: str, display_name: str | None, password_hash: str, role_codes: list[str]) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO monitor_users (username, display_name, password_hash, is_active, password_changed_at)
            VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP(6))
            """,
            (username, display_name, password_hash),
        )
        user_id = int(cur.lastrowid)
        if role_codes:
            cur.execute(
                f"SELECT id FROM monitor_roles WHERE role_code IN ({','.join(['%s'] * len(role_codes))})",
                tuple(role_codes),
            )
            role_ids = [int(row["id"]) for row in cur.fetchall()]
            if role_ids:
                cur.executemany(
                    "INSERT INTO monitor_user_roles (user_id, role_id) VALUES (%s, %s)",
                    [(user_id, role_id) for role_id in role_ids],
                )
    conn.commit()
    return user_id


def set_user_password(conn, user_id: int, password_hash: str, require_password_change: int = 0) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_users
               SET password_hash = %s,
                   require_password_change = %s,
                   failed_login_count = 0,
                   locked_until = NULL,
                   password_changed_at = CURRENT_TIMESTAMP(6),
                   updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s
            """,
            (password_hash, require_password_change, user_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected


def set_user_active(conn, user_id: int, is_active: int) -> int:
    with cursor(conn) as cur:
        cur.execute(
            "UPDATE monitor_users SET is_active = %s, updated_at = CURRENT_TIMESTAMP(6) WHERE id = %s",
            (is_active, user_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected


def set_user_roles(conn, user_id: int, role_codes: list[str]) -> None:
    with cursor(conn) as cur:
        cur.execute("DELETE FROM monitor_user_roles WHERE user_id = %s", (user_id,))
        if role_codes:
            cur.execute(
                f"SELECT id FROM monitor_roles WHERE role_code IN ({','.join(['%s'] * len(role_codes))})",
                tuple(role_codes),
            )
            role_ids = [int(row["id"]) for row in cur.fetchall()]
            if role_ids:
                cur.executemany(
                    "INSERT INTO monitor_user_roles (user_id, role_id) VALUES (%s, %s)",
                    [(user_id, role_id) for role_id in role_ids],
                )
    conn.commit()



def get_user_by_username(conn, username: str):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, username, display_name, is_active, require_password_change,
                   failed_login_count, locked_until, password_changed_at,
                   last_login_at, created_at, updated_at
              FROM monitor_users
             WHERE username = %s
             LIMIT 1
            """,
            (username,),
        )
        return cur.fetchone()


def create_change_request(conn, request_type: str, target_key: str | None, payload: dict[str, Any], requested_by: str, reason_text: str | None = None) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO monitor_config_change_requests
                (request_type, target_key, payload_json, requested_by, reason_text, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (request_type, target_key, _json(payload), requested_by, reason_text),
        )
        request_id = int(cur.lastrowid)
    conn.commit()
    return request_id



def list_change_requests(conn, status: str | None = None, limit: int = 100):
    sql = """
    SELECT id, request_type, target_key, payload_json, requested_by, reason_text,
           status, approved_by, approved_at, applied_at, created_at, updated_at
      FROM monitor_config_change_requests
     WHERE 1=1
    """
    args: list[Any] = []
    if status:
        sql += " AND status = %s"
        args.append(status)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())



def get_change_request(conn, request_id: int):
    with cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, request_type, target_key, payload_json, requested_by, reason_text,
                   status, approved_by, approved_at, applied_at, created_at, updated_at
              FROM monitor_config_change_requests
             WHERE id = %s
             LIMIT 1
            """,
            (request_id,),
        )
        return cur.fetchone()



def approve_change_request(conn, request_id: int, approved_by: str) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_config_change_requests
               SET status = 'approved', approved_by = %s, approved_at = CURRENT_TIMESTAMP(6), updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s AND status = 'pending'
            """,
            (approved_by, request_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected



def reject_change_request(conn, request_id: int, approved_by: str, reason_text: str | None) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_config_change_requests
               SET status = 'rejected', approved_by = %s, approved_at = CURRENT_TIMESTAMP(6),
                   reason_text = COALESCE(%s, reason_text), updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s AND status = 'pending'
            """,
            (approved_by, reason_text, request_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected



def mark_change_request_applied(conn, request_id: int, approved_by: str) -> int:
    with cursor(conn) as cur:
        cur.execute(
            """
            UPDATE monitor_config_change_requests
               SET status = 'applied', approved_by = COALESCE(approved_by, %s),
                   applied_at = CURRENT_TIMESTAMP(6), updated_at = CURRENT_TIMESTAMP(6)
             WHERE id = %s AND status IN ('approved', 'pending')
            """,
            (approved_by, request_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected


def list_web_audit_logs(conn, actor: str | None = None, action_prefix: str | None = None, limit: int = 200):
    sql = """
    SELECT id, actor, action, object_type, object_id, details_json, created_at
      FROM monitor_web_audit_logs
     WHERE 1=1
    """
    args: list[Any] = []
    if actor:
        sql += " AND actor = %s"
        args.append(actor)
    if action_prefix:
        sql += " AND action LIKE %s"
        args.append(f"{action_prefix}%")
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(limit)
    with cursor(conn) as cur:
        cur.execute(sql, tuple(args))
        return list(cur.fetchall())
