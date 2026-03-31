from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Callable

from flask import Response, abort, g, redirect, request, session, url_for
from werkzeug.security import check_password_hash

from app.config import settings
from app.db import get_web_conn
from app.repositories.access_repo import (
    get_auth_context,
    register_failed_login,
    touch_last_login,
)


@dataclass(frozen=True)
class AuthContext:
    username: str
    display_name: str | None
    roles: tuple[str, ...]
    permissions: tuple[str, ...]
    source: str
    user_id: int | None = None
    require_password_change: bool = False


ALL_PERMISSIONS = "*"
SESSION_KEY = "_auth_ctx"
SESSION_AUTH_AT_KEY = "_auth_at"
SESSION_LAST_SEEN_KEY = "_auth_last_seen"


def _unauthorized() -> Response:
    return Response(
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="mysql-repl-monitor"'},
    )


def _wants_html() -> bool:
    best = request.accept_mimetypes.best_match(["text/html", "application/json", "text/plain"])
    return best == "text/html"


def _utcnow() -> datetime:
    return datetime.utcnow()


def _now_epoch() -> int:
    return int(time.time())


def _session_to_ctx(data: dict | None) -> AuthContext | None:
    if not data or not data.get("username"):
        return None
    return AuthContext(
        username=data["username"],
        display_name=data.get("display_name"),
        roles=tuple(data.get("roles") or ()),
        permissions=tuple(data.get("permissions") or ()),
        source=data.get("source") or "db",
        user_id=data.get("user_id"),
        require_password_change=bool(data.get("require_password_change")),
    )


def _ctx_to_session(ctx: AuthContext) -> dict:
    return {
        "username": ctx.username,
        "display_name": ctx.display_name,
        "roles": list(ctx.roles),
        "permissions": list(ctx.permissions),
        "source": ctx.source,
        "user_id": ctx.user_id,
        "require_password_change": bool(ctx.require_password_change),
    }


def _is_locked_until(value) -> bool:
    return bool(value and value > _utcnow())


def authenticate_credentials_detailed(username: str, password: str) -> tuple[AuthContext | None, str]:
    if not username:
        return None, "missing_username"

    if username == settings.admin_username:
        if password == settings.admin_password:
            return AuthContext(
                username=username,
                display_name=username,
                roles=("admin",),
                permissions=(ALL_PERMISSIONS,),
                source="env",
                user_id=None,
                require_password_change=False,
            ), "ok"
        return None, "invalid_credentials"

    conn = get_web_conn()
    try:
        row = get_auth_context(conn, username)
        if not row:
            return None, "invalid_credentials"
        if not row.get("is_active"):
            return None, "inactive"
        if _is_locked_until(row.get("locked_until")):
            return None, "locked"
        password_hash = row.get("password_hash") or ""
        if not password_hash or not check_password_hash(password_hash, password or ""):
            state = register_failed_login(
                conn,
                int(row["id"]),
                threshold=max(1, settings.login_lockout_threshold),
                lockout_minutes=max(1, settings.login_lockout_minutes),
            )
            if _is_locked_until(state.get("locked_until")):
                return None, "locked"
            return None, "invalid_credentials"
        touch_last_login(conn, int(row["id"]))
        return AuthContext(
            username=row["username"],
            display_name=row.get("display_name"),
            roles=tuple(row.get("roles") or []),
            permissions=tuple(row.get("permissions") or []),
            source="db",
            user_id=int(row["id"]),
            require_password_change=bool(row.get("require_password_change")),
        ), "ok"
    finally:
        conn.close()


def authenticate_credentials(username: str, password: str) -> AuthContext | None:
    ctx, _reason = authenticate_credentials_detailed(username, password)
    return ctx


def login_user(ctx: AuthContext) -> None:
    session[SESSION_KEY] = _ctx_to_session(ctx)
    session[SESSION_AUTH_AT_KEY] = _now_epoch()
    session[SESSION_LAST_SEEN_KEY] = _now_epoch()
    session.permanent = True


def logout_user() -> None:
    session.pop(SESSION_KEY, None)
    session.pop(SESSION_AUTH_AT_KEY, None)
    session.pop(SESSION_LAST_SEEN_KEY, None)


def _session_is_expired() -> bool:
    if SESSION_KEY not in session:
        return False
    now = _now_epoch()
    auth_at = int(session.get(SESSION_AUTH_AT_KEY) or now)
    last_seen = int(session.get(SESSION_LAST_SEEN_KEY) or auth_at)
    idle_limit_seconds = max(1, settings.session_idle_minutes) * 60
    absolute_limit_seconds = max(1, settings.session_absolute_minutes) * 60
    if now - last_seen > idle_limit_seconds:
        return True
    if now - auth_at > absolute_limit_seconds:
        return True
    session[SESSION_LAST_SEEN_KEY] = now
    return False


def _load_auth_context() -> AuthContext | None:
    cached = getattr(g, "_auth_ctx", None)
    if cached is not None:
        return cached

    session_ctx = _session_to_ctx(session.get(SESSION_KEY))
    if session_ctx is not None:
        if _session_is_expired():
            logout_user()
            return None
        g._auth_ctx = session_ctx
        return session_ctx

    # Important: for browser HTML flows we use session-based auth only.
    # Browsers may keep sending cached Basic Auth headers after logout,
    # which makes logout appear broken. Keep Basic Auth for API/curl flows.
    if _wants_html():
        return None

    auth = request.authorization
    if not auth or not auth.username:
        return None

    ctx = authenticate_credentials(auth.username, auth.password or "")
    if ctx:
        g._auth_ctx = ctx
    return ctx


def requires_basic_auth(fn: Callable):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ctx = _load_auth_context()
        if not ctx:
            return _unauthorized()
        return fn(*args, **kwargs)

    return wrapper


def current_auth_context() -> AuthContext | None:
    return _load_auth_context()


def current_actor() -> str:
    ctx = _load_auth_context()
    return ctx.username if ctx else "anonymous"


def current_roles() -> tuple[str, ...]:
    ctx = _load_auth_context()
    return ctx.roles if ctx else tuple()


def is_authenticated() -> bool:
    return _load_auth_context() is not None


def current_requires_password_change() -> bool:
    ctx = _load_auth_context()
    return bool(ctx.require_password_change) if ctx else False


def has_permission(permission_code: str) -> bool:
    ctx = _load_auth_context()
    if not ctx:
        return False
    if ALL_PERMISSIONS in ctx.permissions:
        return True
    return permission_code in ctx.permissions


def _auth_failure():
    if _wants_html() and request.method == "GET":
        return redirect(url_for("login", next=request.full_path if request.query_string else request.path))
    return _unauthorized()


def requires_permission(permission_code: str):
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            ctx = _load_auth_context()
            if not ctx:
                return _auth_failure()
            if not has_permission(permission_code):
                abort(403)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def requires_any_permission(*permission_codes: str):
    wanted = tuple(x for x in permission_codes if x)

    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            ctx = _load_auth_context()
            if not ctx:
                return _auth_failure()
            if ALL_PERMISSIONS in ctx.permissions:
                return fn(*args, **kwargs)
            if not any(code in ctx.permissions for code in wanted):
                abort(403)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def validate_password_policy(password: str) -> list[str]:
    errors: list[str] = []
    if len(password or "") < max(1, settings.password_min_length):
        errors.append(f"รหัสผ่านต้องยาวอย่างน้อย {settings.password_min_length} ตัวอักษร")
    if settings.password_require_upper and not any(ch.isupper() for ch in password or ""):
        errors.append("รหัสผ่านต้องมีตัวพิมพ์ใหญ่อย่างน้อย 1 ตัว")
    if settings.password_require_lower and not any(ch.islower() for ch in password or ""):
        errors.append("รหัสผ่านต้องมีตัวพิมพ์เล็กอย่างน้อย 1 ตัว")
    if settings.password_require_digit and not any(ch.isdigit() for ch in password or ""):
        errors.append("รหัสผ่านต้องมีตัวเลขอย่างน้อย 1 ตัว")
    return errors


def get_csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_hex(32)
        session["_csrf_token"] = token
    return token


def validate_csrf() -> None:
    token = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if not token or token != session.get("_csrf_token"):
        abort(400, description="Invalid CSRF token")


def csrf_protect(fn: Callable):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        validate_csrf()
        return fn(*args, **kwargs)

    return wrapper
