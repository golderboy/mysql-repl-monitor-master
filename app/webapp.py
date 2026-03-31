from __future__ import annotations

import json
from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_from_directory, session, url_for

from app.config import settings
from app.db import get_monitor_conn, get_web_conn
from app.repositories.deep_repo import (
    get_deep_run_detail,
    list_deep_result_statuses,
    list_deep_results,
    list_deep_runs,
)
from app.repositories.incident_repo import (
    add_incident_event,
    add_incident_note,
    get_incident_detail,
    get_incident_events,
    get_incident_notes,
    get_last_event_hash,
    list_incidents,
    update_incident_fields,
)
from app.repositories.monitor_repo import (
    fetch_dashboard_summary,
    fetch_open_incidents,
    fetch_recent_mismatches,
    fetch_recent_runs,
    get_server_choices,
)
from app.repositories.schema_repo import (
    get_schema_run_detail,
    list_schema_diff_types,
    list_schema_diffs,
    list_schema_runs,
    list_schema_tables,
)
from app.repositories.settings_repo import list_settings, list_watchlist, upsert_setting, update_watchlist_item
from app.repositories.maintenance_repo import get_setting_map, list_export_archives
from app.repositories.extra_repo import (
    add_alert_policy,
    add_alert_target,
    add_web_audit_log,
    cancel_job_run,
    finish_job_failed,
    finish_job_success,
    get_alert_overview,
    get_job_run_detail,
    list_daily_stats,
    list_hourly_stats,
    list_job_runs,
    queue_job_run,
)
from app.repositories.report_repo import (
    create_report_export_request,
    finish_report_failed,
    finish_report_success,
    get_report_export,
    list_report_exports,
    mark_report_running,
)
from app.repositories.access_repo import (
    approve_change_request,
    create_change_request,
    create_user,
    get_change_request,
    list_change_requests,
    list_roles,
    list_users,
    reject_change_request,
    set_user_active,
    set_user_password,
    set_user_roles,
    mark_change_request_applied,
    list_web_audit_logs,
)
from app.security import (
    authenticate_credentials,
    authenticate_credentials_detailed,
    csrf_protect,
    current_actor,
    current_auth_context,
    current_requires_password_change,
    current_roles,
    get_csrf_token,
    has_permission,
    is_authenticated,
    login_user,
    logout_user,
    requires_any_permission,
    requires_basic_auth,
    requires_permission,
    validate_password_policy,
)
from app.services.deep_compare_service import execute_deep_compare
from app.services.event_hash_service import stable_hash
from app.services.schema_compare_service import execute_schema_compare
from app.services.retention_service import get_retention_plan, run_retention_cleanup
from app.services.evidence_export_service import get_export_config, run_evidence_export
from app.services.report_service import REPORT_EXPORT_DIR, build_report_dataset, generate_report_export, normalize_period_days


app = Flask(__name__)
app.config["SECRET_KEY"] = settings.secret_key
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


def _wants_html_response() -> bool:
    best = request.accept_mimetypes.best_match(["text/html", "application/json", "text/plain"])
    return best == "text/html"


@app.errorhandler(403)
def handle_forbidden(exc):
    if _wants_html_response():
        flash("คุณไม่มีสิทธิ์ใช้งานส่วนนี้", "error")
        target = request.referrer or _landing_url()
        return redirect(target)
    return jsonify({"status": "error", "message": "forbidden"}), 403


@app.errorhandler(400)
def handle_bad_request(exc):
    if _wants_html_response():
        return render_template("error.html", code=400, title="400 Bad request", heading="คำขอไม่ถูกต้อง", message=getattr(exc, "description", None) or "กรอกข้อมูลไม่ครบหรือรูปแบบไม่ถูกต้อง", back_url=request.referrer, home_url=_landing_url()), 400
    return jsonify({"status": "error", "message": getattr(exc, "description", "bad_request")}), 400


@app.errorhandler(404)
def handle_not_found(exc):
    if _wants_html_response():
        return render_template("error.html", code=404, title="404 Not found", heading="ไม่พบหน้าที่ต้องการ", message="เส้นทางนี้ไม่มีอยู่ในระบบ หรืออาจเป็นหน้าที่ผู้ใช้ไม่มีสิทธิ์เข้าถึงจากเมนูปัจจุบัน", back_url=request.referrer, home_url=_landing_url()), 404
    return jsonify({"status": "error", "message": "not_found"}), 404


@app.errorhandler(500)
def handle_server_error(exc):
    if _wants_html_response():
        return render_template("error.html", code=500, title="500 Internal server error", heading="ระบบเกิดข้อผิดพลาดภายใน", message="ดู app log / gunicorn / Apache error log แล้วลองใหม่อีกครั้งหลังแก้สาเหตุ", back_url=request.referrer, home_url=_landing_url()), 500
    return jsonify({"status": "error", "message": "internal_server_error"}), 500


@app.context_processor
def inject_common_template_values():
    return {
        "csrf_token": get_csrf_token,
        "current_actor": current_actor,
        "current_roles": current_roles,
        "current_requires_password_change": current_requires_password_change,
        "has_permission": has_permission,
    }


def _landing_endpoint() -> str:
    candidates = [
        ("dashboard.view", "dashboard"),
        ("incidents.view", "incidents"),
        ("trends.view", "trends_api"),
        ("jobs.view", "jobs_api"),
("alerts.view", "alerts_api"),
        ("reports.view", "reports_page"),
        ("settings.view", "settings_page"),
        ("maintenance.run", "maintenance_page"),
        ("access.view", "access_page"),
    ]
    for permission_code, endpoint in candidates:
        if has_permission(permission_code):
            return endpoint
    return "health"


def _landing_url() -> str:
    return url_for(_landing_endpoint())


def _clear_filter_state(filter_key: str) -> None:
    session.pop(f"filters:{filter_key}", None)


def _route_filters(filter_key: str, raw_filters: dict[str, str], defaults: dict[str, str] | None = None) -> dict[str, str]:
    defaults = defaults or {}
    if request.args.get("reset") == "1":
        _clear_filter_state(filter_key)
        return {key: defaults.get(key, "") for key in raw_filters}

    if not _wants_html_response():
        return {key: (value if value != "" else defaults.get(key, "")) for key, value in raw_filters.items()}

    session_key = f"filters:{filter_key}"
    saved = session.get(session_key) or {}
    resolved: dict[str, str] = {}
    has_explicit_value = False
    for key, value in raw_filters.items():
        if value != "":
            resolved[key] = value
            has_explicit_value = True
        elif key in saved:
            resolved[key] = saved.get(key, "")
        else:
            resolved[key] = defaults.get(key, "")
    if has_explicit_value or session_key not in session:
        session[session_key] = resolved
    return resolved


def _safe_int(value) -> int:
    try:
        if value is None or value == '':
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _aggregate_daily_stats(rows):
    grouped = {}
    fields = [
        'replication_ok_count',
        'replication_warn_count',
        'replication_critical_count',
        'replication_error_count',
        'mismatch_count',
        'incident_opened_count',
        'incident_recovered_count',
    ]
    for row in rows or []:
        key = str(row.get('stat_date') or '')
        if not key:
            continue
        bucket = grouped.setdefault(key, {field: 0 for field in fields})
        for field in fields:
            bucket[field] += _safe_int(row.get(field))
    categories = sorted(grouped)
    return categories, [grouped[key] for key in categories]


def _aggregate_hourly_stats(rows):
    grouped = {}
    for row in rows or []:
        key = str(row.get('bucket_hour') or '')
        if not key:
            continue
        bucket = grouped.setdefault(key, {'lag_max_sec': 0, 'lag_avg_sum': 0, 'lag_avg_count': 0, 'error_count': 0})
        bucket['lag_max_sec'] = max(bucket['lag_max_sec'], _safe_int(row.get('lag_max_sec')))
        lag_avg = row.get('lag_avg_sec')
        if lag_avg not in (None, ''):
            bucket['lag_avg_sum'] += _safe_int(lag_avg)
            bucket['lag_avg_count'] += 1
        bucket['error_count'] += _safe_int(row.get('error_count'))
    categories = sorted(grouped)
    payload = []
    for key in categories:
        row = grouped[key]
        payload.append({
            'lag_max_sec': row['lag_max_sec'],
            'lag_avg_sec': round(row['lag_avg_sum'] / row['lag_avg_count'], 2) if row['lag_avg_count'] else 0,
            'error_count': row['error_count'],
        })
    return categories, payload


def _series_chart_config(*, title: str, categories: list[str], series: list[dict], chart_type: str = 'line', y_title: str | None = None, stacking: str | None = None):
    plot_options = {chart_type: {}}
    if stacking:
        plot_options[chart_type]['stacking'] = stacking
    return {
        'chart': {'type': chart_type, 'backgroundColor': 'transparent', 'height': 320},
        'title': {'text': title},
        'credits': {'enabled': False},
        'xAxis': {'categories': categories, 'crosshair': True},
        'yAxis': {'title': {'text': y_title or None}, 'allowDecimals': False},
        'legend': {'enabled': True},
        'tooltip': {'shared': True},
        'plotOptions': plot_options,
        'series': series,
    }


def _build_daily_error_chart(rows, *, title: str):
    categories, buckets = _aggregate_daily_stats(rows)
    return _series_chart_config(
        title=title,
        categories=categories,
        chart_type='column',
        y_title='events',
        stacking='normal',
        series=[
            {'name': 'Warning', 'data': [_safe_int(row['replication_warn_count']) for row in buckets]},
            {'name': 'Critical', 'data': [_safe_int(row['replication_critical_count']) for row in buckets]},
            {'name': 'Error', 'data': [_safe_int(row['replication_error_count']) for row in buckets]},
            {'name': 'Mismatch', 'data': [_safe_int(row['mismatch_count']) for row in buckets]},
        ],
    )


def _build_daily_incident_chart(rows, *, title: str):
    categories, buckets = _aggregate_daily_stats(rows)
    return _series_chart_config(
        title=title,
        categories=categories,
        chart_type='line',
        y_title='incidents',
        series=[
            {'name': 'Opened', 'data': [_safe_int(row['incident_opened_count']) for row in buckets]},
            {'name': 'Recovered', 'data': [_safe_int(row['incident_recovered_count']) for row in buckets]},
        ],
    )


def _build_hourly_lag_chart(rows, *, title: str):
    categories, buckets = _aggregate_hourly_stats(rows)
    return {
        'chart': {'zoomType': 'xy', 'backgroundColor': 'transparent', 'height': 320},
        'title': {'text': title},
        'credits': {'enabled': False},
        'xAxis': [{'categories': categories, 'crosshair': True}],
        'yAxis': [
            {'title': {'text': 'Lag (sec)'}, 'allowDecimals': False},
            {'title': {'text': 'Error count'}, 'allowDecimals': False, 'opposite': True},
        ],
        'tooltip': {'shared': True},
        'series': [
            {'name': 'Lag max', 'type': 'spline', 'data': [_safe_int(row['lag_max_sec']) for row in buckets], 'yAxis': 0},
            {'name': 'Lag avg', 'type': 'spline', 'data': [float(row['lag_avg_sec']) for row in buckets], 'yAxis': 0},
            {'name': 'Errors', 'type': 'column', 'data': [_safe_int(row['error_count']) for row in buckets], 'yAxis': 1},
        ],
    }


def _build_server_comparison_chart(rows, *, title: str):
    rows = list(rows or [])
    categories = [str(row.get('server_code') or row.get('server_id') or '-') for row in rows]
    return _series_chart_config(
        title=title,
        categories=categories,
        chart_type='bar',
        y_title='events',
        series=[
            {'name': 'Critical + Error', 'data': [_safe_int(row.get('replication_critical_count')) + _safe_int(row.get('replication_error_count')) for row in rows]},
            {'name': 'Mismatch', 'data': [_safe_int(row.get('mismatch_count')) for row in rows]},
            {'name': 'Opened', 'data': [_safe_int(row.get('incident_opened_count')) for row in rows]},
        ],
    )


def _latest_daily_snapshot(rows):
    rows = list(rows or [])
    if not rows:
        return None, []
    latest = max(str(row.get('stat_date') or '') for row in rows)
    latest_rows = [row for row in rows if str(row.get('stat_date') or '') == latest]
    return latest, latest_rows


def _executive_insights(dataset: dict):
    insights = []
    summary_rows = dataset.get('summary_rows') or []
    totals = dataset.get('totals') or {}
    if summary_rows:
        ranked = sorted(summary_rows, key=lambda row: (_safe_int(row.get('replication_error_count')) + _safe_int(row.get('mismatch_count')) + _safe_int(row.get('replication_critical_count'))), reverse=True)
        worst = ranked[0]
        insights.append(f"Server ที่มีสัญญาณเสี่ยงสูงสุดในช่วงนี้คือ {worst.get('server_code') or worst.get('server_id') or '-'}")
    insights.append(f"ช่วงรายงานนี้พบ error รวม {totals.get('replication_error_count', 0)} ครั้ง และ mismatch รวม {totals.get('mismatch_count', 0)} ครั้ง")
    opened = _safe_int(totals.get('incident_opened_count'))
    recovered = _safe_int(totals.get('incident_recovered_count'))
    if opened > 0:
        insights.append(f"อัตราการ recover เทียบกับ incident ที่เปิดใหม่อยู่ที่ {round((recovered / opened) * 100, 1)}%")
    else:
        insights.append('ไม่พบ incident ใหม่ในช่วงรายงานนี้')
    return insights

def _empty_report_dataset(report_type: str, period_days: int) -> dict:
    return {
        "report_type": report_type,
        "period_days": period_days,
        "generated_at": None,
        "summary_rows": [],
        "daily_rows": [],
        "totals": {
            "replication_ok_count": 0,
            "replication_warn_count": 0,
            "replication_critical_count": 0,
            "replication_error_count": 0,
            "mismatch_count": 0,
            "incident_opened_count": 0,
            "incident_recovered_count": 0,
        },
    }


def _safe_build_report_dataset(conn, *, report_type: str, period_days: int) -> dict:
    try:
        dataset = build_report_dataset(conn, report_type=report_type, period_days=period_days)
        if isinstance(dataset, dict):
            return dataset
    except Exception:
        pass
    return _empty_report_dataset(report_type, period_days)


def _audit(conn, actor: str, action: str, object_type: str, object_id: int | None = None, details: dict | None = None):
    add_web_audit_log(
        conn,
        actor=actor,
        action=action,
        object_type=object_type,
        object_id=object_id,
        details=details or {},
    )


def _build_change_request_previews(conn, requests: list[dict]) -> list[dict]:
    settings_rows = {row['setting_key']: row for row in list_settings(conn)}
    watch_rows = {int(row['id']): row for row in list_watchlist(conn)}
    resolved = []
    for row in requests:
        payload = json.loads(row.get('payload_json') or '{}')
        preview_summary = ''
        preview_details = {}
        if row.get('request_type') == 'setting':
            key = (payload.get('setting_key') or row.get('target_key') or '').strip()
            requested_value = payload.get('setting_value')
            current_value = (settings_rows.get(key) or {}).get('setting_value')
            preview_summary = f"setting {key}: {current_value!r} -> {requested_value!r}"
            preview_details = {'setting_key': key, 'current_value': current_value, 'requested_value': requested_value}
        elif row.get('request_type') == 'watchlist':
            watch_id = int(payload.get('watch_id') or row.get('target_key') or 0)
            current = watch_rows.get(watch_id) or {}
            preview_details = {
                'watch_id': watch_id,
                'enabled': {'current': current.get('enabled'), 'requested': payload.get('enabled')},
                'priority': {'current': current.get('priority'), 'requested': payload.get('priority')},
                'compare_strategy': {'current': current.get('compare_strategy'), 'requested': payload.get('compare_strategy')},
                'note': {'current': current.get('note'), 'requested': payload.get('note')},
            }
            preview_summary = f"watchlist {watch_id}: enabled {current.get('enabled')} -> {payload.get('enabled')}, priority {current.get('priority')} -> {payload.get('priority')}"
        row = dict(row)
        row['preview_summary'] = preview_summary
        row['preview_json'] = json.dumps(preview_details, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        resolved.append(row)
    return resolved


def _apply_config_request(conn, row: dict):
    import json

    payload = json.loads(row.get("payload_json") or "{}")
    request_type = row.get("request_type")
    if request_type == "setting":
        key = (payload.get("setting_key") or "").strip()
        value = (payload.get("setting_value") or "").strip()
        if not key:
            raise ValueError("setting_key is required")
        upsert_setting(conn, key, value)
        return {"request_type": "setting", "setting_key": key}
    if request_type == "watchlist":
        watch_id = int(payload.get("watch_id") or 0)
        if watch_id <= 0:
            raise ValueError("watch_id is required")
        update_watchlist_item(
            conn,
            watch_id,
            1 if payload.get("enabled") else 0,
            int(payload.get("priority") or 100),
            (payload.get("compare_strategy") or "business_counter").strip(),
            (payload.get("note") or "").strip() or None,
        )
        return {"request_type": "watchlist", "watch_id": watch_id}
    raise ValueError(f"Unsupported request_type: {request_type}")


@app.before_request
def enforce_password_change():
    if request.endpoint in {"login", "login_post", "logout", "change_password", "change_password_post", "static", None}:
        return None
    if is_authenticated() and current_requires_password_change():
        return redirect(url_for("change_password"))
    return None


@app.get("/login")
def login():
    if is_authenticated():
        return redirect(request.args.get("next") or _landing_url())
    return render_template("login.html", next_url=request.args.get("next") or "", hide_chrome=True)


@app.post("/login")
@csrf_protect
def login_post():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    next_url = (request.form.get("next") or "").strip()
    ctx, auth_reason = authenticate_credentials_detailed(username, password)
    if not ctx:
        conn = get_web_conn()
        try:
            _audit(conn, username or "anonymous", "auth.login_failed", "session", details={"reason": auth_reason, "username": username})
        finally:
            conn.close()
        if auth_reason == "locked":
            flash("บัญชีนี้ถูกล็อกชั่วคราวจากการพยายามเข้าสู่ระบบผิดหลายครั้ง", "error")
        elif auth_reason == "inactive":
            flash("บัญชีนี้ถูกปิดใช้งาน", "error")
        else:
            flash("ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง", "error")
        return render_template("login.html", next_url=next_url, hide_chrome=True), 401
    login_user(ctx)
    conn = get_web_conn()
    try:
        _audit(conn, ctx.username, "auth.login", "session", details={"source": ctx.source})
    finally:
        conn.close()
    if ctx.require_password_change:
        flash("กรุณาเปลี่ยนรหัสผ่านก่อนใช้งานต่อ", "warning")
        return redirect(url_for("change_password"))
    return redirect(next_url or _landing_url())


@app.post("/logout")
@csrf_protect
def logout():
    actor = current_actor()
    if is_authenticated():
        conn = get_web_conn()
        try:
            _audit(conn, actor, "auth.logout", "session")
        finally:
            conn.close()
    logout_user()
    session.pop("_csrf_token", None)
    flash("ออกจากระบบแล้ว", "info")
    return redirect(url_for("login"))


@app.get("/me/password")
@requires_basic_auth
def change_password():
    return render_template("change_password.html", hide_chrome=True)


@app.post("/me/password")
@requires_basic_auth
@csrf_protect
def change_password_post():
    from werkzeug.security import generate_password_hash

    ctx = current_auth_context()
    if not ctx or ctx.source != "db" or not ctx.user_id:
        abort(403)

    password = request.form.get("password") or ""
    password_confirm = request.form.get("password_confirm") or ""
    policy_errors = validate_password_policy(password)
    if password != password_confirm:
        policy_errors.append("รหัสผ่านยืนยันไม่ตรงกัน")
    if policy_errors:
        for message in policy_errors:
            flash(message, "error")
        return render_template("change_password.html", hide_chrome=True), 400

    conn = get_web_conn()
    try:
        affected = set_user_password(conn, ctx.user_id, generate_password_hash(password), require_password_change=0)
        if affected:
            _audit(conn, ctx.username, "auth.change_password", "user", object_id=ctx.user_id)
    finally:
        conn.close()

    refreshed = authenticate_credentials(ctx.username, password)
    if refreshed:
        login_user(refreshed)
    flash("เปลี่ยนรหัสผ่านเรียบร้อยแล้ว", "success")
    return redirect(_landing_url())


@app.get("/")
@requires_permission("dashboard.view")
def dashboard():
    conn = get_web_conn()
    try:
        summary = fetch_dashboard_summary(conn)
        runs = fetch_recent_runs(conn, limit=20)
        incidents = fetch_open_incidents(conn, limit=20)
        mismatches = fetch_recent_mismatches(conn, limit=20)
        try:
            daily_rows = list_daily_stats(conn, days=14, limit=500)
        except Exception:
            daily_rows = []
        try:
            hourly_rows = list_hourly_stats(conn, hours=24, limit=500)
        except Exception:
            hourly_rows = []
    finally:
        conn.close()
    dashboard_error_chart = _build_daily_error_chart(daily_rows or [], title="Error trend overview")
    dashboard_lag_chart = _build_hourly_lag_chart(hourly_rows or [], title="Replication lag overview")
    return render_template(
        "dashboard.html",
        summary=summary,
        runs=runs,
        incidents=incidents,
        mismatches=mismatches,
        dashboard_error_chart=dashboard_error_chart,
        dashboard_lag_chart=dashboard_lag_chart,
    )


@app.get("/incidents")
@requires_permission("incidents.view")
def incidents():
    filters = _route_filters(
        "incidents",
        {
            "status": (request.args.get("status") or "").strip(),
            "issue_type": (request.args.get("issue_type") or "").strip(),
            "severity": (request.args.get("severity") or "").strip(),
            "q": (request.args.get("q") or "").strip(),
            "server_id": (request.args.get("server_id") or "").strip(),
        },
    )
    status = filters["status"] or None
    issue_type = filters["issue_type"] or None
    severity = filters["severity"] or None
    keyword = filters["q"] or None
    server_id_raw = filters["server_id"]
    server_id = int(server_id_raw) if server_id_raw.isdigit() else None

    conn = get_web_conn()
    try:
        rows = list_incidents(
            conn,
            status=status,
            issue_type=issue_type,
            severity=severity,
            server_id=server_id,
            keyword=keyword,
            limit=300,
        )
        servers = get_server_choices(conn)
    finally:
        conn.close()
    return render_template(
        "incidents.html",
        incidents=rows,
        servers=servers,
        filters=filters,
    )


@app.get("/incidents/<int:incident_id>")
@requires_permission("incidents.view")
def incident_detail(incident_id: int):
    conn = get_web_conn()
    try:
        incident = get_incident_detail(conn, incident_id)
        if not incident:
            abort(404)
        events = get_incident_events(conn, incident_id, limit=200)
        notes = get_incident_notes(conn, incident_id, limit=100)
    finally:
        conn.close()
    return render_template("incident_detail.html", incident=incident, events=events, notes=notes)


@app.post("/incidents/<int:incident_id>/status")
@requires_permission("incidents.manage")
@csrf_protect
def incident_update_status(incident_id: int):
    new_status = (request.form.get("new_status") or "").strip().upper() or None
    owner = (request.form.get("owner") or "").strip() or None
    root_cause = (request.form.get("root_cause") or "").strip() or None
    corrective_action = (request.form.get("corrective_action") or "").strip() or None
    summary_result = (request.form.get("summary_result") or "").strip() or None
    note_text = (request.form.get("note_text") or "").strip() or None

    valid_statuses = {None, "OPEN", "ACKNOWLEDGED", "INVESTIGATING", "RECOVERED", "CLOSED"}
    if new_status not in valid_statuses:
        abort(400, description="Invalid status")

    actor = current_actor()
    conn = get_web_conn()
    try:
        incident = get_incident_detail(conn, incident_id)
        if not incident:
            abort(404)

        update_incident_fields(
            conn,
            incident_id=incident_id,
            current_status=new_status,
            owner=owner,
            root_cause=root_cause,
            corrective_action=corrective_action,
            summary_result=summary_result,
        )

        status_changed = new_status is not None and new_status != incident["current_status"]
        if status_changed:
            event_type = {
                "ACKNOWLEDGED": "acknowledged",
                "RECOVERED": "recovered",
                "CLOSED": "closed",
            }.get(new_status, "manual_note")
            prev_hash = get_last_event_hash(conn, incident_id)
            message = f"Status changed from {incident['current_status']} to {new_status}"
            event_hash = stable_hash({
                "incident_id": incident_id,
                "event_type": event_type,
                "old_status": incident["current_status"],
                "new_status": new_status,
                "message": message,
                "created_by": actor,
            })
            add_incident_event(
                conn,
                incident_id=incident_id,
                event_type=event_type,
                old_status=incident["current_status"],
                new_status=new_status,
                message=message,
                created_by=actor,
                prev_event_hash=prev_hash,
                event_hash=event_hash,
            )

        if note_text:
            add_incident_note(conn, incident_id, "GENERAL", note_text, actor)
            prev_hash = get_last_event_hash(conn, incident_id)
            event_hash = stable_hash({
                "incident_id": incident_id,
                "event_type": "manual_note",
                "message": note_text,
                "created_by": actor,
            })
            add_incident_event(
                conn,
                incident_id=incident_id,
                event_type="manual_note",
                old_status=None,
                new_status=new_status or incident["current_status"],
                message=note_text,
                created_by=actor,
                prev_event_hash=prev_hash,
                event_hash=event_hash,
            )

        _audit(conn, actor, "update_incident", "incident", incident_id, {"new_status": new_status, "owner": owner, "has_note": bool(note_text)})

        flash("บันทึก incident สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("incident_detail", incident_id=incident_id))


@app.post("/incidents/<int:incident_id>/note")
@requires_permission("incidents.manage")
@csrf_protect
def incident_add_note(incident_id: int):
    note_type = (request.form.get("note_type") or "GENERAL").strip().upper()
    note_text = (request.form.get("note_text") or "").strip()
    if not note_text:
        flash("กรุณากรอก note", "error")
        return redirect(url_for("incident_detail", incident_id=incident_id))

    actor = current_actor()
    conn = get_web_conn()
    try:
        incident = get_incident_detail(conn, incident_id)
        if not incident:
            abort(404)
        add_incident_note(conn, incident_id, note_type, note_text, actor)
        _audit(conn, actor, "add_incident_note", "incident", incident_id, {"note_type": note_type})
        prev_hash = get_last_event_hash(conn, incident_id)
        event_hash = stable_hash({
            "incident_id": incident_id,
            "event_type": "manual_note",
            "note_type": note_type,
            "message": note_text,
            "created_by": actor,
        })
        add_incident_event(
            conn,
            incident_id=incident_id,
            event_type="manual_note",
            old_status=None,
            new_status=incident["current_status"],
            message=f"[{note_type}] {note_text}",
            created_by=actor,
            prev_event_hash=prev_hash,
            event_hash=event_hash,
        )
        flash("เพิ่ม note สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("incident_detail", incident_id=incident_id))


@app.get("/mismatches")
@requires_permission("dashboard.view")
def mismatches():
    conn = get_web_conn()
    try:
        rows = fetch_recent_mismatches(conn, limit=300)
    finally:
        conn.close()
    return render_template("mismatches.html", mismatches=rows)


@app.get("/schema-runs")
@requires_permission("jobs.view")
def schema_runs():
    conn = get_web_conn()
    try:
        rows = list_schema_runs(conn, limit=100)
        servers = [row for row in get_server_choices(conn) if row["role"] == "SLAVE"]
    finally:
        conn.close()
    return render_template("schema_runs.html", runs=rows, servers=servers)


@app.post("/schema-runs/run")
@requires_permission("jobs.manage")
@csrf_protect
def schema_run_execute():
    target_server_id = int(request.form.get("target_server_id", "0"))
    if target_server_id <= 0:
        abort(400, description="target_server_id is required")

    actor = current_actor()
    conn = get_monitor_conn()
    try:
        run_id = execute_schema_compare(conn, target_server_id=target_server_id, triggered_by=actor)
        _audit(conn, actor, "run_schema_compare", "schema_run", run_id, {"target_server_id": target_server_id})
        flash(f"สั่ง schema compare สำเร็จ run_id={run_id}", "success")
    except Exception as exc:
        flash(f"Schema compare failed: {exc}", "error")
        return redirect(url_for("schema_runs"))
    finally:
        conn.close()
    return redirect(url_for("schema_run_detail", run_id=run_id))


@app.get("/schema-runs/<int:run_id>")
@requires_permission("jobs.view")
def schema_run_detail(run_id: int):
    table_name = (request.args.get("table_name") or "").strip() or None
    diff_type = (request.args.get("diff_type") or "").strip() or None
    conn = get_web_conn()
    try:
        run_row = get_schema_run_detail(conn, run_id)
        if not run_row:
            abort(404)
        diffs = list_schema_diffs(conn, run_id, table_name=table_name, diff_type=diff_type, limit=2000)
        tables = list_schema_tables(conn, run_id)
        diff_types = list_schema_diff_types(conn, run_id)
    finally:
        conn.close()
    return render_template(
        "schema_run_detail.html",
        run_row=run_row,
        diffs=diffs,
        tables=tables,
        diff_types=diff_types,
        filters={
            "table_name": table_name or "",
            "diff_type": diff_type or "",
        },
    )


@app.get("/deep-runs")
@requires_permission("jobs.view")
def deep_runs():
    filters = _route_filters(
        "deep_runs",
        {"status": (request.args.get("status") or "").strip()},
    )
    status = filters["status"] or None
    conn = get_web_conn()
    try:
        rows = list_deep_runs(conn, status=status, limit=100)
        servers = [row for row in get_server_choices(conn) if row["role"] == "SLAVE"]
        watchlist = list_watchlist(conn)
    finally:
        conn.close()
    return render_template("deep_runs.html", runs=rows, servers=servers, watchlist=watchlist, filters=filters)


@app.post("/deep-runs/run")
@requires_permission("jobs.manage")
@csrf_protect
def deep_run_execute():
    target_server_id = int(request.form.get("target_server_id", "0"))
    db_name = (request.form.get("db_name") or "").strip()
    table_name = (request.form.get("table_name") or "").strip()
    pk_column = (request.form.get("pk_column") or "").strip()
    compare_scope = (request.form.get("compare_scope") or "").strip() or None
    chunk_size_raw = (request.form.get("chunk_size") or "1000").strip()
    chunk_size = int(chunk_size_raw) if chunk_size_raw.isdigit() else 1000

    if target_server_id <= 0 or not db_name or not table_name or not pk_column:
        abort(400, description="target_server_id, db_name, table_name, pk_column are required")
    if chunk_size <= 0:
        abort(400, description="chunk_size must be > 0")

    actor = current_actor()
    conn = get_monitor_conn()
    try:
        run_id = execute_deep_compare(
            conn,
            target_server_id=target_server_id,
            db_name=db_name,
            table_name=table_name,
            pk_column=pk_column,
            compare_scope=compare_scope,
            chunk_size=chunk_size,
            triggered_by=actor,
        )
        _audit(conn, actor, "run_deep_compare", "deep_run", run_id, {"target_server_id": target_server_id, "db_name": db_name, "table_name": table_name})
        flash(f"สั่ง deep compare สำเร็จ run_id={run_id}", "success")
    except Exception as exc:
        flash(f"Deep compare failed: {exc}", "error")
        return redirect(url_for("deep_runs"))
    finally:
        conn.close()
    return redirect(url_for("deep_run_detail", run_id=run_id))


@app.get("/deep-runs/<int:run_id>")
@requires_permission("jobs.view")
def deep_run_detail(run_id: int):
    result_status = (request.args.get("result_status") or "").strip() or None
    conn = get_web_conn()
    try:
        run_row = get_deep_run_detail(conn, run_id)
        if not run_row:
            abort(404)
        results = list_deep_results(conn, run_id, result_status=result_status, limit=5000)
        statuses = list_deep_result_statuses(conn, run_id)
    finally:
        conn.close()
    return render_template(
        "deep_run_detail.html",
        run_row=run_row,
        results=results,
        statuses=statuses,
        filters={"result_status": result_status or ""},
    )


@app.get("/settings")
@requires_permission("settings.view")
def settings_page():
    conn = get_web_conn()
    try:
        rows = list_settings(conn)
        watchlist = list_watchlist(conn)
    finally:
        conn.close()
    return render_template("settings.html", settings_rows=rows, watchlist=watchlist)


@app.get("/settings/advanced")
@requires_permission("settings.view")
def settings_advanced_page():
    conn = get_web_conn()
    try:
        settings_rows = list_settings(conn)
        watchlist = list_watchlist(conn)
        retention_plan = get_retention_plan(conn)
        export_config = get_export_config(conn)
    finally:
        conn.close()
    return render_template("settings_advanced.html", settings_rows=settings_rows, watchlist=watchlist, retention_plan=retention_plan, export_config=export_config, app_settings=settings, report_export_dir=str(REPORT_EXPORT_DIR))


@app.post("/settings")
@requires_any_permission("settings.manage", "settings.request")
@csrf_protect
def settings_update():
    action = (request.form.get("action") or "").strip()
    actor = current_actor()
    conn = get_web_conn()
    try:
        direct_apply = has_permission("settings.manage")
        if action == "save_setting":
            key = (request.form.get("setting_key") or "").strip()
            value = (request.form.get("setting_value") or "").strip()
            if not key:
                abort(400, description="setting_key is required")
            if direct_apply:
                upsert_setting(conn, key, value)
                _audit(conn, actor, "apply_setting", "setting", None, {"setting_key": key})
                flash(f"บันทึก setting {key} สำเร็จ", "success")
            else:
                request_id = create_change_request(
                    conn,
                    request_type="setting",
                    target_key=key,
                    payload={"setting_key": key, "setting_value": value},
                    requested_by=actor,
                )
                _audit(conn, actor, "request_setting_change", "config_request", request_id, {"setting_key": key})
                flash(f"ส่งคำขอเปลี่ยน setting {key} แล้ว request_id={request_id}", "success")
        elif action == "save_watch":
            watch_id = int(request.form.get("watch_id", "0"))
            enabled = 1 if request.form.get("enabled") == "1" else 0
            priority = int(request.form.get("priority", "100"))
            compare_strategy = (request.form.get("compare_strategy") or "business_counter").strip()
            note = (request.form.get("note") or "").strip() or None
            payload = {
                "watch_id": watch_id,
                "enabled": enabled,
                "priority": priority,
                "compare_strategy": compare_strategy,
                "note": note,
            }
            if direct_apply:
                update_watchlist_item(conn, watch_id, enabled, priority, compare_strategy, note)
                _audit(conn, actor, "apply_watchlist_change", "watchlist", watch_id, payload)
                flash(f"บันทึก watchlist id={watch_id} สำเร็จ", "success")
            else:
                request_id = create_change_request(
                    conn,
                    request_type="watchlist",
                    target_key=str(watch_id),
                    payload=payload,
                    requested_by=actor,
                )
                _audit(conn, actor, "request_watchlist_change", "config_request", request_id, {"watch_id": watch_id})
                flash(f"ส่งคำขอเปลี่ยน watchlist id={watch_id} แล้ว request_id={request_id}", "success")
        else:
            abort(400, description="Unknown settings action")
    finally:
        conn.close()
    return redirect(url_for("settings_page"))


@app.get("/maintenance")
@requires_permission("maintenance.run")
def maintenance_page():
    conn = get_web_conn()
    try:
        settings_map = get_setting_map(conn)
        retention_plan = get_retention_plan(conn)
        export_config = get_export_config(conn)
    finally:
        conn.close()
    archives = list_export_archives(export_config["export_dir"], limit=50)
    return render_template(
        "maintenance.html",
        settings_map=settings_map,
        retention_plan=retention_plan,
        export_config=export_config,
        archives=archives,
    )


@app.post("/maintenance/cleanup")
@requires_permission("maintenance.run")
@csrf_protect
def maintenance_cleanup():
    actor = current_actor()
    dry_run = request.form.get("dry_run") == "1"
    try:
        result = run_retention_cleanup(triggered_by=actor, dry_run=dry_run)
        deleted_total = sum(int(x.get("deleted_rows") or 0) for x in result["tables"])
        candidate_total = sum(int(x.get("candidate_rows") or 0) for x in result["tables"])
        conn = get_web_conn()
        try:
            _audit(conn, actor, "run_retention_cleanup", "maintenance", None, {"dry_run": dry_run, "deleted_total": deleted_total, "candidate_total": candidate_total})
        finally:
            conn.close()
        if dry_run:
            flash(f"Dry run เสร็จแล้ว พบ candidate_rows={candidate_total}", "success")
        else:
            flash(f"Retention cleanup เสร็จแล้ว deleted_rows={deleted_total}", "success")
    except Exception as exc:
        flash(f"Retention cleanup failed: {exc}", "error")
    return redirect(url_for("maintenance_page"))


@app.post("/maintenance/export")
@requires_permission("maintenance.run")
@csrf_protect
def maintenance_export():
    actor = current_actor()
    lookback_raw = (request.form.get("lookback_days") or "").strip()
    lookback_days = int(lookback_raw) if lookback_raw.isdigit() else None
    try:
        result = run_evidence_export(triggered_by=actor, lookback_days=lookback_days)
        conn = get_web_conn()
        try:
            _audit(conn, actor, "run_evidence_export", "maintenance", None, {"lookback_days": lookback_days, "archive_name": result.get('archive_name')})
        finally:
            conn.close()
        flash(f"Evidence export สำเร็จ: {result['archive_name']}", "success")
    except Exception as exc:
        flash(f"Evidence export failed: {exc}", "error")
    return redirect(url_for("maintenance_page"))


@app.get("/access")
@requires_permission("access.view")
def access_page():
    conn = get_web_conn()
    try:
        users = list_users(conn)
        roles = list_roles(conn)
        requests = _build_change_request_previews(conn, list_change_requests(conn, status=None, limit=200))
    finally:
        conn.close()
    return render_template("access.html", users=users, roles=roles, requests=requests)


@app.post("/access/users")
@requires_permission("access.manage")
@csrf_protect
def access_create_user():
    from werkzeug.security import generate_password_hash

    username = (request.form.get("username") or "").strip()
    display_name = (request.form.get("display_name") or "").strip() or None
    password = request.form.get("password") or ""
    role_codes = request.form.getlist("role_codes")
    if not username or not password:
        abort(400, description="username and password are required")
    policy_errors = validate_password_policy(password)
    if policy_errors:
        for message in policy_errors:
            flash(message, "error")
        return redirect(url_for("access_page"))
    actor = current_actor()
    conn = get_web_conn()
    try:
        user_id = create_user(conn, username, display_name, generate_password_hash(password), role_codes)
        _audit(conn, actor, "create_user", "user", user_id, {"username": username, "role_codes": role_codes})
        flash(f"สร้างผู้ใช้ {username} สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.post("/access/users/<int:user_id>/password")
@requires_permission("access.manage")
@csrf_protect
def access_reset_password(user_id: int):
    from werkzeug.security import generate_password_hash

    password = request.form.get("password") or ""
    require_change = 1 if request.form.get("require_password_change") == "1" else 0
    if not password:
        abort(400, description="password is required")
    policy_errors = validate_password_policy(password)
    if policy_errors:
        for message in policy_errors:
            flash(message, "error")
        return redirect(url_for("access_page"))
    actor = current_actor()
    conn = get_web_conn()
    try:
        affected = set_user_password(conn, user_id, generate_password_hash(password), require_password_change=require_change)
        if not affected:
            abort(404)
        _audit(conn, actor, "reset_user_password", "user", user_id, {"require_password_change": require_change})
        flash(f"รีเซ็ตรหัสผ่าน user_id={user_id} สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.post("/access/users/<int:user_id>/roles")
@requires_permission("access.manage")
@csrf_protect
def access_set_roles(user_id: int):
    role_codes = request.form.getlist("role_codes")
    actor = current_actor()
    conn = get_web_conn()
    try:
        set_user_roles(conn, user_id, role_codes)
        _audit(conn, actor, "set_user_roles", "user", user_id, {"role_codes": role_codes})
        flash(f"อัปเดต role ของ user_id={user_id} สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.post("/access/users/<int:user_id>/active")
@requires_permission("access.manage")
@csrf_protect
def access_set_active(user_id: int):
    is_active = 1 if request.form.get("is_active") == "1" else 0
    actor = current_actor()
    conn = get_web_conn()
    try:
        affected = set_user_active(conn, user_id, is_active)
        if not affected:
            abort(404)
        _audit(conn, actor, "set_user_active", "user", user_id, {"is_active": is_active})
        flash(f"อัปเดตสถานะ user_id={user_id} สำเร็จ", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.post("/access/requests/<int:request_id>/approve")
@requires_permission("settings.approve")
@csrf_protect
def access_approve_request(request_id: int):
    actor = current_actor()
    conn = get_web_conn()
    try:
        row = get_change_request(conn, request_id)
        if not row:
            abort(404)
        if row.get("status") not in {"pending", "approved"}:
            abort(400, description="request already finalized")
        if row.get("status") == "pending":
            approve_change_request(conn, request_id, actor)
        details = _apply_config_request(conn, row)
        mark_change_request_applied(conn, request_id, actor)
        _audit(conn, actor, "approve_config_request", "config_request", request_id, details)
        flash(f"อนุมัติและนำคำขอ {request_id} ไปใช้แล้ว", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.post("/access/requests/<int:request_id>/reject")
@requires_permission("settings.approve")
@csrf_protect
def access_reject_request(request_id: int):
    actor = current_actor()
    reason_text = (request.form.get("reason_text") or "").strip() or None
    conn = get_web_conn()
    try:
        affected = reject_change_request(conn, request_id, actor, reason_text)
        if not affected:
            abort(404)
        _audit(conn, actor, "reject_config_request", "config_request", request_id, {"reason_text": reason_text})
        flash(f"ปฏิเสธคำขอ {request_id} แล้ว", "success")
    finally:
        conn.close()
    return redirect(url_for("access_page"))


@app.get("/access/audit")
@requires_permission("access.view")
def access_audit_page():
    actor_filter = (request.args.get("actor") or "").strip()
    action_prefix = (request.args.get("action_prefix") or "").strip()
    limit_raw = (request.args.get("limit") or "100").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 100
    conn = get_web_conn()
    try:
        rows = list_web_audit_logs(
            conn,
            actor=actor_filter or None,
            action_prefix=action_prefix or None,
            limit=max(1, min(limit, 500)),
        )
    finally:
        conn.close()
    if _wants_html_response():
        return render_template(
            "audit_logs.html",
            items=rows,
            filters={"actor": actor_filter, "action_prefix": action_prefix, "limit": str(max(1, min(limit, 500)))},
        )
    return jsonify({"status": "ok", "items": rows, "filters": {"actor": actor_filter, "action_prefix": action_prefix, "limit": max(1, min(limit, 500))}})


@app.get("/health")
def health():
    return {"status": "ok", "app_env": settings.env}


@app.get("/trends")
@requires_permission("trends.view")
def trends_api():
    filters = _route_filters(
        "trends",
        {
            "days": (request.args.get("days") or "").strip(),
            "hours": (request.args.get("hours") or "").strip(),
            "server_id": (request.args.get("server_id") or "").strip(),
        },
        defaults={"days": "7", "hours": "24", "server_id": ""},
    )
    days = int(filters["days"]) if filters["days"].isdigit() else 7
    hours = int(filters["hours"]) if filters["hours"].isdigit() else 24
    server_id_raw = filters["server_id"]
    server_id = int(server_id_raw) if server_id_raw.isdigit() else None

    conn = get_web_conn()
    try:
        daily = list_daily_stats(conn, days=max(1, days), server_id=server_id)
        hourly = list_hourly_stats(conn, hours=max(1, hours), server_id=server_id)
        servers = get_server_choices(conn)
    finally:
        conn.close()

    response_filters = {"days": max(1, days), "hours": max(1, hours), "server_id": server_id}
    daily_chart = _build_daily_error_chart(daily, title="Daily error and mismatch trend")
    incident_chart = _build_daily_incident_chart(daily, title="Incident open / recover trend")
    lag_chart = _build_hourly_lag_chart(hourly, title="Hourly replication lag and error trend")
    latest_day, latest_rows = _latest_daily_snapshot(daily)
    server_chart = _build_server_comparison_chart(latest_rows, title=f"Latest daily server comparison ({latest_day or '-'} )")
    if _wants_html_response():
        return render_template(
            "trends.html",
            daily=daily,
            hourly=hourly,
            servers=servers,
            filters={"days": str(response_filters["days"]), "hours": str(response_filters["hours"]), "server_id": server_id_raw},
            daily_chart=daily_chart,
            incident_chart=incident_chart,
            lag_chart=lag_chart,
            server_chart=server_chart,
            latest_day=latest_day,
        )

    return jsonify({
        "status": "ok",
        "filters": response_filters,
        "daily": daily,
        "hourly": hourly,
        "charts": {
            "daily": daily_chart,
            "incidents": incident_chart,
            "lag": lag_chart,
            "servers": server_chart,
        },
    })


@app.get("/jobs")
@requires_permission("jobs.view")
def jobs_api():
    filters = _route_filters(
        "jobs",
        {
            "limit": (request.args.get("limit") or "").strip(),
            "status": (request.args.get("status") or "").strip(),
        },
        defaults={"limit": "100", "status": ""},
    )
    status = filters["status"] or None
    limit = int(filters["limit"]) if filters["limit"].isdigit() else 100

    conn = get_web_conn()
    try:
        rows = list_job_runs(conn, status=status, limit=max(1, min(limit, 500)))
    finally:
        conn.close()
    if _wants_html_response():
        return render_template(
            "jobs.html",
            items=rows,
            filters={"limit": str(max(1, min(limit, 500))), "status": status or ""},
        )
    return jsonify({"status": "ok", "items": rows})


@app.get("/jobs/<int:run_id>")
@requires_permission("jobs.view")
def job_detail_api(run_id: int):
    conn = get_web_conn()
    try:
        row = get_job_run_detail(conn, run_id)
    finally:
        conn.close()
    if not row:
        abort(404)
    if _wants_html_response():
        return render_template("job_detail.html", row=row)
    return jsonify({"status": "ok", "item": row})


@app.post("/jobs/enqueue")
@requires_permission("jobs.manage")
def jobs_enqueue_api():
    payload = request.get_json(silent=True) or {}
    job_name = (payload.get("job_name") or payload.get("name") or "").strip()
    if not job_name:
        abort(400, description="job_name is required")

    actor = current_actor()
    conn = get_web_conn()
    try:
        run_id = queue_job_run(
            conn,
            job_name=job_name,
            requested_by=actor,
            payload=payload.get("payload") or payload.get("params") or {},
            server_id=payload.get("server_id"),
        )
        add_web_audit_log(
            conn,
            actor=actor,
            action="enqueue_job",
            object_type="job_run",
            object_id=run_id,
            details={"job_name": job_name},
        )
    finally:
        conn.close()
    return jsonify({"status": "ok", "run_id": run_id}), 201


@app.post("/jobs/<int:run_id>/cancel")
@requires_permission("jobs.manage")
@csrf_protect
def jobs_cancel_api(run_id: int):
    actor = current_actor()
    conn = get_web_conn()
    try:
        affected = cancel_job_run(conn, run_id)
        if not affected:
            abort(404)
        add_web_audit_log(
            conn,
            actor=actor,
            action="cancel_job",
            object_type="job_run",
            object_id=run_id,
            details={"run_id": run_id},
        )
    finally:
        conn.close()
    if _wants_html_response():
        flash(f"ส่งคำขอ cancel job {run_id} แล้ว", "success")
        return redirect(url_for("job_detail_api", run_id=run_id))
    return jsonify({"status": "ok", "run_id": run_id, "cancel_requested": True})


@app.get("/reports/daily")
@requires_permission("reports.view")
def reports_daily_page():
    days_raw = (request.args.get("days") or "7").strip()
    days = int(days_raw) if days_raw.isdigit() else 7
    days = max(1, min(days, 90))
    conn = get_web_conn()
    try:
        dataset = _safe_build_report_dataset(conn, report_type="daily", period_days=days)
    finally:
        conn.close()
    daily_chart = _build_daily_error_chart(dataset.get('daily_rows') or [], title="Daily report error trend")
    incident_chart = _build_daily_incident_chart(dataset.get('daily_rows') or [], title="Daily report incident trend")
    server_chart = _build_server_comparison_chart(dataset.get('summary_rows') or [], title="Per-server issue comparison")
    if _wants_html_response():
        return render_template(
            "reports_daily.html",
            dataset=dataset,
            filters={"days": str(days)},
            daily_chart=daily_chart,
            incident_chart=incident_chart,
            server_chart=server_chart,
        )
    return jsonify({"status": "ok", "dataset": dataset, "charts": {"daily": daily_chart, "incidents": incident_chart, "servers": server_chart}})


@app.get("/reports/admin")
@requires_permission("reports.view")
def reports_admin_page():
    days_raw = (request.args.get("days") or "7").strip()
    days = int(days_raw) if days_raw.isdigit() else 7
    days = max(1, min(days, 90))
    conn = get_web_conn()
    try:
        dataset = _safe_build_report_dataset(conn, report_type="daily", period_days=days)
        incidents = fetch_open_incidents(conn, limit=10)
        jobs = list_job_runs(conn, limit=10)
        exports = list_report_exports(conn, limit=10)
    finally:
        conn.close()
    daily_chart = _build_daily_error_chart(dataset.get('daily_rows') or [], title="Admin daily error trend")
    incident_chart = _build_daily_incident_chart(dataset.get('daily_rows') or [], title="Admin incident trend")
    server_chart = _build_server_comparison_chart(dataset.get('summary_rows') or [], title="Server comparison for admin review")
    if _wants_html_response():
        return render_template(
            "reports_admin.html",
            dataset=dataset,
            incidents=incidents,
            jobs=jobs,
            exports=exports,
            filters={"days": str(days)},
            daily_chart=daily_chart,
            incident_chart=incident_chart,
            server_chart=server_chart,
        )
    return jsonify({
        "status": "ok",
        "filters": {"days": days},
        "dataset": dataset,
        "incidents": incidents,
        "jobs": jobs,
        "exports": exports,
        "charts": {"daily": daily_chart, "incidents": incident_chart, "servers": server_chart},
    })


@app.get("/reports/executive")
@requires_permission("reports.view")
def reports_executive_page():
    days_raw = (request.args.get("days") or "30").strip()
    days = int(days_raw) if days_raw.isdigit() else 30
    days = max(7, min(days, 180))
    conn = get_web_conn()
    try:
        dataset = _safe_build_report_dataset(conn, report_type="monthly", period_days=days)
    finally:
        conn.close()
    daily_chart = _build_daily_error_chart(dataset.get('daily_rows') or [], title="Executive error trend")
    incident_chart = _build_daily_incident_chart(dataset.get('daily_rows') or [], title="Executive incident trend")
    server_chart = _build_server_comparison_chart(dataset.get('summary_rows') or [], title="Server comparison (risk focus)")
    insights = _executive_insights(dataset)
    if _wants_html_response():
        return render_template(
            "reports_executive.html",
            dataset=dataset,
            filters={"days": str(days)},
            daily_chart=daily_chart,
            incident_chart=incident_chart,
            server_chart=server_chart,
            insights=insights,
        )
    return jsonify({
        "status": "ok",
        "filters": {"days": days},
        "dataset": dataset,
        "insights": insights,
        "charts": {"daily": daily_chart, "incidents": incident_chart, "servers": server_chart},
    })


@app.get("/reports")
@requires_permission("reports.view")
def reports_page():
    limit_raw = (request.args.get("limit") or "50").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 50
    conn = get_web_conn()
    try:
        rows = list_report_exports(conn, limit=max(1, min(limit, 200)))
        overview_dataset = _safe_build_report_dataset(conn, report_type="monthly", period_days=30)
    finally:
        conn.close()
    if _wants_html_response():
        return render_template(
            "reports.html",
            items=rows,
            filters={"limit": str(max(1, min(limit, 200)))},
            overview_dataset=overview_dataset,
            overview_chart=_build_daily_error_chart(overview_dataset.get('daily_rows') or [], title="30-day error trend"),
        )
    return jsonify({"status": "ok", "items": rows, "overview": overview_dataset})


@app.post("/reports/export")
@requires_permission("reports.manage")
@csrf_protect
def reports_export():
    actor = current_actor()
    report_type = (request.form.get("report_type") or "daily").strip().lower()
    export_format = (request.form.get("export_format") or "csv").strip().lower()
    report_view = (request.form.get("report_view") or "").strip().lower()
    delivery = (request.form.get("delivery") or "download").strip().lower()
    period_days_raw = (request.form.get("period_days") or "0").strip()
    period_days = int(period_days_raw) if period_days_raw.isdigit() else 0
    next_page = (request.form.get("next_page") or "").strip()
    if next_page not in {"/reports", "/reports/daily", "/reports/admin", "/reports/executive"}:
        next_page = "/reports"
    if report_type not in {"daily", "weekly", "monthly"}:
        abort(400, description="report_type must be daily/weekly/monthly")
    if export_format not in {"csv", "pdf"}:
        abort(400, description="export_format must be csv/pdf")
    if delivery not in {"download", "queue"}:
        abort(400, description="delivery must be download/queue")
    if report_view not in {"", "daily", "admin", "executive", "hub"}:
        abort(400, description="report_view must be daily/admin/executive/hub")
    normalized_days = normalize_period_days(report_type, period_days)
    payload = {
        "report_type": report_type,
        "export_format": export_format,
        "period_days": normalized_days,
        "report_view": report_view or None,
        "report_id": None,
    }

    conn = get_web_conn()
    try:
        job_run_id, report_id = create_report_export_request(
            conn,
            report_type=report_type,
            export_format=export_format,
            requested_by=actor,
            period_days=normalized_days,
            payload=payload,
        )
        if delivery == "download":
            try:
                mark_report_running(conn, report_id)
                generated = generate_report_export(
                    conn,
                    report_type=report_type,
                    export_format=export_format,
                    period_days=normalized_days,
                    requested_by=actor,
                    report_id=report_id,
                    job_run_id=job_run_id,
                    report_view=report_view or None,
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
                finish_job_success(conn, job_run_id, {
                    "report_id": report_id,
                    "file_name": generated.file_name,
                    "file_relpath": generated.file_relpath,
                    "delivery": "download",
                })
                add_web_audit_log(
                    conn,
                    actor=actor,
                    action="download_report_export_now",
                    object_type="report_export",
                    object_id=report_id,
                    details={"job_run_id": job_run_id, "report_type": report_type, "export_format": export_format, "period_days": normalized_days, "report_view": report_view or None},
                )
                return send_from_directory(REPORT_EXPORT_DIR, generated.file_name, as_attachment=True, download_name=generated.file_name)
            except Exception as exc:
                finish_report_failed(conn, report_id, str(exc))
                finish_job_failed(conn, job_run_id, str(exc), {"delivery": "download"})
                add_web_audit_log(
                    conn,
                    actor=actor,
                    action="download_report_export_failed",
                    object_type="report_export",
                    object_id=report_id,
                    details={"job_run_id": job_run_id, "error": str(exc)[:500]},
                )
                flash("ออกรายงานไม่สำเร็จ โปรดตรวจสอบ log แล้วลองใหม่", "error")
                return redirect(next_page)

        add_web_audit_log(
            conn,
            actor=actor,
            action="queue_report_export",
            object_type="report_export",
            object_id=report_id,
            details={"job_run_id": job_run_id, "report_type": report_type, "export_format": export_format, "period_days": normalized_days, "report_view": report_view or None},
        )
        flash(f"ส่งงานออกรายงานแล้ว report_id={report_id} job_run_id={job_run_id}", "success")
        return redirect(next_page)
    finally:
        conn.close()


@app.get("/reports/<int:report_id>/download")
@requires_permission("reports.view")
def reports_download(report_id: int):
    conn = get_web_conn()
    try:
        row = get_report_export(conn, report_id)
    finally:
        conn.close()
    if not row:
        abort(404)
    if row.get("status") != "success" or not row.get("file_name"):
        abort(404)
    conn = get_web_conn()
    try:
        _audit(conn, current_actor(), "download_report_export", "report_export", report_id, {"file_name": row.get("file_name")})
    finally:
        conn.close()
    return send_from_directory(REPORT_EXPORT_DIR, row["file_name"], as_attachment=True, download_name=row["file_name"])


@app.get("/alerts")
@requires_permission("alerts.view")
def alerts_api():
    conn = get_web_conn()
    try:
        overview = get_alert_overview(conn)
    finally:
        conn.close()
    if _wants_html_response():
        return render_template("alerts.html", policies=overview.get("policies") or [], targets=overview.get("targets") or [])
    return jsonify({"status": "ok", **overview})


@app.post("/alerts/policies")
@requires_permission("alerts.manage")
@csrf_protect
def alert_policy_create_api():
    payload = request.get_json(silent=True) if request.is_json else None
    if not payload:
        quiet_raw = (request.form.get("quiet_hours") or "").strip()
        quiet_value = None
        if quiet_raw:
            try:
                quiet_value = json.loads(quiet_raw)
            except json.JSONDecodeError:
                quiet_value = {"raw": quiet_raw}
        payload = {"policy_name": request.form.get("policy_name"), "issue_type": request.form.get("issue_type"), "severity": request.form.get("severity"), "channel": request.form.get("channel"), "repeat_minutes": request.form.get("repeat_minutes"), "quiet_hours": quiet_value, "is_enabled": request.form.get("is_enabled") == "1"}
    policy_name = (payload.get("policy_name") or "").strip()
    issue_type = (payload.get("issue_type") or "").strip()
    severity = (payload.get("severity") or "").strip().upper()
    channel = (payload.get("channel") or "telegram").strip()
    repeat_minutes = int(payload.get("repeat_minutes") or 30)
    quiet_hours = payload.get("quiet_hours")
    is_enabled = 1 if payload.get("is_enabled", True) else 0

    if not policy_name or not issue_type or severity not in {"INFO", "WARNING", "CRITICAL"}:
        abort(400, description="policy_name, issue_type and valid severity are required")

    actor = current_actor()
    conn = get_web_conn()
    try:
        policy_id = add_alert_policy(
            conn,
            policy_name=policy_name,
            issue_type=issue_type,
            severity=severity,
            channel=channel,
            repeat_minutes=repeat_minutes,
            quiet_hours=quiet_hours,
            is_enabled=is_enabled,
        )
        add_web_audit_log(
            conn,
            actor=actor,
            action="add_alert_policy",
            object_type="alert_policy",
            object_id=policy_id,
            details={"policy_name": policy_name},
        )
    finally:
        conn.close()
    if _wants_html_response():
        flash(f"สร้าง alert policy สำเร็จ policy_id={policy_id}", "success")
        return redirect(url_for("alerts_api"))
    return jsonify({"status": "ok", "policy_id": policy_id}), 201


@app.post("/alerts/targets")
@requires_permission("alerts.manage")
@csrf_protect
def alert_target_create_api():
    payload = request.get_json(silent=True) if request.is_json else None
    payload = payload or {"policy_id": request.form.get("policy_id"), "target_type": request.form.get("target_type"), "target_value": request.form.get("target_value"), "is_enabled": request.form.get("is_enabled") == "1"}
    policy_id = int(payload.get("policy_id") or 0)
    target_type = (payload.get("target_type") or "telegram_chat_id").strip()
    target_value = (payload.get("target_value") or "").strip()
    is_enabled = 1 if payload.get("is_enabled", True) else 0

    if policy_id <= 0 or not target_value:
        abort(400, description="policy_id and target_value are required")

    actor = current_actor()
    conn = get_web_conn()
    try:
        target_id = add_alert_target(
            conn,
            policy_id=policy_id,
            target_type=target_type,
            target_value=target_value,
            is_enabled=is_enabled,
        )
        add_web_audit_log(
            conn,
            actor=actor,
            action="add_alert_target",
            object_type="alert_target",
            object_id=target_id,
            details={"policy_id": policy_id, "target_type": target_type},
        )
    finally:
        conn.close()
    if _wants_html_response():
        flash(f"สร้าง alert target สำเร็จ target_id={target_id}", "success")
        return redirect(url_for("alerts_api"))
    return jsonify({"status": "ok", "target_id": target_id}), 201
