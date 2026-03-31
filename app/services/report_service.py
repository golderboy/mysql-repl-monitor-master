from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

from app.repositories.incident_repo import get_incident_detail, get_incident_events, get_incident_notes
from app.repositories.monitor_repo import fetch_open_incidents
from app.repositories.report_repo import list_report_daily_rows, list_report_summary_rows

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_EXPORT_DIR = PROJECT_ROOT / "exports" / "reports"


@dataclass(frozen=True)
class GeneratedReport:
    file_name: str
    file_path: Path
    file_relpath: str
    file_size_bytes: int
    sha256_hex: str
    manifest: dict[str, Any]


def ensure_report_export_dir() -> Path:
    REPORT_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    return REPORT_EXPORT_DIR


def _period_label(report_type: str, days: int) -> str:
    if report_type == "daily":
        return f"daily-{days}d"
    if report_type == "weekly":
        return f"weekly-{days}d"
    if report_type == "monthly":
        return f"monthly-{days}d"
    return f"custom-{days}d"


def normalize_period_days(report_type: str, period_days: int | None) -> int:
    if period_days and period_days > 0:
        return period_days
    defaults = {"daily": 1, "weekly": 7, "monthly": 30}
    return defaults.get(report_type, 7)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _english_text(value: Any, *, empty: str = "-") -> str:
    raw = _safe_str(value)
    if not raw:
        return empty
    try:
        cleaned = raw.encode("ascii", "ignore").decode("ascii")
    except Exception:
        cleaned = raw
    cleaned = " ".join(cleaned.split())
    if cleaned:
        return cleaned
    return empty


def _safe_call(default: Any, callback: Callable[[], Any]) -> Any:
    try:
        return callback()
    except Exception:
        return default


def _server_risk_score(row: dict[str, Any]) -> int:
    return (
        (_safe_int(row.get("replication_critical_count")) * 5)
        + (_safe_int(row.get("replication_error_count")) * 4)
        + (_safe_int(row.get("mismatch_count")) * 2)
        + (_safe_int(row.get("incident_opened_count")) * 3)
        + (_safe_int(row.get("replication_warn_count")) * 1)
        - (_safe_int(row.get("incident_recovered_count")) * 1)
    )


def _incident_score(row: dict[str, Any]) -> int:
    severity = _safe_str(row.get("severity")).upper()
    status = _safe_str(row.get("current_status")).upper()
    score = {"CRITICAL": 100, "WARNING": 50, "INFO": 20}.get(severity, 10)
    if status in {"OPEN", "ACKNOWLEDGED", "INVESTIGATING"}:
        score += 40
    if status == "RECOVERED":
        score += 10
    score += min(_safe_int(row.get("occurrence_count")), 20)
    return score


def _overall_status(dataset: dict[str, Any]) -> str:
    totals = dataset["totals"]
    critical = _safe_int(totals.get("replication_critical_count"))
    errors = _safe_int(totals.get("replication_error_count"))
    mismatches = _safe_int(totals.get("mismatch_count"))
    opened = _safe_int(totals.get("incident_opened_count"))
    if critical > 0 or errors >= 3 or opened >= 5:
        return "Urgent attention required"
    if errors > 0 or mismatches > 0 or opened > 0:
        return "Issues require follow-up"
    return "Normal operating range"


def _top_servers(dataset: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    rows = list(dataset.get("summary_rows") or [])
    rows.sort(key=lambda row: (_server_risk_score(row), _safe_str(row.get("server_code"))), reverse=True)
    return rows[:limit]


def _server_brief(row: dict[str, Any]) -> str:
    return (
        f"{_safe_str(row.get('server_code')) or '-'} | risk={_server_risk_score(row)} | "
        f"crit={_safe_int(row.get('replication_critical_count'))} | err={_safe_int(row.get('replication_error_count'))} | "
        f"mismatch={_safe_int(row.get('mismatch_count'))} | opened={_safe_int(row.get('incident_opened_count'))}"
    )


def _report_profile(report_view: str | None, report_type: str) -> str:
    view = _safe_str(report_view).lower()
    if view in {"daily", "admin", "executive", "hub"}:
        return view
    return {"daily": "daily", "weekly": "admin", "monthly": "executive"}.get(report_type, "daily")


def _time_window_label(dataset: dict[str, Any]) -> str:
    daily_rows = dataset.get("daily_rows") or []
    if not daily_rows:
        return f"Last {dataset.get('period_days', 0)} days"
    start_date = _safe_str(daily_rows[0].get("stat_date"))
    end_date = _safe_str(daily_rows[-1].get("stat_date"))
    if start_date and end_date:
        return f"Period {start_date} to {end_date}"
    return f"Last {dataset.get('period_days', 0)} days"


def _profile_title(profile: str) -> str:
    return {
        "daily": "Daily Operations Summary",
        "admin": "IT Operations Review",
        "executive": "Executive Summary",
        "hub": "Monitoring Summary",
    }.get(profile, "Monitoring Summary")


def _profile_subtitle(profile: str) -> str:
    return {
        "daily": "One-page daily status and follow-up.",
        "admin": "One-page IT risk, backlog, and actions.",
        "executive": "One-page status, risk, and actions.",
        "hub": "One-page monitoring overview.",
    }.get(profile, "One-page monitoring overview.")


def _status_short_label(dataset: dict[str, Any]) -> str:
    status = _overall_status(dataset).lower()
    if 'urgent' in status:
        return 'Urgent'
    if 'attention' in status or 'follow' in status:
        return 'Attention'
    return 'Stable'


def _abbreviation_note() -> str:
    return 'Abbrev: OI=open incidents, Crit=critical, Err=error, MM=mismatch.'


def _summary_bullets(dataset: dict[str, Any], profile: str) -> list[str]:
    totals = dataset["totals"]
    top_server = (_top_servers(dataset, 1) or [{}])[0]
    bullets = [
        _time_window_label(dataset).replace('Period ', 'Period: '),
        f"OI {_safe_int(totals.get('incident_opened_count'))} | Crit {_safe_int(totals.get('replication_critical_count'))} | Err {_safe_int(totals.get('replication_error_count'))} | MM {_safe_int(totals.get('mismatch_count'))}",
    ]
    server_code = _safe_str(top_server.get("server_code"))
    if server_code:
        bullets.append(
            f"Top risk: {server_code} | Risk {_server_risk_score(top_server)} | MM {_safe_int(top_server.get('mismatch_count'))} | OI {_safe_int(top_server.get('incident_opened_count'))}"
        )
    important = dataset.get("important_incidents") or []
    if important:
        bullets.append(f"Appendix: {', '.join(_safe_str(item.get('incident_code')) or '-' for item in important[:3])}")
    else:
        bullets.append("Appendix: none")
    if profile == "executive":
        bullets.append("Action: assign an owner to each open incident and fix the top-risk server first.")
    elif profile == "admin":
        bullets.append("Action: clear MM backlog and verify pending evidence/signature checks.")
    else:
        bullets.append("Action: use this page for stand-up and handoff.")
    return bullets[:5]


def _key_metrics(dataset: dict[str, Any]) -> list[tuple[str, str, str]]:
    totals = dataset["totals"]
    top_server = (_top_servers(dataset, 1) or [{}])[0]
    top_server_label = _safe_str(top_server.get("server_code")) or "-"
    return [
        ("System status", _status_short_label(dataset), "Action level"),
        ("Open incidents", str(_safe_int(totals.get("incident_opened_count"))), "Needs follow-up"),
        ("Mismatch", str(_safe_int(totals.get("mismatch_count"))), "MM count"),
        ("Top risk server", top_server_label, "Priority"),
    ]


def _safe_open_incidents(conn, limit: int = 12) -> list[dict[str, Any]]:
    return _safe_call([], lambda: fetch_open_incidents(conn, limit=limit))


def _safe_incident_bundle(conn, incident_id: int) -> dict[str, Any]:
    detail = _safe_call({}, lambda: get_incident_detail(conn, incident_id) or {})
    events = _safe_call([], lambda: get_incident_events(conn, incident_id, limit=8) or [])
    notes = _safe_call([], lambda: get_incident_notes(conn, incident_id, limit=5) or [])
    return {"detail": detail, "events": events, "notes": notes}


def _select_important_incidents(raw_rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    rows = list(raw_rows or [])
    rows.sort(
        key=lambda row: (
            _incident_score(row),
            _safe_str(row.get("last_detected_at")),
            _safe_str(row.get("incident_code")),
        ),
        reverse=True,
    )
    return rows[:limit]


def build_report_dataset(conn, *, report_type: str, period_days: int) -> dict[str, Any]:
    days = normalize_period_days(report_type, period_days)
    daily_rows = list_report_daily_rows(conn, days=days)
    summary_rows = list_report_summary_rows(conn, days=days)
    totals = {
        "replication_ok_count": sum(_safe_int(row.get("replication_ok_count")) for row in summary_rows),
        "replication_warn_count": sum(_safe_int(row.get("replication_warn_count")) for row in summary_rows),
        "replication_critical_count": sum(_safe_int(row.get("replication_critical_count")) for row in summary_rows),
        "replication_error_count": sum(_safe_int(row.get("replication_error_count")) for row in summary_rows),
        "mismatch_count": sum(_safe_int(row.get("mismatch_count")) for row in summary_rows),
        "incident_opened_count": sum(_safe_int(row.get("incident_opened_count")) for row in summary_rows),
        "incident_recovered_count": sum(_safe_int(row.get("incident_recovered_count")) for row in summary_rows),
    }
    raw_incidents = _safe_open_incidents(conn, limit=12)
    important_incidents = _select_important_incidents(raw_incidents, limit=3)
    appendix_incidents: list[dict[str, Any]] = []
    for row in important_incidents:
        bundle = _safe_incident_bundle(conn, _safe_int(row.get("id")))
        merged = dict(row)
        merged.update(bundle.get("detail") or {})
        merged["events"] = bundle.get("events") or []
        merged["notes"] = bundle.get("notes") or []
        appendix_incidents.append(merged)
    return {
        "report_type": report_type,
        "period_days": days,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "summary_rows": summary_rows,
        "daily_rows": daily_rows,
        "totals": totals,
        "important_incidents": appendix_incidents,
        "open_incident_count": len(raw_incidents),
    }


def _csv_rows(dataset: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    rows.append(["section", "report_type", dataset["report_type"]])
    rows.append(["section", "period_days", dataset["period_days"]])
    for key, value in dataset["totals"].items():
        rows.append(["total", key, value])
    rows.append([])
    rows.append([
        "server_code",
        "replication_ok_count",
        "replication_warn_count",
        "replication_critical_count",
        "replication_error_count",
        "mismatch_count",
        "incident_opened_count",
        "incident_recovered_count",
    ])
    for row in dataset["summary_rows"]:
        rows.append([
            row.get("server_code"),
            row.get("replication_ok_count"),
            row.get("replication_warn_count"),
            row.get("replication_critical_count"),
            row.get("replication_error_count"),
            row.get("mismatch_count"),
            row.get("incident_opened_count"),
            row.get("incident_recovered_count"),
        ])
    rows.append([])
    rows.append([
        "stat_date",
        "server_code",
        "replication_ok_count",
        "replication_warn_count",
        "replication_critical_count",
        "replication_error_count",
        "mismatch_count",
        "incident_opened_count",
        "incident_recovered_count",
    ])
    for row in dataset["daily_rows"]:
        rows.append([
            row.get("stat_date"),
            row.get("server_code"),
            row.get("replication_ok_count"),
            row.get("replication_warn_count"),
            row.get("replication_critical_count"),
            row.get("replication_error_count"),
            row.get("mismatch_count"),
            row.get("incident_opened_count"),
            row.get("incident_recovered_count"),
        ])
    rows.append([])
    rows.append(["important_incident_code", "severity", "status", "server_code", "summary"])
    for row in dataset.get("important_incidents") or []:
        rows.append([
            row.get("incident_code"),
            row.get("severity"),
            row.get("current_status"),
            row.get("server_code"),
            row.get("system_summary"),
        ])
    return rows


def _write_csv(file_path: Path, dataset: dict[str, Any]) -> None:
    with file_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerows(_csv_rows(dataset))


# ---------------------------------------------------------------------------
# Rich PDF generation for executive/admin one-page summary + appendix
# ---------------------------------------------------------------------------

def _register_pdf_fonts() -> tuple[str, str]:
    return "Helvetica", "Helvetica-Bold"


def _chart_image_server_risk(dataset: dict[str, Any]) -> BytesIO | None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return None

    rows = _top_servers(dataset, limit=5)
    if not rows:
        return None
    labels = [_safe_str(row.get("server_code")) or "-" for row in rows]
    values = [_server_risk_score(row) for row in rows]

    fig, ax = plt.subplots(figsize=(4.8, 2.3), dpi=160)
    ax.barh(labels, values)
    ax.set_title("Server risk score")
    ax.set_xlabel("score")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


def _chart_image_daily_trend(dataset: dict[str, Any]) -> BytesIO | None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return None

    daily_rows = dataset.get("daily_rows") or []
    if not daily_rows:
        return None

    buckets: dict[str, dict[str, int]] = {}
    for row in daily_rows:
        key = _safe_str(row.get("stat_date")) or "-"
        current = buckets.setdefault(key, {"mismatch": 0, "opened": 0, "recovered": 0, "error": 0})
        current["mismatch"] += _safe_int(row.get("mismatch_count"))
        current["opened"] += _safe_int(row.get("incident_opened_count"))
        current["recovered"] += _safe_int(row.get("incident_recovered_count"))
        current["error"] += _safe_int(row.get("replication_error_count"))

    dates = list(buckets.keys())
    mismatches = [buckets[d]["mismatch"] for d in dates]
    opened = [buckets[d]["opened"] for d in dates]
    recovered = [buckets[d]["recovered"] for d in dates]
    errors = [buckets[d]["error"] for d in dates]

    fig, ax = plt.subplots(figsize=(4.8, 2.3), dpi=160)
    ax.plot(dates, mismatches, marker="o", label="Mismatch")
    ax.plot(dates, opened, marker="o", label="Opened")
    ax.plot(dates, recovered, marker="o", label="Recovered")
    if any(errors):
        ax.plot(dates, errors, marker="o", label="Error")
    ax.set_title("Daily trend")
    ax.tick_params(axis="x", rotation=30)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=7)
    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


def _split_lines(text: str, *, width: int, regular_font: str, font_size: int) -> list[str]:
    from reportlab.lib.utils import simpleSplit
    return simpleSplit(text, regular_font, font_size, width)


def _draw_wrapped_text(c, text: str, x: float, y: float, width: float, leading: float, font_name: str, font_size: int, max_lines: int | None = None) -> float:
    lines = _split_lines(text, width=int(width), regular_font=font_name, font_size=font_size)
    if max_lines is not None:
        lines = lines[:max_lines]
    cursor_y = y
    c.setFont(font_name, font_size)
    for line in lines:
        c.drawString(x, cursor_y, line)
        cursor_y -= leading
    return cursor_y


def _draw_card(c, x: float, y: float, w: float, h: float, title: str, value: str, subtitle: str, *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, 10, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(x, y, w, h, 10, stroke=1, fill=0)
    c.setFillColor(colors.HexColor("#4B648A"))
    c.setFont(regular_font, 8)
    c.drawString(x + 10, y + h - 16, title)
    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 18)
    c.drawString(x + 10, y + h - 38, value)
    c.setFillColor(colors.HexColor("#52667A"))
    c.setFont(regular_font, 8)
    c.drawString(x + 10, y + 10, subtitle[:46])


def _draw_bullets_box(c, title: str, bullets: list[str], x: float, y: float, w: float, h: float, *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(x, y, w, h, 12, stroke=1, fill=0)
    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(x + 12, y + h - 18, title)
    cursor_y = y + h - 36
    for bullet in bullets[:5]:
        cursor_y = _draw_wrapped_text(c, f"- {bullet}", x + 14, cursor_y, w - 26, 12, regular_font, 9, max_lines=3)
        cursor_y -= 4
        if cursor_y < y + 18:
            break


def _draw_incident_table(c, incidents: list[dict[str, Any]], x: float, y: float, w: float, h: float, *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(x, y, w, h, 12, stroke=1, fill=0)
    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(x + 12, y + h - 18, "Important incidents")
    if not incidents:
        c.setFont(regular_font, 9)
        c.drawString(x + 12, y + h - 38, "No major incident in this reporting period")
        return
    cursor_y = y + h - 38
    for row in incidents[:4]:
        code = _safe_str(row.get("incident_code")) or "-"
        server = _safe_str(row.get("server_code")) or "-"
        sev = _safe_str(row.get("severity")).upper() or "-"
        status = _safe_str(row.get("current_status")).upper() or "-"
        summary = _english_text(row.get("system_summary"), empty="No summary")
        c.setFont(bold_font, 9)
        c.drawString(x + 12, cursor_y, f"{code} | {sev} | {server} | {status}")
        cursor_y -= 12
        cursor_y = _draw_wrapped_text(c, summary, x + 12, cursor_y, w - 24, 10, regular_font, 8, max_lines=2)
        cursor_y -= 8
        if cursor_y < y + 18:
            break


def _draw_chart_panel(c, title: str, image_bytes: BytesIO | None, x: float, y: float, w: float, h: float, *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    from reportlab.lib.utils import ImageReader
    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(x, y, w, h, 12, stroke=1, fill=0)
    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(x + 12, y + h - 18, title)
    if image_bytes is None:
        c.setFont(regular_font, 9)
        c.drawString(x + 12, y + h - 40, "Not enough data to draw a chart")
        return
    c.drawImage(ImageReader(image_bytes), x + 10, y + 10, width=w - 20, height=h - 34, preserveAspectRatio=True, mask='auto')


def _draw_header(c, profile: str, dataset: dict[str, Any], requested_by: str, *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    page_w, page_h = 595.2756, 841.8898
    c.setFillColor(colors.HexColor("#173A8A"))
    c.roundRect(28, page_h - 116, page_w - 56, 84, 18, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont(regular_font, 9)
    c.drawString(44, page_h - 58, "MySQL Replication Monitor")
    c.setFont(bold_font, 18)
    c.drawString(44, page_h - 80, _profile_title(profile))
    c.setFont(regular_font, 9)
    c.drawString(44, page_h - 96, _profile_subtitle(profile))
    c.setFillColor(colors.HexColor("#EAF1FF"))
    c.roundRect(page_w - 220, page_h - 92, 160, 28, 10, stroke=0, fill=1)
    c.setFillColor(colors.HexColor("#173A8A"))
    c.setFont(bold_font, 9)
    c.drawString(page_w - 210, page_h - 75, f"Prepared by {requested_by} | {dataset['generated_at'][:10]}")
    c.setFillColor(colors.black)


def _draw_footer(c, *, regular_font: str) -> None:
    from reportlab.lib import colors
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.line(32, 28, 563, 28)
    c.setFillColor(colors.HexColor("#6B7C93"))
    c.setFont(regular_font, 8)
    c.drawString(32, 16, "Generated from monitoring summary tables and incident records for operational decision support")


def _incident_recommendations(incident: dict[str, Any]) -> list[str]:
    severity = _safe_str(incident.get("severity")).upper()
    issue_type = _safe_str(incident.get("issue_type"))
    rows = [f"Assign a clear owner and target time for incident {(_safe_str(incident.get('incident_code')) or '-')}"]
    if "schema" in issue_type or "signature" in issue_type or _safe_int(incident.get("mismatch_count")) > 0:
        rows.append("Validate source and target evidence and confirm that mismatch does not affect production data.")
    if severity == "CRITICAL":
        rows.append("Summarize service impact and prepare rollback or mitigation if the trend does not improve.")
    else:
        rows.append("Track recurrence and open a root-cause task if the same incident repeats.")
    return rows[:3]


def _draw_incident_appendix_page(c, incident: dict[str, Any], *, regular_font: str, bold_font: str) -> None:
    from reportlab.lib import colors
    page_w, page_h = 595.2756, 841.8898
    x = 32
    y_top = page_h - 44

    c.setFont(bold_font, 18)
    c.setFillColor(colors.HexColor("#102A43"))
    c.drawString(x, y_top, f"Appendix: {(_safe_str(incident.get('incident_code')) or 'INCIDENT')}" )
    c.setFont(regular_font, 10)
    c.drawString(x, y_top - 18, f"{_safe_str(incident.get('severity')).upper()} | {_safe_str(incident.get('current_status')).upper()} | server {_safe_str(incident.get('server_code')) or '-'}")

    c.setFillColor(colors.white)
    c.roundRect(32, 660, 531, 92, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(32, 660, 531, 92, 12, stroke=1, fill=0)
    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(44, 734, "Summary")
    c.setFont(regular_font, 10)
    _draw_wrapped_text(c, _english_text(incident.get("system_summary"), empty="No incident summary"), 44, 716, 507, 13, regular_font, 10, max_lines=5)

    c.setFillColor(colors.white)
    c.roundRect(32, 522, 255, 122, 12, stroke=1, fill=1)
    c.roundRect(308, 522, 255, 122, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(32, 522, 255, 122, 12, stroke=1, fill=0)
    c.roundRect(308, 522, 255, 122, 12, stroke=1, fill=0)

    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(44, 624, "Details")
    c.setFont(regular_font, 9)
    info_lines = [
        f"Issue type: {_safe_str(incident.get('issue_type')) or '-'}",
        f"Detected first: {_safe_str(incident.get('first_detected_at')) or '-'}",
        f"Detected last: {_safe_str(incident.get('last_detected_at')) or '-'}",
        f"Owner: {_safe_str(incident.get('owner')) or '-'}",
        f"DB/Table: {(_safe_str(incident.get('db_name')) or '-')}/{(_safe_str(incident.get('table_name')) or '-')}",
    ]
    yy = 606
    for line in info_lines:
        c.drawString(44, yy, line)
        yy -= 16

    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(320, 624, "Recommendations")
    yy = 606
    for line in _incident_recommendations(incident):
        yy = _draw_wrapped_text(c, f"- {line}", 320, yy, 228, 12, regular_font, 9, max_lines=3)
        yy -= 4

    c.setFillColor(colors.white)
    c.roundRect(32, 294, 255, 206, 12, stroke=1, fill=1)
    c.roundRect(308, 294, 255, 206, 12, stroke=1, fill=1)
    c.setStrokeColor(colors.HexColor("#D6DDEA"))
    c.roundRect(32, 294, 255, 206, 12, stroke=1, fill=0)
    c.roundRect(308, 294, 255, 206, 12, stroke=1, fill=0)

    c.setFillColor(colors.HexColor("#102A43"))
    c.setFont(bold_font, 11)
    c.drawString(44, 480, "Latest events")
    c.drawString(320, 480, "Notes")

    events = incident.get("events") or []
    notes = incident.get("notes") or []
    yy = 462
    c.setFont(regular_font, 8)
    if events:
        for row in events[:8]:
            line = f"- {_safe_str(row.get('event_time') or row.get('created_at'))[:19]} | {_safe_str(row.get('event_type')) or '-'} | {_english_text(row.get('message'), empty='-')}"
            yy = _draw_wrapped_text(c, line, 44, yy, 228, 10, regular_font, 8, max_lines=2)
            yy -= 4
            if yy < 312:
                break
    else:
        c.drawString(44, yy, "No logged events")

    yy = 462
    if notes:
        for row in notes[:6]:
            note_type = _safe_str(row.get("note_type")) or "note"
            line = f"- {note_type}: {_english_text(row.get('note_text'), empty='-')}"
            yy = _draw_wrapped_text(c, line, 320, yy, 228, 10, regular_font, 8, max_lines=2)
            yy -= 4
            if yy < 312:
                break
    else:
        c.drawString(320, yy, "No notes")

    _draw_footer(c, regular_font=regular_font)
    c.showPage()


def _write_rich_pdf(file_path: Path, dataset: dict[str, Any], *, report_view: str, requested_by: str) -> None:
    try:
        _write_pillow_pdf_fallback(file_path, dataset, report_view=report_view, requested_by=requested_by)
        return
    except Exception:
        _write_minimal_fallback_pdf(file_path, dataset, report_view=report_view, requested_by=requested_by)
        return

def _ascii_fallback_text(text: str) -> str:
    try:
        cleaned = str(text or "").encode("ascii", "ignore").decode("ascii")
    except Exception:
        cleaned = str(text or "")
    cleaned = cleaned.replace(chr(0), " ")
    cleaned = " ".join(cleaned.split())
    return cleaned or "-"


def _pdf_escape_text(text: str) -> str:
    safe = _ascii_fallback_text(text)
    return safe.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class _SimplePdfBuilder:
    def __init__(self) -> None:
        self._pages: list[str] = []

    def add_page(self, commands: list[str]) -> None:
        self._pages.append("\n".join(commands) + "\n")

    def write(self, file_path: Path) -> None:
        objects: list[bytes] = []

        def add_object(data: bytes) -> int:
            objects.append(data)
            return len(objects)

        catalog_idx = add_object(b"")
        pages_idx = add_object(b"")
        font_regular_idx = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
        font_bold_idx = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")

        page_indices: list[int] = []
        for page_content in self._pages:
            stream_bytes = page_content.encode("latin-1", errors="replace")
            content_idx = add_object(b"<< /Length %d >>\nstream\n" % len(stream_bytes) + stream_bytes + b"endstream")
            page_obj = (
                f"<< /Type /Page /Parent {pages_idx} 0 R /MediaBox [0 0 595 842] "
                f"/Resources << /Font << /F1 {font_regular_idx} 0 R /F2 {font_bold_idx} 0 R >> >> "
                f"/Contents {content_idx} 0 R >>"
            ).encode("ascii")
            page_idx = add_object(page_obj)
            page_indices.append(page_idx)

        kids = " ".join(f"{idx} 0 R" for idx in page_indices)
        objects[pages_idx - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_indices)} >>".encode("ascii")
        objects[catalog_idx - 1] = f"<< /Type /Catalog /Pages {pages_idx} 0 R >>".encode("ascii")

        out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = [0]
        for idx, obj in enumerate(objects, start=1):
            offsets.append(len(out))
            out.extend(f"{idx} 0 obj\n".encode("ascii"))
            out.extend(obj)
            out.extend(b"\nendobj\n")
        xref_offset = len(out)
        out.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
        out.extend(b"0000000000 65535 f \n")
        for offset in offsets[1:]:
            out.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
        out.extend(
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_idx} 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("ascii")
        )
        file_path.write_bytes(bytes(out))


def _pdf_cmd_text(
    x: float,
    y: float,
    text: str,
    *,
    size: int = 10,
    bold: bool = False,
    fill_rgb: tuple[float, float, float] = (0, 0, 0),
) -> str:
    font_name = "F2" if bold else "F1"
    return (
        f"q {fill_rgb[0]:.3f} {fill_rgb[1]:.3f} {fill_rgb[2]:.3f} rg "
        f"BT /{font_name} {size} Tf 1 0 0 1 {x:.2f} {y:.2f} Tm ({_pdf_escape_text(text)}) Tj ET Q"
    )


def _pdf_cmd_line(x1: float, y1: float, x2: float, y2: float, *, width: float = 1.0) -> str:
    return f"{width:.2f} w {x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S"


def _pdf_cmd_rect(x: float, y: float, w: float, h: float, *, fill_rgb: tuple[float, float, float] | None = None, stroke_rgb: tuple[float, float, float] | None = (0.82, 0.86, 0.91), line_width: float = 1.0) -> str:
    parts: list[str] = []
    if stroke_rgb is not None:
        parts.append(f"{stroke_rgb[0]:.3f} {stroke_rgb[1]:.3f} {stroke_rgb[2]:.3f} RG")
    if fill_rgb is not None:
        parts.append(f"{fill_rgb[0]:.3f} {fill_rgb[1]:.3f} {fill_rgb[2]:.3f} rg")
    parts.append(f"{line_width:.2f} w {x:.2f} {y:.2f} {w:.2f} {h:.2f} re")
    if fill_rgb is not None and stroke_rgb is not None:
        parts.append("B")
    elif fill_rgb is not None:
        parts.append("f")
    else:
        parts.append("S")
    return " ".join(parts)


def _pdf_wrapped_lines(text: str, limit: int = 78) -> list[str]:
    words = _ascii_fallback_text(text).split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) <= limit:
            current += " " + word
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _pdf_add_wrapped(commands: list[str], text: str, *, x: float, y: float, size: int = 10, leading: float = 12, bold: bool = False, max_lines: int | None = None) -> float:
    lines = _pdf_wrapped_lines(text, max(20, int(90 - size * 2)))
    if max_lines is not None:
        lines = lines[:max_lines]
    cursor = y
    for line in lines:
        commands.append(_pdf_cmd_text(x, cursor, line, size=size, bold=bold))
        cursor -= leading
    return cursor


def _pdf_draw_metric_cards(commands: list[str], dataset: dict[str, Any]) -> None:
    cards = _key_metrics(dataset)
    x_positions = [32, 171, 310, 449]
    for idx, (title, value, subtitle) in enumerate(cards[:4]):
        x = x_positions[idx]
        commands.append(_pdf_cmd_rect(x, 646, 114, 60, fill_rgb=(1, 1, 1)))
        commands.append(_pdf_cmd_text(x + 8, 690, title, size=8))
        commands.append(_pdf_cmd_text(x + 8, 670, value, size=16, bold=True))
        commands.append(_pdf_cmd_text(x + 8, 654, subtitle[:20], size=7))


def _pdf_draw_server_risk_chart(commands: list[str], dataset: dict[str, Any], *, x: float, y: float, w: float, h: float) -> None:
    commands.append(_pdf_cmd_rect(x, y, w, h, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(x + 8, y + h - 18, "Server risk", size=11, bold=True))
    rows = _top_servers(dataset, limit=4)
    if not rows:
        commands.append(_pdf_cmd_text(x + 8, y + h - 40, "No data", size=9))
        return
    max_score = max([_server_risk_score(r) for r in rows] + [1])
    top = y + h - 40
    bar_area = w - 85
    for idx, row in enumerate(rows):
        cy = top - idx * 28
        label = (_safe_str(row.get("server_code")) or "-")[:10]
        score = _server_risk_score(row)
        bar_w = max(2, (score / max_score) * bar_area)
        commands.append(_pdf_cmd_text(x + 8, cy + 4, label, size=8))
        commands.append(_pdf_cmd_rect(x + 58, cy, bar_w, 10, fill_rgb=(0.18, 0.42, 0.82), stroke_rgb=None))
        commands.append(_pdf_cmd_text(x + 62 + bar_w, cy + 2, str(score), size=7))


def _pdf_draw_daily_trend_chart(commands: list[str], dataset: dict[str, Any], *, x: float, y: float, w: float, h: float) -> None:
    commands.append(_pdf_cmd_rect(x, y, w, h, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(x + 8, y + h - 18, "Daily mismatch/opened", size=11, bold=True))
    daily_rows = dataset.get("daily_rows") or []
    if not daily_rows:
        commands.append(_pdf_cmd_text(x + 8, y + h - 40, "No data", size=9))
        return
    buckets: dict[str, dict[str, int]] = {}
    for row in daily_rows:
        key = _safe_str(row.get("stat_date")) or "-"
        current = buckets.setdefault(key, {"mismatch": 0, "opened": 0})
        current["mismatch"] += _safe_int(row.get("mismatch_count"))
        current["opened"] += _safe_int(row.get("incident_opened_count"))
    dates = list(buckets.keys())[-6:]
    max_val = max([buckets[d]["mismatch"] for d in dates] + [buckets[d]["opened"] for d in dates] + [1])
    plot_x = x + 14
    plot_y = y + 24
    plot_w = w - 28
    plot_h = h - 54
    commands.append(_pdf_cmd_line(plot_x, plot_y, plot_x, plot_y + plot_h, width=0.8))
    commands.append(_pdf_cmd_line(plot_x, plot_y, plot_x + plot_w, plot_y, width=0.8))
    gap = plot_w / max(len(dates), 1)
    for idx, d in enumerate(dates):
        bx = plot_x + idx * gap + 8
        mismatch_h = (buckets[d]["mismatch"] / max_val) * (plot_h - 18)
        opened_h = (buckets[d]["opened"] / max_val) * (plot_h - 18)
        commands.append(_pdf_cmd_rect(bx, plot_y, 12, mismatch_h, fill_rgb=(0.91, 0.49, 0.13), stroke_rgb=None))
        commands.append(_pdf_cmd_rect(bx + 14, plot_y, 12, opened_h, fill_rgb=(0.18, 0.42, 0.82), stroke_rgb=None))
        commands.append(_pdf_cmd_text(bx - 2, plot_y - 12, d[-5:], size=6))


def _pdf_draw_one_page_summary(dataset: dict[str, Any], report_view: str, requested_by: str) -> list[str]:
    commands: list[str] = []
    commands.append(_pdf_cmd_rect(24, 746, 547, 72, fill_rgb=(0.09, 0.23, 0.54), stroke_rgb=None))
    commands.append("1 1 1 rg")
    commands.append(_pdf_cmd_text(40, 798, "MySQL Replication Monitor", size=10, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 776, _profile_title(report_view), size=18, bold=True, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 760, _ascii_fallback_text(_profile_subtitle(report_view)), size=9, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(395, 796, f"By {requested_by}", size=9, bold=True, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(395, 782, f"Date {dataset.get('generated_at', '')[:10]}", size=9, fill_rgb=(1, 1, 1)))
    commands.append("0 0 0 rg")

    _pdf_draw_metric_cards(commands, dataset)

    commands.append(_pdf_cmd_rect(32, 492, 280, 130, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 604, "Key points", size=11, bold=True))
    cy = 586
    for bullet in _summary_bullets(dataset, report_view)[:5]:
        cy = _pdf_add_wrapped(commands, f"- {bullet}", x=40, y=cy, size=8, leading=11, max_lines=2)
        cy -= 4

    commands.append(_pdf_cmd_rect(328, 492, 235, 130, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(336, 604, "Important incidents", size=11, bold=True))
    cy = 586
    incidents = dataset.get("important_incidents") or []
    if incidents:
        for row in incidents[:3]:
            line = f"{_safe_str(row.get('incident_code')) or '-'} | {_safe_str(row.get('severity')).upper() or '-'} | {_safe_str(row.get('server_code')) or '-'}"
            commands.append(_pdf_cmd_text(336, cy, line[:38], size=8, bold=True))
            cy -= 12
            cy = _pdf_add_wrapped(commands, _english_text(row.get('system_summary'), empty='No summary'), x=336, y=cy, size=7, leading=10, max_lines=2)
            cy -= 6
    else:
        commands.append(_pdf_cmd_text(336, cy, "No major incident in this period", size=8))

    _pdf_draw_server_risk_chart(commands, dataset, x=32, y=250, w=255, h=210)
    _pdf_draw_daily_trend_chart(commands, dataset, x=308, y=250, w=255, h=210)

    commands.append(_pdf_cmd_rect(32, 96, 531, 126, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 204, "Action recommendations", size=11, bold=True))
    cy = 186
    for line in _summary_bullets(dataset, report_view)[-2:]:
        cy = _pdf_add_wrapped(commands, f"- {line}", x=40, y=cy, size=8, leading=11, max_lines=3)
        cy -= 6
    commands.append(_pdf_cmd_line(32, 40, 563, 40, width=0.8))
    commands.append(_pdf_cmd_text(32, 24, "Generated from monitoring summary tables and incident records", size=8))
    return commands


def _pdf_draw_incident_appendix(incident: dict[str, Any]) -> list[str]:
    commands: list[str] = []
    commands.append(_pdf_cmd_text(32, 804, f"Incident appendix: {_safe_str(incident.get('incident_code')) or '-'}", size=18, bold=True))
    commands.append(_pdf_cmd_text(32, 786, f"Severity {_safe_str(incident.get('severity')).upper() or '-'} | Status {_safe_str(incident.get('current_status')).upper() or '-'} | Server {_safe_str(incident.get('server_code')) or '-'}", size=10))

    commands.append(_pdf_cmd_rect(32, 650, 531, 104, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 736, "Summary", size=11, bold=True))
    _pdf_add_wrapped(commands, _english_text(incident.get('system_summary'), empty='No incident summary'), x=40, y=718, size=9, leading=12, max_lines=5)

    commands.append(_pdf_cmd_rect(32, 520, 255, 108, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 610, "Details", size=11, bold=True))
    info_lines = [
        f"Issue type: {_safe_str(incident.get('issue_type')) or '-'}",
        f"Detected first: {_safe_str(incident.get('first_detected_at')) or '-'}",
        f"Detected last: {_safe_str(incident.get('last_detected_at')) or '-'}",
        f"Owner: {_safe_str(incident.get('owner')) or '-'}",
        f"DB/Table: {(_safe_str(incident.get('db_name')) or '-')}/{(_safe_str(incident.get('table_name')) or '-')}",
    ]
    y = 592
    for line in info_lines:
        commands.append(_pdf_cmd_text(40, y, line[:46], size=8))
        y -= 14

    commands.append(_pdf_cmd_rect(308, 520, 255, 108, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(316, 610, "Recommendations", size=11, bold=True))
    y = 592
    for line in _incident_recommendations(incident):
        y = _pdf_add_wrapped(commands, f"- {line}", x=316, y=y, size=8, leading=11, max_lines=2)
        y -= 4

    commands.append(_pdf_cmd_rect(32, 250, 255, 246, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(40, 478, "Latest events", size=11, bold=True))
    y = 460
    events = incident.get('events') or []
    if events:
        for row in events[:10]:
            line = f"- {_safe_str(row.get('event_time') or row.get('created_at'))[:19]} | {_safe_str(row.get('event_type')) or '-'} | {_english_text(row.get('message'), empty='-')}"
            y = _pdf_add_wrapped(commands, line, x=40, y=y, size=7, leading=9, max_lines=2)
            y -= 3
            if y < 268:
                break
    else:
        commands.append(_pdf_cmd_text(40, y, "No events logged", size=8))

    commands.append(_pdf_cmd_rect(308, 250, 255, 246, fill_rgb=(1, 1, 1)))
    commands.append(_pdf_cmd_text(316, 478, "Notes", size=11, bold=True))
    y = 460
    notes = incident.get('notes') or []
    if notes:
        for row in notes[:8]:
            line = f"- {_safe_str(row.get('note_type')) or 'note'}: {_english_text(row.get('note_text'), empty='-')}"
            y = _pdf_add_wrapped(commands, line, x=316, y=y, size=7, leading=9, max_lines=2)
            y -= 3
            if y < 268:
                break
    else:
        commands.append(_pdf_cmd_text(316, y, "No notes", size=8))

    commands.append(_pdf_cmd_line(32, 40, 563, 40, width=0.8))
    commands.append(_pdf_cmd_text(32, 24, "Appendix generated from incident records", size=8))
    return commands


def _write_pillow_pdf_fallback(file_path: Path, dataset: dict[str, Any], *, report_view: str, requested_by: str) -> None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        _write_minimal_fallback_pdf(file_path, dataset, report_view=report_view, requested_by=requested_by)
        return

    font_candidates = [
        Path('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'),
        Path('/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf'),
    ]
    font_bold_candidates = [
        Path('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'),
        Path('/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf'),
    ]

    def load_font(candidates: list[Path], size: int):
        for candidate in candidates:
            if candidate.exists():
                try:
                    return ImageFont.truetype(str(candidate), size=size)
                except Exception:
                    pass
        return ImageFont.load_default()

    W, H = 1240, 1754
    font_regular = load_font(font_candidates, 22)
    font_small = load_font(font_candidates, 16)
    font_tiny = load_font(font_candidates, 14)
    font_bold = load_font(font_bold_candidates, 30)
    font_card = load_font(font_bold_candidates, 24)
    font_card_big = load_font(font_bold_candidates, 28)
    font_section = load_font(font_bold_candidates, 18)

    colors = {
        'ink': (16, 42, 67),
        'muted': (82, 102, 122),
        'soft': (75, 100, 138),
        'line': (214, 221, 234),
        'brand': (23, 58, 138),
        'brand_soft': (234, 241, 255),
        'blue': (46, 107, 209),
        'orange': (232, 124, 34),
        'green': (16, 185, 129),
        'red': (220, 53, 69),
        'amber': (245, 158, 11),
        'white': (255, 255, 255),
    }

    def draw_wrapped(draw, text: str, x: int, y: int, font, width: int, fill=colors['ink'], max_lines: int | None = None, line_gap: int = 5) -> int:
        words = str(text or '').split()
        if not words:
            return y
        lines: list[str] = []
        current = words[0]
        for word in words[1:]:
            trial = current + ' ' + word
            if draw.textlength(trial, font=font) <= width:
                current = trial
            else:
                lines.append(current)
                current = word
        lines.append(current)
        if max_lines is not None and len(lines) > max_lines:
            lines = lines[:max_lines]
            if not lines[-1].endswith('...'):
                while draw.textlength(lines[-1] + '...', font=font) > width and len(lines[-1]) > 4:
                    lines[-1] = lines[-1][:-1]
                lines[-1] += '...'
        line_h = int(getattr(font, 'size', 16) * 1.2)
        for line in lines:
            draw.text((x, y), line, font=font, fill=fill)
            y += line_h + line_gap
        return y

    def fit_font_for_text(draw, text: str, candidates: list, width: int):
        for font in candidates:
            if draw.textlength(str(text or ''), font=font) <= width:
                return font
        return candidates[-1]

    def status_fill(value: str):
        lower = (value or '').lower()
        if 'urgent' in lower:
            return colors['red']
        if 'issue' in lower or 'follow-up' in lower:
            return colors['amber']
        return colors['green']

    def risk_band(score: int) -> str:
        if score >= 200:
            return 'High'
        if score >= 100:
            return 'Medium'
        return 'Low'

    def metric_card(draw, x: int, y: int, w: int, h: int, title: str, value: str, subtitle: str):
        draw.rounded_rectangle((x, y, x + w, y + h), radius=18, outline=colors['line'], width=2, fill=colors['white'])
        draw.text((x + 18, y + 18), title, font=font_small, fill=colors['soft'])
        if title == 'System status':
            badge_text = value
            fill = status_fill(value)
            badge_top = y + 50
            badge_bottom = y + 118
            draw.rounded_rectangle((x + 18, badge_top, x + w - 18, badge_bottom), radius=16, fill=fill)
            draw_wrapped(draw, badge_text, x + 28, badge_top + 10, font_section, w - 56, fill=colors['white'], max_lines=2, line_gap=0)
            draw.text((x + 18, y + h - 24), subtitle, font=font_tiny, fill=colors['muted'])
            return
        value_font = fit_font_for_text(draw, value, [font_card_big, font_card, font_section], w - 36)
        draw.text((x + 18, y + 54), value, font=value_font, fill=colors['ink'])
        draw_wrapped(draw, subtitle, x + 18, y + h - 46, font_tiny, w - 36, fill=colors['muted'], max_lines=2, line_gap=2)

    pages: list[Image.Image] = []

    img = Image.new('RGB', (W, H), 'white')
    d = ImageDraw.Draw(img)
    d.rounded_rectangle((40, 40, W - 40, 180), radius=24, fill=colors['brand'])
    d.text((70, 70), 'MySQL Replication Monitor', font=font_small, fill='white')
    d.text((70, 100), _profile_title(report_view), font=font_bold, fill='white')
    d.text((70, 144), _profile_subtitle(report_view), font=font_small, fill='white')
    d.text((W - 350, 92), f'By {requested_by}', font=font_small, fill=colors['brand_soft'])
    d.text((W - 350, 124), f'Date {dataset.get("generated_at", "")[:10]}', font=font_small, fill=colors['brand_soft'])

    cards = _key_metrics(dataset)
    x_positions = [50, 350, 650, 950]
    for idx, (title, value, subtitle) in enumerate(cards[:4]):
        metric_card(d, x_positions[idx], 228, 240, 132, title, value, subtitle)

    # Summary boxes
    left_x, right_x = 50, 650
    top_y = 390
    box_w = 560
    box_h = 340
    d.rounded_rectangle((left_x, top_y, left_x + box_w, top_y + box_h), radius=18, outline=colors['line'], width=2, fill='white')
    d.text((left_x + 24, top_y + 24), 'Key points', font=font_section, fill=colors['ink'])
    yy = top_y + 72
    for bullet in _summary_bullets(dataset, report_view)[:5]:
        yy = draw_wrapped(d, '- ' + bullet, left_x + 30, yy, font_small, box_w - 56, fill=colors['ink'], max_lines=2, line_gap=2) + 8

    d.rounded_rectangle((right_x, top_y, right_x + box_w, top_y + box_h), radius=18, outline=colors['line'], width=2, fill='white')
    d.text((right_x + 24, top_y + 24), 'Important incidents', font=font_section, fill=colors['ink'])
    yy = top_y + 72
    incidents = dataset.get('important_incidents') or []
    if incidents:
        for row in incidents[:3]:
            d.text((right_x + 24, yy), f"{_safe_str(row.get('incident_code')) or '-'} | {_safe_str(row.get('severity')).upper() or '-'} | {_safe_str(row.get('server_code')) or '-'}", font=font_small, fill=colors['ink'])
            yy += 24
            yy = draw_wrapped(d, _english_text(row.get('system_summary'), empty='No summary'), right_x + 30, yy, font_tiny, box_w - 56, fill=colors['muted'], max_lines=3, line_gap=2) + 10
            if yy > top_y + box_h - 50:
                break
    else:
        draw_wrapped(d, 'No major incident in this period', right_x + 24, yy, font_small, box_w - 48, fill=colors['ink'], max_lines=2)

    # Charts area
    chart_y = 770
    chart_h = 430
    left_chart_x = 50
    right_chart_x = 650
    chart_w = 560

    # Server risk chart
    d.rounded_rectangle((left_chart_x, chart_y, left_chart_x + chart_w, chart_y + chart_h), radius=18, outline=colors['line'], width=2, fill='white')
    d.text((left_chart_x + 24, chart_y + 24), 'Server risk', font=font_section, fill=colors['ink'])
    d.text((left_chart_x + 24, chart_y + 52), 'Risk score with level', font=font_tiny, fill=colors['muted'])
    rows = _top_servers(dataset, limit=4)
    if rows:
        max_score = max([_server_risk_score(r) for r in rows] + [1])
        for idx, row in enumerate(rows):
            y0 = chart_y + 110 + idx * 82
            label = _safe_str(row.get('server_code')) or '-'
            score = _server_risk_score(row)
            band = risk_band(score)
            d.text((left_chart_x + 24, y0), label, font=font_small, fill=colors['ink'])
            band_fill = colors['red'] if band == 'High' else colors['amber'] if band == 'Medium' else colors['green']
            d.rounded_rectangle((left_chart_x + 120, y0 + 4, left_chart_x + 120 + 76, y0 + 28), radius=10, fill=band_fill)
            d.text((left_chart_x + 135, y0 + 6), band, font=font_tiny, fill=colors['white'])
            bar_w = int((score / max_score) * 240)
            d.rounded_rectangle((left_chart_x + 220, y0 + 2, left_chart_x + 220 + max(bar_w, 8), y0 + 30), radius=8, fill=colors['blue'])
            d.text((left_chart_x + 230 + max(bar_w, 8), y0), f'{score}', font=font_small, fill=colors['ink'])

    # Daily trend chart
    d.rounded_rectangle((right_chart_x, chart_y, right_chart_x + chart_w, chart_y + chart_h), radius=18, outline=colors['line'], width=2, fill='white')
    d.text((right_chart_x + 24, chart_y + 24), 'Daily MM / OI', font=font_section, fill=colors['ink'])
    d.text((right_chart_x + 24, chart_y + 52), 'Orange=MM, Blue=OI', font=font_tiny, fill=colors['muted'])
    daily_rows = dataset.get('daily_rows') or []
    if daily_rows:
        buckets: dict[str, dict[str, int]] = {}
        for row in daily_rows:
            key = _safe_str(row.get('stat_date')) or '-'
            current = buckets.setdefault(key, {'mismatch': 0, 'opened': 0})
            current['mismatch'] += _safe_int(row.get('mismatch_count'))
            current['opened'] += _safe_int(row.get('incident_opened_count'))
        dates = list(buckets.keys())[-6:]
        max_val = max([buckets[d]['mismatch'] for d in dates] + [buckets[d]['opened'] for d in dates] + [1])
        plot_x0, plot_y0 = right_chart_x + 36, chart_y + 360
        plot_w, plot_h = 460, 240
        d.line((plot_x0, plot_y0, plot_x0, plot_y0 - plot_h), fill=(120, 130, 140), width=2)
        d.line((plot_x0, plot_y0, plot_x0 + plot_w, plot_y0), fill=(120, 130, 140), width=2)
        gap = plot_w / max(len(dates), 1)
        for idx, day in enumerate(dates):
            bx = int(plot_x0 + idx * gap + 20)
            mismatch_h = int((buckets[day]['mismatch'] / max_val) * (plot_h - 30))
            opened_h = int((buckets[day]['opened'] / max_val) * (plot_h - 30))
            d.rounded_rectangle((bx, plot_y0 - mismatch_h, bx + 28, plot_y0), radius=5, fill=colors['orange'])
            d.rounded_rectangle((bx + 34, plot_y0 - opened_h, bx + 62, plot_y0), radius=5, fill=colors['blue'])
            d.text((bx - 8, plot_y0 + 12), day[-5:], font=font_tiny, fill=colors['ink'])

    # Recommendations footer box
    foot_y = 1230
    d.rounded_rectangle((50, foot_y, 1180, 1600), radius=18, outline=colors['line'], width=2, fill='white')
    d.text((74, foot_y + 24), 'Action recommendations', font=font_section, fill=colors['ink'])
    yy = foot_y + 72
    recommendations = _summary_bullets(dataset, report_view)[-2:]
    for line in recommendations:
        yy = draw_wrapped(d, '- ' + line, 80, yy, font_small, 1060, fill=colors['ink'], max_lines=2, line_gap=2) + 8
    yy = draw_wrapped(d, _abbreviation_note(), 80, max(yy + 8, foot_y + 118), font_tiny, 1060, fill=colors['muted'], max_lines=2, line_gap=2)

    d.line((60, 1670, 1180, 1670), fill=colors['line'], width=2)
    d.text((60, 1688), 'Generated from monitoring summary tables and incident records', font=font_tiny, fill=(107,124,147))
    pages.append(img)

    for incident in incidents[:3]:
        img = Image.new('RGB', (W, H), 'white')
        d = ImageDraw.Draw(img)
        d.text((60, 60), f"Incident appendix: {_safe_str(incident.get('incident_code')) or '-'}", font=font_bold, fill=colors['ink'])
        d.text((60, 110), f"{_safe_str(incident.get('severity')).upper()} | {_safe_str(incident.get('current_status')).upper()} | server {_safe_str(incident.get('server_code')) or '-'}", font=font_small, fill=colors['soft'])
        d.rounded_rectangle((60, 160, 1180, 380), radius=18, outline=colors['line'], width=2, fill='white')
        d.text((84, 186), 'Summary', font=font_section, fill=colors['ink'])
        draw_wrapped(d, _english_text(incident.get('system_summary'), empty='No incident summary'), 90, 236, font_small, 1030, fill=colors['ink'], max_lines=6)
        d.rounded_rectangle((60, 420, 540, 700), radius=18, outline=colors['line'], width=2, fill='white')
        d.text((84, 446), 'Details', font=font_section, fill=colors['ink'])
        yy = 494
        for line in [
            f"Issue type: {_safe_str(incident.get('issue_type')) or '-'}",
            f"Detected first: {_safe_str(incident.get('first_detected_at')) or '-'}",
            f"Detected last: {_safe_str(incident.get('last_detected_at')) or '-'}",
            f"Owner: {_safe_str(incident.get('owner')) or '-'}",
            f"DB/Table: {(_safe_str(incident.get('db_name')) or '-')}/{(_safe_str(incident.get('table_name')) or '-')}",
        ]:
            yy = draw_wrapped(d, line, 90, yy, font_small, 400, fill=colors['ink'], max_lines=2) + 6
        d.rounded_rectangle((580, 420, 1180, 700), radius=18, outline=colors['line'], width=2, fill='white')
        d.text((604, 446), 'Recommendations', font=font_section, fill=colors['ink'])
        yy = 494
        for line in _incident_recommendations(incident):
            yy = draw_wrapped(d, '- ' + line, 610, yy, font_small, 520, fill=colors['ink'], max_lines=3) + 8
        d.rounded_rectangle((60, 740, 540, 1600), radius=18, outline=colors['line'], width=2, fill='white')
        d.text((84, 766), 'Latest events', font=font_section, fill=colors['ink'])
        yy = 814
        events = incident.get('events') or []
        if events:
            for row in events[:12]:
                line = f"- {_safe_str(row.get('event_time') or row.get('created_at'))[:19]} | {_safe_str(row.get('event_type')) or '-'} | {_english_text(row.get('message'), empty='-')}"
                yy = draw_wrapped(d, line, 90, yy, font_tiny, 410, fill=colors['ink'], max_lines=2) + 6
                if yy > 1540:
                    break
        else:
            d.text((90, yy), 'No events logged', font=font_small, fill=colors['ink'])
        d.rounded_rectangle((580, 740, 1180, 1600), radius=18, outline=colors['line'], width=2, fill='white')
        d.text((604, 766), 'Notes', font=font_section, fill=colors['ink'])
        yy = 814
        notes = incident.get('notes') or []
        if notes:
            for row in notes[:10]:
                line = f"- {_safe_str(row.get('note_type')) or 'note'}: {_english_text(row.get('note_text'), empty='-')}"
                yy = draw_wrapped(d, line, 610, yy, font_tiny, 500, fill=colors['ink'], max_lines=2) + 6
                if yy > 1540:
                    break
        else:
            d.text((610, yy), 'No notes', font=font_small, fill=colors['ink'])
        pages.append(img)

    first, rest = pages[0], pages[1:]
    first.save(file_path, 'PDF', resolution=150.0, save_all=True, append_images=rest)

def _write_minimal_fallback_pdf(file_path: Path, dataset: dict[str, Any], *, report_view: str, requested_by: str) -> None:
    builder = _SimplePdfBuilder()
    builder.add_page(_pdf_draw_one_page_summary(dataset, report_view, requested_by))
    for incident in (dataset.get('important_incidents') or [])[:3]:
        builder.add_page(_pdf_draw_incident_appendix(incident))
    builder.write(file_path)


def _sha256_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def generate_report_export(
    conn,
    *,
    report_type: str,
    export_format: str,
    period_days: int,
    requested_by: str,
    report_id: int,
    job_run_id: int,
    report_view: str | None = None,
) -> GeneratedReport:
    export_format = export_format.lower().strip()
    if export_format not in {"csv", "pdf"}:
        raise ValueError("export_format must be csv or pdf")

    days = normalize_period_days(report_type, period_days)
    dataset = build_report_dataset(conn, report_type=report_type, period_days=days)
    export_dir = ensure_report_export_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = ".csv" if export_format == "csv" else ".pdf"
    profile = _report_profile(report_view, report_type)
    view_suffix = "" if profile in {"daily", "hub"} else f"-{profile}"
    file_name = f"report-{report_type}{view_suffix}-{_period_label(report_type, days)}-{timestamp}-{report_id}{suffix}"
    file_path = export_dir / file_name

    if export_format == "csv":
        _write_csv(file_path, dataset)
    else:
        _write_rich_pdf(file_path, dataset, report_view=profile, requested_by=requested_by)

    sha256_hex = _sha256_file(file_path)
    file_size_bytes = file_path.stat().st_size
    try:
        file_relpath = str(file_path.relative_to(PROJECT_ROOT))
    except ValueError:
        file_relpath = str(file_path)

    manifest = {
        "report_id": report_id,
        "job_run_id": job_run_id,
        "report_type": report_type,
        "report_view": profile,
        "export_format": export_format,
        "period_days": days,
        "requested_by": requested_by,
        "generated_at": dataset["generated_at"],
        "file_name": file_name,
        "file_relpath": file_relpath,
        "file_size_bytes": file_size_bytes,
        "sha256_hex": sha256_hex,
        "row_counts": {"summary_rows": len(dataset["summary_rows"]), "daily_rows": len(dataset["daily_rows"]), "important_incidents": len(dataset.get("important_incidents") or [])},
        "totals": dataset["totals"],
    }
    manifest_path = export_dir / f"{file_name}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return GeneratedReport(
        file_name=file_name,
        file_path=file_path,
        file_relpath=file_relpath,
        file_size_bytes=file_size_bytes,
        sha256_hex=sha256_hex,
        manifest=manifest,
    )


def _write_matplotlib_pdf_fallback(file_path: Path, dataset: dict[str, Any], *, report_view: str, requested_by: str) -> None:
    _write_minimal_fallback_pdf(file_path, dataset, report_view=report_view, requested_by=requested_by)
