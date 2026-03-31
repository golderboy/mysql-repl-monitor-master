from __future__ import annotations

import json
from typing import Any, Dict

from app.db import cursor


DEFAULT_SQL = {
    "ovst": '''
        SELECT
          'ovst' AS table_name,
          COUNT(*) AS row_count,
          (SELECT vn FROM ovst WHERE vn <> '' ORDER BY vn DESC LIMIT 1) AS last_vn,
          (SELECT vn FROM ovst WHERE vn <> '' AND vstdate = CURDATE() ORDER BY vn DESC LIMIT 1) AS last_vn_today,
          (SELECT hn FROM ovst WHERE hn < 900000 ORDER BY hn DESC LIMIT 1) AS last_hn,
          (SELECT an FROM ovst WHERE an <> '' ORDER BY an DESC LIMIT 1) AS last_an
        FROM ovst
    ''',
    "patient": '''
        SELECT
          'patient' AS table_name,
          COUNT(*) AS row_count,
          (SELECT hn FROM patient WHERE hn LIKE '00%' ORDER BY hn DESC LIMIT 1) AS last_patient_hn
        FROM patient
    ''',
    "person": '''
        SELECT
          'person' AS table_name,
          COUNT(*) AS row_count,
          (SELECT person_id FROM person ORDER BY person_id DESC LIMIT 1) AS last_person_id
        FROM person
    ''',
    "vn_stat": "SELECT 'vn_stat' AS table_name, COUNT(*) AS row_count FROM vn_stat",
    "an_stat": "SELECT 'an_stat' AS table_name, COUNT(*) AS row_count FROM an_stat",
    "opitemrece": '''
        SELECT
          'opitemrece' AS table_name,
          COUNT(*) AS row_count,
          SUM(vstdate = CURDATE()) AS count_today,
          (SELECT vn FROM opitemrece WHERE vn <> '' ORDER BY vn DESC LIMIT 1) AS last_vn
        FROM opitemrece
    ''',
    "ipt": '''
        SELECT
          'ipt' AS table_name,
          COUNT(*) AS row_count,
          SUM(regdate = CURDATE()) AS admit_today,
          SUM(dchdate = CURDATE()) AS discharge_today,
          (SELECT an FROM ipt WHERE an <> '' ORDER BY an DESC LIMIT 1) AS last_an
        FROM ipt
    ''',
    "lab_head": '''
        SELECT
          'lab_head' AS table_name,
          COUNT(*) AS row_count,
          SUM(order_date = CURDATE()) AS count_today,
          MAX(lab_order_number) AS last_lab_order_number
        FROM lab_head
    ''',
    "lab_order": '''
        SELECT
          'lab_order' AS table_name,
          COUNT(*) AS row_count,
          MAX(lab_order_number) AS last_lab_order_number,
          MAX(update_datetime) AS last_update_datetime
        FROM lab_order
    ''',
}


def build_signature(conn, watch_item: Dict[str, Any]) -> Dict[str, Any]:
    table_name = watch_item["table_name"]
    override_sql = watch_item.get("signature_sql_override")
    sql = override_sql or DEFAULT_SQL.get(table_name)
    if not sql:
        raise ValueError(f"No signature SQL for table: {table_name}")

    with cursor(conn) as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}

    row["table"] = table_name
    return row


def compare_signature(master_signature: Dict[str, Any], slave_signature: Dict[str, Any]) -> tuple[str, str | None]:
    if master_signature == slave_signature:
        return "match", None

    keys = sorted(set(master_signature.keys()) | set(slave_signature.keys()))
    diffs = []
    for key in keys:
        if master_signature.get(key) != slave_signature.get(key):
            diffs.append(
                f"{key}: master={master_signature.get(key)!r}, slave={slave_signature.get(key)!r}"
            )
    return "mismatch", "; ".join(diffs[:10])
