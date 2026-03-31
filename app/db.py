from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict

import pymysql
from pymysql.cursors import DictCursor

from app.config import DbConfig, settings


def _connect(cfg: DbConfig):
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.name,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
        connect_timeout=cfg.connect_timeout,
        read_timeout=cfg.read_timeout,
        write_timeout=cfg.read_timeout,
    )


def get_monitor_conn():
    return _connect(settings.monitor_db)


def get_web_conn():
    return _connect(settings.web_db)


def get_prod_conn(server: Dict[str, Any]):
    return pymysql.connect(
        host=server["host"],
        port=int(server["port"]),
        user=server["username"],
        password=server["password_plain"],
        database=server["db_name"],
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=True,
        connect_timeout=settings.prod_connect_timeout,
        read_timeout=settings.prod_read_timeout,
        write_timeout=settings.prod_read_timeout,
    )


@contextmanager
def cursor(conn):
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
