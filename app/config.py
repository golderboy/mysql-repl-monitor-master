from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


def _as_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    connect_timeout: int = 8
    read_timeout: int = 30


@dataclass(frozen=True)
class AppConfig:
    env: str = os.getenv("APP_ENV", "development")
    debug: bool = _as_bool("APP_DEBUG", True)
    secret_key: str = os.getenv("APP_SECRET_KEY", "change-me")
    timezone: str = os.getenv("TZ", "Asia/Bangkok")
    monitor_db: DbConfig = DbConfig(
        host=os.getenv("MONITOR_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("MONITOR_DB_PORT", "3307")),
        name=os.getenv("MONITOR_DB_NAME", "db_monitor"),
        user=os.getenv("MONITOR_DB_USER", "mon_app"),
        password=os.getenv("MONITOR_DB_PASSWORD", ""),
        connect_timeout=int(os.getenv("PROD_DB_CONNECT_TIMEOUT", "8")),
        read_timeout=int(os.getenv("PROD_DB_READ_TIMEOUT", "30")),
    )
    web_db: DbConfig = DbConfig(
        host=os.getenv("WEB_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("WEB_DB_PORT", "3307")),
        name=os.getenv("WEB_DB_NAME", "db_monitor"),
        user=os.getenv("WEB_DB_USER", "mon_web_admin"),
        password=os.getenv("WEB_DB_PASSWORD", ""),
    )
    prod_connect_timeout: int = int(os.getenv("PROD_DB_CONNECT_TIMEOUT", "8"))
    prod_read_timeout: int = int(os.getenv("PROD_DB_READ_TIMEOUT", "30"))
    lag_warning_sec: int = int(os.getenv("LAG_WARNING_SEC", "60"))
    lag_critical_sec: int = int(os.getenv("LAG_CRITICAL_SEC", "300"))
    recover_after_consecutive_ok: int = int(os.getenv("RECOVER_AFTER_CONSECUTIVE_OK", "2"))
    signature_consecutive_mismatch_to_open: int = int(
        os.getenv("SIGNATURE_CONSECUTIVE_MISMATCH_TO_OPEN", "2")
    )
    telegram_enabled: bool = _as_bool("TELEGRAM_ENABLED", False)
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "change-me")
    session_idle_minutes: int = int(os.getenv("SESSION_IDLE_MINUTES", "30"))
    session_absolute_minutes: int = int(os.getenv("SESSION_ABSOLUTE_MINUTES", "720"))
    login_lockout_threshold: int = int(os.getenv("LOGIN_LOCKOUT_THRESHOLD", "5"))
    login_lockout_minutes: int = int(os.getenv("LOGIN_LOCKOUT_MINUTES", "15"))
    password_min_length: int = int(os.getenv("PASSWORD_MIN_LENGTH", "10"))
    password_require_upper: bool = _as_bool("PASSWORD_REQUIRE_UPPER", True)
    password_require_lower: bool = _as_bool("PASSWORD_REQUIRE_LOWER", True)
    password_require_digit: bool = _as_bool("PASSWORD_REQUIRE_DIGIT", True)


settings = AppConfig()
