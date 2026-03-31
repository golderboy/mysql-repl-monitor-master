from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


SEVERITY_ORDER = {
    "INFO": 10,
    "WARNING": 20,
    "CRITICAL": 30,
}


def severity_rank(value: str | None) -> int:
    if not value:
        return 0
    return SEVERITY_ORDER.get(value.upper(), 0)
