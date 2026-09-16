import hashlib
import json
from datetime import datetime
from typing import Any

from core.model import Course

TRACKED_FIELDS = (
    "title",
    "url",
    "course_type",
    "direction",
    "format",
    "level",
    "price",
    "date_start",
    "date_end",
    "status",
    "country",
    "city",
    "languages",
    "description",
)


def _normalize(value: Any) -> Any:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return sorted(set(value))
    return value


def content_hash(course: Course) -> str:
    """sha256(hex) of the canonical JSON of the tracked fields (SPEC §5.2)."""
    canonical = {field: _normalize(getattr(course, field)) for field in TRACKED_FIELDS}
    payload = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
