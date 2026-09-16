"""Rule-based classification of level/format/city/country (SPEC §9). Pure -
no DB access. Derived fields never feed content_hash."""
from dataclasses import dataclass
from pathlib import Path

import yaml

GEO_ALIASES_PATH = Path(__file__).resolve().parent / "geo_aliases.yaml"
RULES_VERSION = 1

LEVEL_VALUES = ("intern", "junior", "middle", "senior", "unknown")
FORMAT_VALUES = ("online", "offline", "hybrid", "unknown")

LEVEL_KEYWORDS: dict[str, list[str]] = {
    "intern": ["intern", "internship", "стажування"],
    "junior": ["junior", "beginner", "basic", "entry level", "trainee"],
    "middle": ["middle", "intermediate", "fundamentals"],
    "senior": ["senior", "advanced", "specialization", "expert"],
}

FORMAT_KEYWORDS: dict[str, list[str]] = {
    "online": ["online", "remote", "self-paced"],
    "offline": ["offline", "on-site", "face-to-face", "classroom"],
    "hybrid": ["hybrid", "blended"],
}


@dataclass(frozen=True)
class Classification:
    level_norm: str
    format_norm: str
    city_norm: str | None
    country_norm: str | None


def _load_geo_aliases() -> dict:
    with open(GEO_ALIASES_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_GEO = _load_geo_aliases()
_CITY_ALIASES: dict[str, str] = _GEO.get("cities", {})
_COUNTRY_ALIASES: dict[str, str] = _GEO.get("countries", {})


def _normalize_geo(value: str | None, aliases: dict[str, str]) -> str | None:
    if not value or not value.strip():
        return None
    return aliases.get(value.strip().lower(), value.strip())


def _match_keyword(value: str | None, keywords: dict[str, list[str]]) -> str | None:
    if not value:
        return None
    text = value.lower()
    for norm, words in keywords.items():
        if any(word in text for word in words):
            return norm
    return None


def classify(fields: dict[str, str | None]) -> Classification:
    """fields: level, course_type, title, format, city, country (SPEC §9)."""
    level_norm = (
        _match_keyword(fields.get("level"), LEVEL_KEYWORDS)
        or _match_keyword(fields.get("course_type"), LEVEL_KEYWORDS)
        or _match_keyword(fields.get("title"), LEVEL_KEYWORDS)
        or "unknown"
    )
    format_norm = _match_keyword(fields.get("format"), FORMAT_KEYWORDS) or "unknown"

    return Classification(
        level_norm=level_norm,
        format_norm=format_norm,
        city_norm=_normalize_geo(fields.get("city"), _CITY_ALIASES),
        country_norm=_normalize_geo(fields.get("country"), _COUNTRY_ALIASES),
    )
