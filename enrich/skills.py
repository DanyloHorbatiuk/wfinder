"""Skill extraction from course text fields (SPEC §8.4). Pure - no DB access."""
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

TAXONOMY_PATH = Path(__file__).resolve().parent / "skills_taxonomy.yaml"
SKILLS_VERSION = 1

STRICT_ALIAS_FIELDS = ("title", "tags")

_BOUNDARY_LEFT = r"(?<![A-Za-z0-9+#.])"
_BOUNDARY_RIGHT = r"(?![A-Za-z0-9+#])"


@dataclass(frozen=True)
class Match:
    skill: str
    matched_text: str
    field: str


def _load_taxonomy() -> dict:
    with open(TAXONOMY_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _pattern(alias: str, case_sensitive: bool) -> re.Pattern:
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(_BOUNDARY_LEFT + re.escape(alias) + _BOUNDARY_RIGHT, flags)


_TAXONOMY = _load_taxonomy()
SKILLS: list[dict] = _TAXONOMY.get("skills", [])
DIRECTION_MAP: dict[str, list[str]] = _TAXONOMY.get("direction_map", {})

# (skill_name, pattern, case_sensitive) triples, precompiled once at import time.
_ALIAS_PATTERNS: list[tuple[str, re.Pattern]] = []
_STRICT_PATTERNS: list[tuple[str, re.Pattern]] = []

for _skill in SKILLS:
    _name = _skill["name"]
    for _alias in _skill.get("aliases", []):
        _ALIAS_PATTERNS.append((_name, _pattern(_alias, case_sensitive=False)))
    for _alias in _skill.get("strict_aliases", []):
        _STRICT_PATTERNS.append((_name, _pattern(_alias, case_sensitive=True)))


def extract(fields: dict[str, str | None]) -> set[Match]:
    """fields keys are whichever of title/direction/description/tags exist for
    this course (SPEC §8.2). Returns every (skill, matched_text, field) hit."""
    matches: set[Match] = set()

    for skill_name, pattern in _ALIAS_PATTERNS:
        for field_name, text in fields.items():
            if field_name == "direction" or not text:
                continue
            found = pattern.search(text)
            if found:
                matches.add(Match(skill=skill_name, matched_text=found.group(0), field=field_name))

    for skill_name, pattern in _STRICT_PATTERNS:
        for field_name in STRICT_ALIAS_FIELDS:
            text = fields.get(field_name)
            if not text:
                continue
            found = pattern.search(text)
            if found:
                matches.add(Match(skill=skill_name, matched_text=found.group(0), field=field_name))

    direction = fields.get("direction")
    if direction:
        for skill_name in DIRECTION_MAP.get(direction, []):
            matches.add(Match(skill=skill_name, matched_text=direction, field="direction"))

    return matches


_FIELD_PRIORITY = {"title": 0, "direction": 1, "description": 2, "tags": 3}


def pick_one_per_skill(matches: set[Match]) -> dict[str, Match]:
    """course_skill stores one (matched_text, field) row per (course, skill) -
    PRIMARY KEY (course_id, skill_id) per SPEC §8.5 - so when the same skill is
    hit via more than one alias/field, deterministically keep one: prefer
    title, then direction, then description, then tags; ties broken by text."""
    best: dict[str, Match] = {}
    for match in matches:
        key = (_FIELD_PRIORITY.get(match.field, 99), match.matched_text)
        current = best.get(match.skill)
        if current is None or key < (_FIELD_PRIORITY.get(current.field, 99), current.matched_text):
            best[match.skill] = match
    return best
