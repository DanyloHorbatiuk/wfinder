import html

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.db import Session as SessionFactory
from core.model import Course
from notify.telegram import send_telegram_message
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_PART_LEN = 4000


def build_and_send_digest(load_report: dict) -> None:
    """SPEC §7.1: built from the LoadReport (XCom), not from Course.created_at (D2)."""
    session = SessionFactory()
    try:
        parts = build_digest_parts(session, load_report)
        for part in parts:
            send_telegram_message(part)
        logger.info(f"digest sent: {len(parts)} part(s)")
    finally:
        session.close()


def build_digest_parts(session: Session, load_report: dict) -> list[str]:
    new_course_ids = load_report.get("new_course_ids") or []
    per_source = load_report.get("per_source") or {}
    blocked = load_report.get("blocked") or []
    errors = load_report.get("errors") or []

    total_changed = sum(stats.get("changed", 0) for stats in per_source.values())
    total_closed = sum(stats.get("closed", 0) for stats in per_source.values())

    lines: list[str] = []

    if new_course_ids:
        lines.append(f"🆕 Нові пропозиції: {len(new_course_ids)}\n")
        for course in _load_courses(session, new_course_ids):
            free_label = "🆓" if course.price == "Free" else "💰"
            line = f"{free_label} <b>{html.escape(course.title or '')}</b> ({html.escape(course.source or '')})"
            if course.url:
                line += f"\n{html.escape(course.url)}"
            lines.append(line)
    else:
        lines.append("Сьогодні нових курсів немає.")

    if total_changed or total_closed:
        lines.append(f"\nЗмінено: {total_changed} · Закрито: {total_closed}")

    for entry in blocked:
        source = html.escape(str(entry.get("source", "")))
        reason = html.escape(str(entry.get("reason", "")))
        lines.append(f"⚠️ Закриття пропущено: {source} ({reason})")

    if errors:
        lines.append(f"❗ Помилки обробки: {len(errors)} файлів")

    return _split_into_parts(lines)


def _load_courses(session: Session, course_ids: list[int]) -> list[Course]:
    if not course_ids:
        return []
    stmt = select(Course).where(Course.id.in_(course_ids))
    by_id = {course.id: course for course in session.scalars(stmt)}
    return [by_id[cid] for cid in course_ids if cid in by_id]


def _split_into_parts(lines: list[str]) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        line_len = len(line) + 1  # +1 for the newline that will join it
        if current and current_len + line_len > MAX_PART_LEN:
            parts.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += line_len
    if current:
        parts.append("\n".join(current))
    return parts or [""]
