"""Print 20 random active courses with their matched skills and match
snippets, for manual review of false positives (SPEC §8.6).

Usage: python -m scripts.skills_sample
"""
from sqlalchemy import func, select

from core.db import Session
from core.model import Course, CourseSkill, Skill

SAMPLE_SIZE = 20


def main() -> None:
    session = Session()
    try:
        stmt = (
            select(Course)
            .where(Course.active_to.is_(None))
            .order_by(func.random())
            .limit(SAMPLE_SIZE)
        )
        courses = list(session.scalars(stmt))
        if not courses:
            print("no active courses found")
            return

        for course in courses:
            print(f"\n=== {course.source}/{course.source_id}: {course.title!r} ===")
            matches = session.execute(
                select(Skill.name, CourseSkill.matched_text, CourseSkill.field)
                .join(CourseSkill, CourseSkill.skill_id == Skill.id)
                .where(CourseSkill.course_id == course.id)
                .order_by(Skill.name)
            ).all()
            if not matches:
                print("  (no skills matched)")
                continue
            for skill_name, matched_text, field in matches:
                print(f"  {skill_name}: {matched_text!r} (in {field})")
    finally:
        session.close()


if __name__ == "__main__":
    main()
