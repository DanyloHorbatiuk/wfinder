from datetime import datetime, timedelta

from airflow.sdk import dag, task


def _task_failure_alert(context: dict) -> None:
    from notify.alerts import task_failure_alert
    task_failure_alert(context)


@dag(
    schedule="30 8 * * *",  # daily at 8:30 am UTC
    start_date=datetime(2026, 6, 16),
    catchup=False,
    default_args={"on_failure_callback": _task_failure_alert},
)

def load_courses_pipeline_dag():

    @task
    def get_sources() -> list[dict]:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        PREFIX = "CAREERS_URL_"
        list = []
        for key, value in os.environ.items():
            if key.startswith(PREFIX):
                list.append({"name": key.removeprefix(PREFIX).lower(), "url": value})
        return list

    @task(retries=3, retry_delay=timedelta(minutes=2), max_active_tis_per_dag=5)
    def fetch_one_source(source: dict) -> dict:
        from fetch import fetch_and_save_single
        return fetch_and_save_single(source["name"], source["url"])

    @task(trigger_rule="all_done")
    def load_all_to_db(run_id: str = None) -> dict:
        from core.db import Session
        from storage.loader import process_pending_files

        session = Session()
        try:
            report = process_pending_files(session, run_id=run_id)
            return report.to_dict()
        finally:
            session.close()

    @task
    def extract_skills() -> dict:
        """SPEC §8.5: sync the taxonomy into skill, then (re)tag every course
        version whose skills_version is stale."""
        from sqlalchemy import delete, select

        from core.db import Session
        from core.model import Course, CourseSkill, Skill
        from enrich.skills import SKILLS, SKILLS_VERSION, extract, pick_one_per_skill
        from utils.logger import get_logger

        logger = get_logger(__name__)
        session = Session()
        try:
            existing_skills = {s.name: s for s in session.scalars(select(Skill))}
            for skill_def in SKILLS:
                name = skill_def["name"]
                category = skill_def["category"]
                existing = existing_skills.get(name)
                if existing is None:
                    skill_row = Skill(name=name, category=category)
                    session.add(skill_row)
                    session.flush()
                    existing_skills[name] = skill_row
                elif existing.category != category:
                    existing.category = category
            session.commit()

            skill_id_by_name = {name: s.id for name, s in existing_skills.items()}

            stmt = select(Course).where(
                (Course.skills_version.is_(None)) | (Course.skills_version < SKILLS_VERSION)
            )
            courses = list(session.scalars(stmt))
            course_ids = [c.id for c in courses]
            if course_ids:
                session.execute(delete(CourseSkill).where(CourseSkill.course_id.in_(course_ids)))

            for course in courses:
                fields = {"title": course.title, "direction": course.direction, "description": course.description}
                for match in pick_one_per_skill(extract(fields)).values():
                    skill_id = skill_id_by_name.get(match.skill)
                    if skill_id is None:
                        continue
                    session.add(
                        CourseSkill(
                            course_id=course.id,
                            skill_id=skill_id,
                            matched_text=match.matched_text[:255],
                            field=match.field,
                        )
                    )
                course.skills_version = SKILLS_VERSION
            session.commit()

            result = {"skills_synced": len(existing_skills), "courses_processed": len(courses)}
            logger.info(f"extract_skills: {result}")
            return result
        finally:
            session.close()

    @task(trigger_rule="all_done")
    def notify_digest(load_report: dict):
        from notify.digest import build_and_send_digest
        build_and_send_digest(load_report)
        return True

    @task(trigger_rule="all_done")
    def finalize(load_report: dict):
        """Fails the DAG run (no retries) if any upstream task failed, or if
        load_all_to_db recorded per-file errors it otherwise isolates and
        swallows by design (F-02). SPEC §6.1."""
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context
        from airflow.utils.state import TaskInstanceState

        context = get_current_context()
        failed = context["dag_run"].get_task_instances(
            state=[TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        )
        if failed:
            task_ids = sorted({ti.task_id for ti in failed})
            raise AirflowFailException(f"upstream task(s) failed: {', '.join(task_ids)}")

        errors = (load_report or {}).get("errors") or []
        if errors:
            raise AirflowFailException(f"load_all_to_db reported {len(errors)} file error(s): {errors}")

    sources = get_sources()
    fetch_task = fetch_one_source.expand(source=sources)
    load_task = load_all_to_db()
    skills_task = extract_skills()
    notify_task = notify_digest(load_task)
    finalize_task = finalize(load_task)

    _ = fetch_task >> load_task >> skills_task >> notify_task >> finalize_task


load_courses_pipeline_dag()
