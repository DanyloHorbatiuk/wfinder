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
    notify_task = notify_digest(load_task)
    finalize_task = finalize(load_task)

    _ = fetch_task >> load_task >> notify_task >> finalize_task


load_courses_pipeline_dag()
