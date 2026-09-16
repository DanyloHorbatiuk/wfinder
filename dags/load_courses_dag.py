from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    schedule="30 8 * * *",  # daily at 8:30 am UTC
    start_date=datetime(2026, 6, 16),
    catchup=False,
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

    @task
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
    def notify_new_courses_task():
        from notify.digest import build_and_send_today_digest
        build_and_send_today_digest()
        return True

    sources = get_sources()
    fetch_task = fetch_one_source.expand(source=sources)
    load_task = load_all_to_db()
    notify_task = notify_new_courses_task()

    _ = fetch_task >> load_task >> notify_task


load_courses_pipeline_dag()
