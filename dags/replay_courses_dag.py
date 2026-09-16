from airflow.sdk import dag, task

TABLES_TO_BACKUP = ("courses", "load_stats", "course_skill", "course_enrichment")
DERIVED_TABLES_TO_TRUNCATE = ("course_skill", "course_enrichment", "load_stats", "courses")


@dag(
    schedule=None,
    catchup=False,
    params={"dry_run": True, "confirm": ""},
)
def replay_courses_dag():

    @task
    def validate_params() -> dict:
        """dry_run=false requires confirm='REPLAY', otherwise the task fails (SPEC §6.2)."""
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context

        context = get_current_context()
        params = context["params"]
        dry_run = bool(params.get("dry_run", True))
        confirm = params.get("confirm", "")
        if not dry_run and confirm != "REPLAY":
            raise AirflowFailException(
                "dry_run=false requires confirm='REPLAY' - refusing to run a real replay without explicit confirmation"
            )
        return {"dry_run": dry_run}

    @task
    def sync_file_records() -> dict:
        """Walk every raw/ object (with pagination) and reconcile file_record (SPEC §6.2 step 1)."""
        from sqlalchemy import select

        from core.db import Session
        from core.model import FileRecord
        from storage.minio import BUCKET, get_s3_client
        from utils.logger import get_logger

        logger = get_logger(__name__)
        session = Session()
        try:
            s3_client = get_s3_client()
            existing_by_key = {fr.key: fr for fr in session.scalars(select(FileRecord))}

            created = 0
            backfilled = 0
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=BUCKET, Prefix="raw/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    last_modified = obj["LastModified"].replace(tzinfo=None)
                    source = key.removeprefix("raw/").split("_")[0]
                    existing = existing_by_key.get(key)
                    if existing is None:
                        session.add(
                            FileRecord(
                                bucket=BUCKET,
                                key=key,
                                etag=obj["ETag"].strip('"'),
                                source=source,
                                size_bytes=obj["Size"],
                                fetched_at=last_modified,
                            )
                        )
                        created += 1
                    elif existing.fetched_at is None:
                        existing.fetched_at = last_modified
                        backfilled += 1
            session.commit()
            logger.info(f"sync_file_records: created={created} backfilled={backfilled}")
            return {"created": created, "backfilled": backfilled}
        finally:
            session.close()

    @task
    def backup(validated: dict) -> list[str]:
        """CREATE TABLE backup_<ts>_<table> AS SELECT * for existing derived tables,
        skipped entirely in dry_run (SPEC §6.2 step 2)."""
        if validated["dry_run"]:
            return []

        from sqlalchemy import text

        from core.db import engine
        from utils.logger import get_logger
        from utils.time import utcnow

        logger = get_logger(__name__)
        suffix = utcnow().strftime("%Y%m%d%H%M")
        created = []
        with engine.begin() as conn:
            for table in TABLES_TO_BACKUP:
                exists = conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :t)"),
                    {"t": table},
                ).scalar()
                if not exists:
                    continue
                backup_name = f"backup_{suffix}_{table}"
                conn.execute(text(f'CREATE TABLE "{backup_name}" AS SELECT * FROM "{table}"'))
                created.append(backup_name)
        logger.info(f"backup: created tables {created}")
        return created

    @task
    def rebuild(validated: dict) -> dict:
        """Truncate derived tables, reset file_record to pending, replay every file
        through the real loader inside one outer transaction: ROLLBACK on dry_run,
        COMMIT otherwise (SPEC §6.2 step 3). Skill/enrichment extraction (E-01/E-04)
        is not implemented yet, so it's not part of the rebuild."""
        import time as time_module

        from sqlalchemy import func, select, text

        from core.db import Session
        from core.model import Course, FileRecord
        from storage.loader import process_pending_files
        from utils.logger import get_logger

        logger = get_logger(__name__)
        dry_run = validated["dry_run"]
        start = time_module.monotonic()
        session = Session()
        try:
            for table in DERIVED_TABLES_TO_TRUNCATE:
                exists = session.execute(
                    text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :t)"),
                    {"t": table},
                ).scalar()
                if exists:
                    session.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))

            session.execute(text("UPDATE file_record SET status = 'pending', error_message = NULL, processed_at = NULL"))
            session.flush()

            report = process_pending_files(session, run_id="replay", commit_per_file=False)

            files_total = session.scalar(select(func.count()).select_from(FileRecord))
            files_done = session.scalar(select(func.count()).select_from(FileRecord).where(FileRecord.status == "done"))
            files_error = session.scalar(select(func.count()).select_from(FileRecord).where(FileRecord.status == "error"))
            versions = session.scalar(select(func.count()).select_from(Course))
            postings_subq = select(Course.source, Course.source_id).distinct().subquery()
            postings = session.scalar(select(func.count()).select_from(postings_subq))

            summary = {
                "dry_run": dry_run,
                "duration_ms": int((time_module.monotonic() - start) * 1000),
                "files_total": files_total,
                "files_done": files_done,
                "files_error": files_error,
                "postings": postings,
                "versions": versions,
                "blocked": report.blocked,
                "errors": report.errors,
            }

            if dry_run:
                session.rollback()
                logger.info(f"rebuild (dry-run, rolled back): {summary}")
            else:
                session.commit()
                logger.info(f"rebuild (committed): {summary}")

            return summary
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @task
    def report(summary: dict) -> None:
        """One summary: files, errors, postings, versions, blocked snapshots,
        duration. Logged always; sent to Telegram only for a real (non-dry-run)
        replay (SPEC §6.2 step 4). No per-snapshot guard alerts during replay."""
        from notify.telegram import send_telegram_message
        from utils.logger import get_logger

        logger = get_logger(__name__)
        lines = [
            "🔁 Replay звіт" + (" (dry-run)" if summary["dry_run"] else ""),
            f"Тривалість: {summary['duration_ms']} мс",
            f"Файли: {summary['files_done']}/{summary['files_total']} done, {summary['files_error']} помилок",
            f"Пропозиції: {summary['postings']}, версії: {summary['versions']}",
            f"Заблоковані знімки: {len(summary['blocked'])}",
        ]
        if summary["errors"]:
            lines.append(f"Помилки обробки: {len(summary['errors'])} файлів")

        text_report = "\n".join(lines)
        logger.info(f"replay report:\n{text_report}")
        if not summary["dry_run"]:
            send_telegram_message(text_report)

    validated = validate_params()
    synced = sync_file_records()
    backed_up = backup(validated)
    summary = rebuild(validated)
    reported = report(summary)

    _ = validated >> synced >> backed_up >> summary >> reported


replay_courses_dag()
