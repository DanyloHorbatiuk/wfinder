import json
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.orm import Session

from adapters.epam import EpamAdapter
from adapters.softserve import SoftServeAdapter
from core.db import Session as SessionFactory
from core.hashing import content_hash
from core.model import Course, FileRecord
from core.repository import CourseRepository, FileRecordRepository
from storage.minio import get_s3_client
from utils.logger import get_logger
from utils.time import utcnow

logger = get_logger(__name__)

ADAPTERS = {
    "epam": EpamAdapter(),
    "softserve": SoftServeAdapter(),
}

ERROR_MESSAGE_MAX_LEN = 1000


@dataclass
class SourceStats:
    received: int = 0
    inserted: int = 0
    changed: int = 0
    unchanged: int = 0
    closed: int = 0
    rejected: int = 0

    def to_dict(self) -> dict:
        return {
            "received": self.received,
            "inserted": self.inserted,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "closed": self.closed,
            "rejected": self.rejected,
        }


@dataclass
class LoadReport:
    """SPEC §5.1. Serialized to a dict for Airflow XCom via to_dict()."""

    new_course_ids: list[int] = field(default_factory=list)
    per_source: dict[str, SourceStats] = field(default_factory=dict)
    blocked: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)

    def stats_for(self, source: str) -> SourceStats:
        return self.per_source.setdefault(source, SourceStats())

    def to_dict(self) -> dict:
        return {
            "new_course_ids": self.new_course_ids,
            "per_source": {s: stats.to_dict() for s, stats in self.per_source.items()},
            "blocked": self.blocked,
            "errors": self.errors,
        }


def process_pending_files(session: Session, run_id: str | None = None, commit_per_file: bool = True) -> LoadReport:
    """SPEC §5.1. Closing missing postings (E-06) and load_stats (E-08) are not
    implemented yet - this only covers insert/changed/unchanged (§5.3 steps 1-4)."""
    report = LoadReport()
    failed_sources: set[str] = set()

    file_repo = FileRecordRepository(session)
    pending = file_repo.get_pending_ordered()
    logger.info(f"{len(pending)} pending files to process")

    s3_client = get_s3_client()

    for file_record in pending:
        if file_record.source in failed_sources:
            continue

        savepoint = session.begin_nested() if not commit_per_file else None
        try:
            _process_one_file(session, file_record, s3_client, report)
        except Exception as exc:
            logger.error(f"error processing file {file_record.key}: {exc}")
            if savepoint is not None:
                savepoint.rollback()
            else:
                session.rollback()
            message = str(exc)[:ERROR_MESSAGE_MAX_LEN]
            file_record.status = "error"
            file_record.error_message = message
            if commit_per_file:
                session.commit()
            else:
                session.flush()
            report.errors.append({"file_key": file_record.key, "message": message})
            failed_sources.add(file_record.source)
        else:
            if savepoint is not None:
                savepoint.commit()
            if commit_per_file:
                session.commit()

    logger.info(f"pending files processed: {report.to_dict()}")
    return report


def _process_one_file(session: Session, file_record: FileRecord, s3_client, report: LoadReport) -> None:
    logger.info(f"processing file {file_record.key}...")

    adapter = ADAPTERS.get(file_record.source)
    if adapter is None:
        raise ValueError(f"unknown source: {file_record.source}")

    response = s3_client.get_object(Bucket=file_record.bucket, Key=file_record.key)
    raw = json.loads(response["Body"].read())
    candidates = adapter.parse(raw, file_record.id)

    snapshot_at = file_record.fetched_at
    stats = report.stats_for(file_record.source)

    deduped: dict[str, Course] = {}
    duplicates = 0
    for candidate in candidates:
        if candidate.source_id in deduped:
            duplicates += 1
        deduped[candidate.source_id] = candidate  # last one in the file wins
    if duplicates:
        logger.info(f"file {file_record.key}: {duplicates} duplicate source_id(s) in snapshot")

    stats.received += len(deduped)

    course_repo = CourseRepository(session)
    active_by_source_id = course_repo.get_active_by_source(file_record.source)

    for source_id, candidate in deduped.items():
        candidate.file_record_id = file_record.id
        h = content_hash(candidate)
        existing = active_by_source_id.get(source_id)

        if existing is None:
            candidate.content_hash = h
            candidate.active_from = snapshot_at
            candidate.last_seen_at = snapshot_at
            session.add(candidate)
            session.flush()
            report.new_course_ids.append(candidate.id)
            stats.inserted += 1
        elif existing.content_hash is None:
            # legacy row predating content_hash: backfill without creating a new version
            existing.content_hash = h
            existing.last_seen_at = snapshot_at
            stats.unchanged += 1
        elif existing.content_hash == h:
            existing.last_seen_at = snapshot_at
            stats.unchanged += 1
        else:
            existing.active_to = snapshot_at
            existing.close_reason = "changed"
            session.flush()  # required before insert: partial unique index on (source, source_id)
            candidate.content_hash = h
            candidate.active_from = snapshot_at
            candidate.last_seen_at = snapshot_at
            session.add(candidate)
            session.flush()
            stats.changed += 1

    file_record.status = "done"
    file_record.processed_at = utcnow()
    logger.info(f"file '{file_record.key}' processed: {stats.to_dict()}")


def save_file_record(
    bucket: str,
    key: str,
    etag: str,
    source: str,
    size_bytes: int,
    fetched_at: datetime,
) -> FileRecord:
    session = SessionFactory()
    try:
        file_record = FileRecord(
            bucket=bucket,
            key=key,
            etag=etag,
            source=source,
            size_bytes=size_bytes,
            fetched_at=fetched_at,
        )
        session.add(file_record)
        session.commit()
        return file_record
    finally:
        session.close()
