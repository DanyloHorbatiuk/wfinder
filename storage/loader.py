import json
import time
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from adapters.epam import EpamAdapter
from adapters.softserve import SoftServeAdapter
from core.db import Session as SessionFactory
from core.guard import evaluate_closure_guard
from core.hashing import content_hash
from core.model import Course, FileRecord, LoadStats
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

    def add(self, other: "SourceStats") -> None:
        self.received += other.received
        self.inserted += other.inserted
        self.changed += other.changed
        self.unchanged += other.unchanged
        self.closed += other.closed
        self.rejected += other.rejected

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
    """SPEC §5.1, §5.3: insert/changed/unchanged (steps 1-4), closing postings
    that disappeared from the snapshot behind the mass-closure guard (steps 5-6,
    E-06), and load_stats (step 7, E-08)."""
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
            _process_one_file(session, file_record, s3_client, report, run_id)
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


def _process_one_file(
    session: Session, file_record: FileRecord, s3_client, report: LoadReport, run_id: str | None
) -> None:
    logger.info(f"processing file {file_record.key}...")
    start = time.monotonic()

    adapter = ADAPTERS.get(file_record.source)
    if adapter is None:
        raise ValueError(f"unknown source: {file_record.source}")

    _check_snapshot_order(session, file_record)

    response = s3_client.get_object(Bucket=file_record.bucket, Key=file_record.key)
    raw = json.loads(response["Body"].read())
    parse_result = adapter.parse(raw, file_record.id)
    candidates = parse_result.valid
    rejected_items = parse_result.rejected

    snapshot_at = file_record.fetched_at
    file_stats = SourceStats()

    deduped: dict[str, Course] = {}
    duplicates = 0
    for candidate in candidates:
        if candidate.source_id in deduped:
            duplicates += 1
        deduped[candidate.source_id] = candidate  # last one in the file wins
    if duplicates:
        logger.info(f"file {file_record.key}: {duplicates} duplicate source_id(s) in snapshot")

    valid_ids = set(deduped.keys())
    # rejected records with a valid source_id count as "received" and "seen" (SPEC §5.3 п.5, §10)
    rejected_ids = {r.source_id for r in rejected_items if r.source_id} - valid_ids

    file_stats.received = len(valid_ids) + len(rejected_ids)
    file_stats.rejected = len(rejected_ids)

    if rejected_items:
        _quarantine_rejected(file_record, rejected_items)

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
            file_stats.inserted += 1
        elif existing.content_hash is None:
            # legacy row predating content_hash: backfill without creating a new version
            existing.content_hash = h
            existing.last_seen_at = snapshot_at
            file_stats.unchanged += 1
        elif existing.content_hash == h:
            existing.last_seen_at = snapshot_at
            file_stats.unchanged += 1
        else:
            existing.active_to = snapshot_at
            existing.close_reason = "changed"
            session.flush()  # required before insert: partial unique index on (source, source_id)
            candidate.content_hash = h
            candidate.active_from = snapshot_at
            candidate.last_seen_at = snapshot_at
            session.add(candidate)
            session.flush()
            file_stats.changed += 1

    seen_ids = valid_ids | rejected_ids
    to_close = [c for sid, c in active_by_source_id.items() if sid not in seen_ids]
    decision = evaluate_closure_guard(
        session,
        source=file_record.source,
        received=file_stats.received,
        active_count=len(active_by_source_id),
        to_close_count=len(to_close),
    )
    if decision.allowed:
        for course in to_close:
            course.active_to = snapshot_at
            course.close_reason = "removed"
        file_stats.closed = len(to_close)
    else:
        logger.info(f"file {file_record.key}: closures blocked - {decision.reason}")
        report.blocked.append({"source": file_record.source, "reason": decision.reason})

    duration_ms = int((time.monotonic() - start) * 1000)
    session.add(
        LoadStats(
            file_record_id=file_record.id,
            source=file_record.source,
            snapshot_at=snapshot_at,
            run_id=run_id,
            received=file_stats.received,
            rejected=file_stats.rejected,
            duplicates=duplicates,
            inserted=file_stats.inserted,
            changed=file_stats.changed,
            unchanged=file_stats.unchanged,
            closed=file_stats.closed,
            closures_blocked=not decision.allowed,
            block_reason=decision.reason,
            parser_version=adapter.PARSER_VERSION,
            duration_ms=duration_ms,
        )
    )
    session.flush()

    report.stats_for(file_record.source).add(file_stats)

    file_record.status = "done"
    file_record.processed_at = utcnow()
    logger.info(f"file '{file_record.key}' processed: {file_stats.to_dict()}")


def _quarantine_rejected(file_record: FileRecord, rejected_items: list) -> None:
    """One MinIO object per snapshot with rejected records: quarantine/{source}/{file_stem}.jsonl
    (SPEC §10). Not part of the DB transaction - MinIO writes aren't rolled back,
    matching how raw/ writes already work."""
    from pathlib import Path

    from storage.minio import put_object

    file_stem = Path(file_record.key).stem
    lines = [
        json.dumps({"source_id": r.source_id, "reason": r.reason, "item": r.raw_item}, ensure_ascii=False, default=str)
        for r in rejected_items
    ]
    content = ("\n".join(lines) + "\n").encode("utf-8")
    put_object(f"quarantine/{file_record.source}/{file_stem}.jsonl", content, content_type="application/jsonl")


def _check_snapshot_order(session: Session, file_record: FileRecord) -> None:
    """SPEC §5.1 п.4: a snapshot older than the last one we already recorded
    for this source means history is out of order - fix via replay (E-03)."""
    last_snapshot_at = session.scalar(
        select(func.max(LoadStats.snapshot_at)).where(LoadStats.source == file_record.source)
    )
    if last_snapshot_at is not None and file_record.fetched_at < last_snapshot_at:
        raise ValueError(
            f"out-of-order snapshot: fetched_at={file_record.fetched_at} "
            f"is older than the last recorded snapshot_at={last_snapshot_at} for source={file_record.source}"
        )


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
