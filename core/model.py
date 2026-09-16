from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

from utils.time import utcnow

Base = declarative_base()


class FileRecord(Base):
    __tablename__ = "file_record"

    id = Column(Integer, primary_key=True)
    bucket = Column(String, nullable=False)
    key = Column(String, unique=True, nullable=False)
    etag = Column(String, nullable=False)
    source = Column(String, nullable=False)  # "epam" | "softserve" | "sigma"
    status = Column(String, default="pending", nullable=False)  # pending → done/error
    size_bytes = Column(Integer, nullable=True)
    fetched_at = Column(DateTime, nullable=False)  # time of the source snapshot, not the upload
    uploaded_at = Column(DateTime, default=utcnow, nullable=False)
    processed_at = Column(DateTime, nullable=True)
    error_message = Column(String, nullable=True)

    courses = relationship(
        "Course",
        back_populates="file_record",
        cascade="all, delete-orphan",  # каскадне видалення
    )

    def __repr__(self) -> str:
        return f"FileRecord(id={self.id}, bucket={self.bucket}, key={self.key})"


class Course(Base):
    __tablename__ = "courses"
    __table_args__ = (
        # checks unique combinations of ("source", "source_id") only in rows with active status
        Index(
            "uq_active_course_per_source",
            "source", "source_id",
            unique=True,
            postgresql_where=Column("active_to").is_(None),
        ),
        Index("ix_courses_posting", "source", "source_id", "active_from"),
        CheckConstraint(
            "close_reason IN ('changed', 'removed')",
            name="ck_courses_close_reason_valid",
        ),
        # NOTE: the composite CHECK ((active_to IS NULL) = (close_reason IS NULL)) from
        # SPEC §4.2 is intentionally NOT added yet - pre-existing closed rows have
        # active_to set with close_reason still NULL (content_hash-based history starts
        # now). Add it once replay (E-03) has backfilled close_reason for old rows.
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)  # "epam" | "softserve" | "sigma"
    source_id = Column(String, nullable=False)  # original id from source (str to cover all cases)
    title = Column(String, nullable=False)
    url = Column(String, nullable=True)
    course_type = Column(String, nullable=True)  # "Training", "Internship", "Course"
    direction = Column(String, nullable=True)  # "DevOps", "CloudAndDevOps", "QA"
    format = Column(String, nullable=True)  # "Online", "Offline"
    level = Column(String, nullable=True)  # "Junior", "Middle", "Specialization"
    price = Column(String, nullable=True)  # "Free" or actual price string
    date_start = Column(DateTime, nullable=True)
    date_end = Column(DateTime, nullable=True)
    status = Column(String, nullable=True)  # "Open for Registration", "RegistrationOpen"
    country = Column(String, nullable=True)
    city = Column(String, nullable=True)
    languages = Column(JSONB, nullable=True)  # ["English", "Ukrainian"]
    description = Column(Text, nullable=True)  # populated for SoftServe only; EPAM has no such field (I-01)

    content_hash = Column(String(64), nullable=True)  # sha256 of tracked fields, see core/hashing.py
    last_seen_at = Column(DateTime, nullable=True)  # snapshot_at of the last time this version was observed
    close_reason = Column(String(16), nullable=True)  # "changed" | "removed", NULL while active

    created_at = Column(DateTime, default=utcnow, nullable=False)
    active_from = Column(DateTime, nullable=True)
    active_to = Column(DateTime, nullable=True)  # NULL = active

    file_record_id = Column(Integer, ForeignKey("file_record.id"), nullable=False)
    file_record = relationship("FileRecord", back_populates="courses")

    def __repr__(self) -> str:
        return (
            f"Course(id={self.id}/{self.source_id}, source={self.source!r}, "
            f"title={self.title!r}, type={self.course_type!r}, status={self.status!r})"
        )


class LoadStats(Base):
    """One row per processed file_record (SPEC §4.2, §5.3 п.7)."""

    __tablename__ = "load_stats"

    id = Column(Integer, primary_key=True)
    file_record_id = Column(Integer, ForeignKey("file_record.id"), unique=True, nullable=False)
    source = Column(String, nullable=False)
    snapshot_at = Column(DateTime, nullable=False)  # = file_record.fetched_at
    run_id = Column(String, nullable=True)  # Airflow run_id or "replay"

    received = Column(Integer, nullable=False)  # distinct source_id in the snapshot, including rejected
    rejected = Column(Integer, nullable=False, default=0)  # 0 until E-05
    duplicates = Column(Integer, nullable=False, default=0)  # repeated source_id in the snapshot
    inserted = Column(Integer, nullable=False)
    changed = Column(Integer, nullable=False)
    unchanged = Column(Integer, nullable=False)
    closed = Column(Integer, nullable=False, default=0)  # 0 until E-06

    closures_blocked = Column(Boolean, nullable=False, default=False)  # until E-06
    block_reason = Column(Text, nullable=True)

    parser_version = Column(Integer, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"LoadStats(file_record_id={self.file_record_id}, source={self.source!r}, snapshot_at={self.snapshot_at})"
