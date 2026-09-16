from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
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
    status = Column(String, default="pending", nullable=False)  # pending → processing → done/error
    size_bytes = Column(Integer, nullable=True)
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
