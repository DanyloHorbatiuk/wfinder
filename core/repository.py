from sqlalchemy import select
from sqlalchemy.orm import Session

from core.model import FileRecord, Course


class FileRecordRepository:

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_records_by_status(self, status: str) -> list[FileRecord]:
        stmt = select(FileRecord).where(FileRecord.status == status)
        return list(self.session.scalars(stmt))

    def get_pending_ordered(self) -> list[FileRecord]:
        stmt = (
            select(FileRecord)
            .where(FileRecord.status == "pending")
            .order_by(FileRecord.fetched_at, FileRecord.id)
        )
        return list(self.session.scalars(stmt))


class CourseRepository:

    def __init__(self, session: Session):
        self.session = session

    def get_active_by_source(self, source: str) -> dict[str, Course]:
        stmt = select(Course).where(Course.source == source, Course.active_to.is_(None))
        return {c.source_id: c for c in self.session.scalars(stmt)}