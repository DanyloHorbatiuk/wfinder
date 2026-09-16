from datetime import datetime

from pydantic import ValidationError

from adapters.base import BaseAdapter, ParseResult, Rejected
from core.model import Course
from core.validation import CourseIn


class SoftServeAdapter(BaseAdapter):
    source = "softserve"

    def parse(self, raw: dict, file_record_id: int) -> ParseResult:
        all_courses = raw["data"]
        valid: list[Course] = []
        rejected: list[Rejected] = []

        for course in all_courses:
            source_id = str(course["id"])
            url = course.get("url")
            date_start = self._parse_date(course.get("start_at"))
            date_end = self._parse_date(course.get("end_at"))

            try:
                CourseIn(
                    source_id=source_id,
                    title=course["name"],
                    url=url,
                    date_start=date_start,
                    date_end=date_end,
                    languages=None,
                )
            except ValidationError as exc:
                rejected.append(Rejected(source_id=source_id, reason=str(exc), raw_item=course))
                continue

            valid.append(
                Course(
                    source=self.source,
                    source_id=source_id,
                    title=course["name"],
                    url=url,
                    course_type=course.get("type_name"),
                    direction=None,
                    format=course.get("format"),
                    level=None,
                    price=course.get("payment"),
                    date_start=date_start,
                    date_end=date_end,
                    status=course.get("status"),
                    country="Ukraine",
                    city=None,
                    languages=None,
                    description=course.get("description"),
                    file_record_id=file_record_id,
                )
            )
        return ParseResult(valid=valid, rejected=rejected)

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
