from datetime import datetime

from adapters.base import BaseAdapter
from core.model import Course


class SoftServeAdapter(BaseAdapter):
    source = "softserve"

    def parse(self, raw: dict, file_record_id: int) -> list[Course]:
        all_courses = raw["data"]
        courses = []

        for course in all_courses:
            courses.append(
                Course(
                    source=self.source,
                    source_id=str(course["id"]),
                    title=course["name"],
                    url=course.get("url"),
                    course_type=course.get("type_name"),
                    direction=None,
                    format=course.get("format"),
                    level=None,
                    price=course.get("payment"),
                    date_start=self._parse_date(course.get("start_at")),
                    date_end=self._parse_date(course.get("end_at")),
                    status=course.get("status"),
                    country="Ukraine",
                    city=None,
                    languages=None,
                    description=course.get("description"),
                    file_record_id=file_record_id,
                )
            )
        return courses

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")