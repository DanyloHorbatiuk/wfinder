from datetime import datetime

from pydantic import ValidationError

from adapters.base import BaseAdapter, ParseResult, Rejected
from core.model import Course
from core.validation import CourseIn

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class EpamAdapter(BaseAdapter):
    source = "epam"

    def parse(self, raw: dict, file_record_id: int) -> ParseResult:
        all_courses = raw["pageProps"]["trainings"]["Items"]
        valid: list[Course] = []
        rejected: list[Rejected] = []
        for course in all_courses:
            ukraine_found = False
            for country in course["PlanLocations"]:
                if country["Country"] == "Ukraine":
                    ukraine_found = True
            if not (ukraine_found and course["PlanLevel"] > 2):
                continue

            source_id = str(course["Id"])
            date_start = self._parse_date(course.get("DateStarted"))
            date_end = self._parse_date(course.get("DateFinished"))

            try:
                CourseIn(
                    source_id=source_id,
                    title=course["Name"],
                    url=None,
                    date_start=date_start,
                    date_end=date_end,
                    languages=course.get("ProgramLanguages"),
                )
            except ValidationError as exc:
                rejected.append(Rejected(source_id=source_id, reason=str(exc), raw_item=course))
                continue

            valid.append(
                Course(
                    source=self.source,
                    source_id=source_id,
                    title=course["Name"],
                    url=None,
                    course_type=course.get("Type"),
                    direction=course.get("MainSkillStringId"),
                    format=course.get("Format"),
                    level=course.get("Level"),
                    price=course.get("Pricing"),
                    date_start=date_start,
                    date_end=date_end,
                    status=course.get("Status"),
                    country="Ukraine",
                    city=None,
                    languages=course.get("ProgramLanguages"),
                    description=None,  # EPAM raw data has no description field (I-01)
                    file_record_id=file_record_id,
                )
            )
        return ParseResult(valid=valid, rejected=rejected)

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.strptime(value, DATE_FORMAT)
