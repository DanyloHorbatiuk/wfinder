from datetime import datetime

from adapters.base import BaseAdapter
from core.model import Course

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class EpamAdapter(BaseAdapter):
    source = "epam"

    def parse(self, raw: dict, file_record_id: int) -> list[Course]:
        all_courses = raw["pageProps"]["trainings"]["Items"]
        courses = []
        for course in all_courses:
            ukraine_found = False
            for country in course["PlanLocations"]:
                if country["Country"] == "Ukraine":
                    ukraine_found = True
            if ukraine_found and course["PlanLevel"] > 2:
                courses.append(
                    Course(
                        source=self.source,
                        source_id=str(course["Id"]),
                        title=course["Name"],
                        url=None,
                        course_type=course.get("Type"),
                        direction=course.get("MainSkillStringId"),
                        format=course.get("Format"),
                        level=course.get("Level"),
                        price=course.get("Pricing"),
                        date_start=self._parse_date(course.get("DateStarted")),
                        date_end=self._parse_date(course.get("DateFinished")),
                        status=course.get("Status"),
                        country="Ukraine",
                        city=None,
                        languages=course.get("ProgramLanguages"),
                        description=None,  # EPAM raw data has no description field (I-01)
                        file_record_id=file_record_id,
                    )
                )
        return courses

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.strptime(value, DATE_FORMAT)
