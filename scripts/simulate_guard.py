"""Simulate the mass-closure guard (E-06, SPEC §5.4) against a source's real
active postings, inside a transaction that is always rolled back.

Builds a synthetic snapshot from real active courses (either empty, or keeping
only a share of them) and feeds it through the real process_pending_files()
loader path, so the simulation exercises the exact same guard logic the DAG
uses. Never commits.

Usage:
    python -m scripts.simulate_guard --source softserve --empty
    python -m scripts.simulate_guard --source epam --keep-share 0.2
"""
import argparse
import json

from core.db import Session
from core.model import Course, FileRecord, LoadStats
from core.repository import CourseRepository
from utils.time import utcnow


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self, payload: bytes):
        self._payload = payload

    def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self._payload)}


def _build_synthetic_raw(source: str, keep_courses: list[Course]) -> bytes:
    if source == "epam":
        items = [
            {
                "Id": course.source_id,
                "Name": course.title,
                "PlanLevel": 3,
                "PlanLocations": [{"Country": "Ukraine"}],
                "Type": course.course_type,
                "MainSkillStringId": course.direction,
                "Format": course.format,
                "Level": course.level,
                "Pricing": course.price,
                "DateStarted": None,
                "DateFinished": None,
                "Status": course.status,
                "ProgramLanguages": course.languages,
            }
            for course in keep_courses
        ]
        return json.dumps({"pageProps": {"trainings": {"Items": items}}}).encode("utf-8")

    data = [
        {
            "id": course.source_id,
            "name": course.title,
            "url": course.url,
            "type_name": course.course_type,
            "format": course.format,
            "payment": course.price,
            "start_at": None,
            "end_at": None,
            "status": course.status,
            "description": course.description,
        }
        for course in keep_courses
    ]
    return json.dumps({"data": data}).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=["epam", "softserve"])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--empty", action="store_true", help="simulate a snapshot with 0 postings (expect R1)")
    group.add_argument("--keep-share", type=float, help="keep only this share of active postings (low values expect R3)")
    args = parser.parse_args()

    session = Session()
    courses_before = session.query(Course).count()
    load_stats_before = session.query(LoadStats).count()

    try:
        active = list(CourseRepository(session).get_active_by_source(args.source).values())

        if args.empty:
            keep = []
        else:
            keep_n = max(0, round(len(active) * args.keep_share))
            keep = active[:keep_n]

        raw = _build_synthetic_raw(args.source, keep)

        file_record = FileRecord(
            bucket="wfinder",
            key=f"raw/{args.source}_simulate_guard.json",
            etag="simulate",
            source=args.source,
            size_bytes=len(raw),
            fetched_at=utcnow(),
        )
        session.add(file_record)
        session.flush()

        import storage.loader as loader_module
        loader_module.get_s3_client = lambda: _FakeS3Client(raw)
        from storage.loader import process_pending_files

        report = process_pending_files(session, run_id="simulate", commit_per_file=False)

        print(f"source={args.source} active_before={len(active)} received={len(keep)}")
        if report.errors:
            for error in report.errors:
                print(f"error: {error['message']}")
        if report.blocked:
            for entry in report.blocked:
                print(f"blocked: {entry['reason']}")
        else:
            print("blocked: none (closure allowed)")
    finally:
        session.rollback()
        courses_after = session.query(Course).count()
        load_stats_after = session.query(LoadStats).count()
        session.close()
        print(f"rows unchanged -> courses: {courses_before}/{courses_after}, load_stats: {load_stats_before}/{load_stats_after}")


if __name__ == "__main__":
    main()
