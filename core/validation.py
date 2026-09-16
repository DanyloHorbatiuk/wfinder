"""Post-mapping validation of a single parsed record (SPEC §10)."""
from datetime import datetime

from pydantic import BaseModel, field_validator, model_validator

TITLE_MAX_LEN = 500


class CourseIn(BaseModel):
    source_id: str
    title: str
    url: str | None = None
    date_start: datetime | None = None
    date_end: datetime | None = None
    languages: list[str] | None = None

    model_config = {"extra": "ignore"}

    @field_validator("source_id")
    @classmethod
    def _source_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("source_id must not be empty")
        return v

    @field_validator("title")
    @classmethod
    def _title_valid(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("title must not be empty")
        if len(v) > TITLE_MAX_LEN:
            raise ValueError(f"title must not exceed {TITLE_MAX_LEN} characters")
        return v

    @field_validator("url")
    @classmethod
    def _url_valid(cls, v: str | None) -> str | None:
        if v is not None and not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("url must start with http(s) or be null")
        return v

    @model_validator(mode="after")
    def _dates_ordered(self) -> "CourseIn":
        if self.date_start is not None and self.date_end is not None and self.date_end < self.date_start:
            raise ValueError("date_end must be >= date_start")
        return self
