from abc import ABC, abstractmethod
from dataclasses import dataclass

from core.model import Course


@dataclass(frozen=True)
class Rejected:
    source_id: str | None
    reason: str
    raw_item: dict


@dataclass(frozen=True)
class ParseResult:
    valid: list[Course]
    rejected: list[Rejected]


class BaseAdapter(ABC):
    source: str
    PARSER_VERSION: int = 1

    @abstractmethod
    def parse(self, raw: dict, file_record_id: int) -> ParseResult:
        """Перетворює сирий знімок джерела в ParseResult(valid, rejected) (SPEC §10)"""
        pass
