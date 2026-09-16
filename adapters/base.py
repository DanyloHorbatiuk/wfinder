from abc import ABC, abstractmethod

from core.model import Course


class BaseAdapter(ABC):
    source: str
    PARSER_VERSION: int = 1

    @abstractmethod
    def parse(self, raw: dict, file_record_id: int) -> list[Course]:
        """Перетворює сирий знімок джерела в список уніфікованих Course"""
        pass
