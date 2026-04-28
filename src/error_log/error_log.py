
from typing import Optional
import json
from dataclasses import asdict, dataclass

from src.async_crawler.enums import FetchResultStatus
from src.retry_strategy.models import BaseError


@dataclass
class LogEntity:
    error_type: BaseError
    url: str
    try_count: int
    try_count_by_type: int
    backoff: float
    result: Optional[FetchResultStatus] = None

class ErrorLog:
    def __init__(self):
        self.log: list[LogEntity] = []

    def append(self, log: LogEntity):
        self.log.append(log)

    def mark_finished(self, url: str):
        for log in self.log:
            if log.url == url and log.result is None:
                log.result = FetchResultStatus.FINISHED

    def mark_failed(self, url: str):
        for log in self.log:
            if log.url == url and log.result is None:
                log.result = FetchResultStatus.FAILED

    def export_error_log(self, filename: str):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(
                [asdict(log) for log in self.log],
                f,
                ensure_ascii=False,
                indent=2,
                default=str
            )
