from dataclasses import dataclass
from datetime import datetime

@dataclass
class DataItem:
    url: str
    title: str
    text: str
    links: list[str]
    metadata: dict
    crawled_at: datetime
    status_code: int
    content_type: str


@dataclass
class ErrorLogEntity:
    data: DataItem
    error: Exception
    try_count: int
