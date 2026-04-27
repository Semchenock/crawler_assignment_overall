from dataclasses import dataclass
from typing import Optional

from src.async_crawler.enums import FetchResultStatus, ErrorTypes
from src.html_parser.models import ParseResult


@dataclass
class FetchResult:
    url: str
    status: FetchResultStatus
    html: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[ErrorTypes] = None
    parsed: Optional[ParseResult] = None
    status_code: Optional[int] = None
    content_type: Optional[str] = None

@dataclass
class CrawlResult:
    failed_urls: dict[str, FetchResult]
    processed_urls: dict[str, FetchResult]

class BlockedByRobots(Exception):
    pass
