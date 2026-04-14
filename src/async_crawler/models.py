from dataclasses import dataclass
from typing import Optional

from src.async_crawler.enums import FetchResultStatus
from src.html_parser.models import ParseResult


@dataclass
class FetchResult:
    url: str
    status: FetchResultStatus
    html: Optional[str] = None
    error: Optional[str] = None
    parsed: Optional[ParseResult] = None

@dataclass
class CrawlResult:
    failed_urls: dict[str, FetchResult]
    processed_urls: dict[str, FetchResult]
