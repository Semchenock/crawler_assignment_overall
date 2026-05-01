from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedRobotsData:
    allow: list[str]
    disallow: list[str]
    sitemap_url: Optional[str]
    crawl_delay: Optional[float]
