import time
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse


class CrawlerStats:
    def __init__(self):
        self.successful_requests_count = 0
        self.failed_requests_count = 0
        self.processed_urls_by_domain = defaultdict(int)
        self.status_codes = defaultdict(int)
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def reset(self):
        self.successful_requests_count = 0
        self.failed_requests_count = 0
        self.processed_urls_by_domain = defaultdict(int)
        self.status_codes = defaultdict(int)
        self.started_at: Optional[float] = None
        self.ended_at: Optional[float] = None

    def start_crawling(self):
        self.reset()
        self.started_at = time.monotonic()

    def end_crawling(self):
        self.ended_at = time.monotonic()

    def handle_successful_request(self, status_code: int):
        self.successful_requests_count += 1
        self.status_codes[status_code] += 1

    def handle_failed_request(self, status_code: int):
        self.failed_requests_count += 1
        self.status_codes[status_code] += 1

    def handle_processed_url(self, url: str):
        domain = urlparse(url).hostname
        self.processed_urls_by_domain[domain] += 1

    def get_stats(self):
        top_domains = sorted(
            self.processed_urls_by_domain.items(),
            key=lambda x: x[1],
            reverse=True
        )

        top_error_codes = sorted(
            self.status_codes.items(),
            key=lambda x: x[1],
            reverse=True
        )

        total_processed_urls = sum((count for domain, count in self.processed_urls_by_domain))

        end_time = time.monotonic() if self.ended_at is None else self.ended_at

        total_time = end_time - self.started_at

        return {
            'total_processed_urls': total_processed_urls,
            'successful_requests_count': self.successful_requests_count,
            'failed_requests_count': self.failed_requests_count,
            'average_speed': total_time / total_processed_urls,
            'top_domains': top_domains,
            'top_error_codes': top_error_codes,
            'total_time': total_time,
        }

