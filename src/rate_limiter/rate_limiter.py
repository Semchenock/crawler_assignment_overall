import asyncio
import time
import logging
from collections import defaultdict
import random
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)


class _RateLimitContext:
    def __init__(self, limiter: RateLimiter, url: str | None):
        self.limiter = limiter
        self.url = url

    async def __aenter__(self):
        await self.limiter.acquire(self.url)

    async def __aexit__(self, exc_type, exc, tb):
        pass

class RateLimiter:
    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True, min_interval: float = 0.0, max_jitter: float = 0.0):
        self.min_interval = min_interval
        self.max_jitter = max_jitter
        self.interval = 1.0 / requests_per_second
        self.per_domain = per_domain
        self.domain_interval: dict[str, float] = defaultdict(lambda: self.interval)

        self.lock = asyncio.Lock()

        self.next_allowed_time = 0.0
        self.domain_next_allowed = defaultdict(float)

    def __call__(self, url: str | None = None):
        return _RateLimitContext(self, url)

    def _get_nex_interval(self, domain: str) -> float:
        rps_interval = self.domain_interval[domain] if self.per_domain and domain else self.interval
        interval = max(rps_interval, self.min_interval)
        jitter = random.uniform(0, self.max_jitter)
        return interval + jitter


    async def acquire(self, url: str | None = None):
        domain = urlparse(url).hostname

        while True:
            async with self.lock:
                now = time.monotonic()

                if self.per_domain and domain:
                    next_time = self.domain_next_allowed[domain]
                else:
                    next_time = self.next_allowed_time

                wait_time = next_time - now

                if wait_time <= 0:
                    interval = self._get_nex_interval(domain)

                    if self.per_domain and domain:
                        new_next_time = now + interval
                        self.domain_next_allowed[domain] = new_next_time
                    else:
                        new_next_time = now + interval
                        self.next_allowed_time = new_next_time

                    return

            if wait_time <= 0:
                return

            await asyncio.sleep(wait_time)

    def set_domain_interval(self, url: str, interval: float):
        if not self.per_domain:
            logging.warning("Rate limiter per domain is disabled")
            return

        self.domain_interval[url] = interval
