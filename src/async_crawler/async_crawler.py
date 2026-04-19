import asyncio
from contextlib import asynccontextmanager

import aiohttp
import logging
from typing import Optional
from urllib.parse import urlparse
import re

from src.async_crawler.models import FetchResult, CrawlResult
from src.async_crawler.enums import FetchResultStatus, ErrorTypes
from src.html_parser.html_parser import HtmlParser
from src.crawler_queue.crawler_queue import CrawlerQueue
from src.rate_limiter.rate_limiter import RateLimiter
from src.robots_parser.robots_parser import RobotsParser
from src.semaphore_manager.semaphore_manager import SemaphoreManager

logging.basicConfig(level=logging.INFO)

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 10, min_interval: Optional[float] = None, respect_robots=True, max_jitter: float=0.0):
        self.max_concurrent = max_concurrent
        self.semaphore_manager = SemaphoreManager(max_global=max_concurrent, max_per_domain=max_per_domain)
        self.session = None
        self.html_parser = HtmlParser()
        self.rate_limiter = RateLimiter(per_domain=respect_robots, min_interval=min_interval, max_jitter=max_jitter)
        self.crawler_queue = CrawlerQueue()
        self.robots_parser = RobotsParser(respect_robots=respect_robots)
        self.visited_urls = set()
        self.processed_urls: dict[str, FetchResult] = {}
        self.failed_urls: dict[str, FetchResult] = {}
        self.start_urls: list[str] = []
        self.max_depth: int = 0
        self.same_domain_only: bool = False
        self.exclude_patern: Optional[str] = None
        self.include_patern: Optional[str] = None
        self.user_agent: Optional[str] = None
        self.start_time = asyncio.get_event_loop().time()

    async def _init_session(self):
        if self.session is not None:
            return

        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_read=5
        )
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def _process_robots_txt(self, url: str) -> bool:
        try:
            await self.robots_parser.fetch_robots(url)
            interval = self.robots_parser.get_crawl_delay(url, self.user_agent)
            if interval:
                self.rate_limiter.set_domain_interval(url, interval)

            is_allowed_url = self.robots_parser.can_fetch(url, self.user_agent)
            return is_allowed_url

        except Exception as e:
            logging.warning(f"Failed crawling robots.txt {e}")
            return True



    @asynccontextmanager
    async def _acquire(self, url: str):
        async with self.semaphore_manager.acquire(url):
            async with self.rate_limiter(url):
                yield

    async def fetch_url(self, url: str) -> FetchResult:
        error_args = {"url": url, "status": FetchResultStatus.FAILED}

        await self._init_session()
        can_fetch_url = await self._process_robots_txt(url)

        if not can_fetch_url:
            error_msg= f"Prohibited by robots.txt {url}"
            logging.warning(error_msg)
            return FetchResult(**error_args, error_type=ErrorTypes.BLOCKED_BY_ROBOTS, error=error_msg)

        async with self._acquire(url):
            logging.info(f"▶️ Start {url}")

            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()

                    logging.info(f"✅ Done {url}")
                    return FetchResult(url=url, status=FetchResultStatus.FINISHED, html=text)

            except aiohttp.ClientResponseError as e:
                error_msg = f"🚫 HTTP error {url}: {e.status}"
                logging.warning(error_msg)
                return FetchResult(**error_args, error_type=ErrorTypes.HTTP, error=error_msg)
            except asyncio.TimeoutError:
                error_msg= f"⏰ Timeout {url}"
                logging.warning(error_msg)
                return FetchResult(**error_args, error_type=ErrorTypes.TIMEOUT, error=error_msg)
            except aiohttp.ClientError as e:
                error_msg = f"❌ Network error {url}: {e}"
                logging.warning(error_msg)
                return FetchResult(**error_args, error_type=ErrorTypes.NETWORK, error=error_msg)

    async def fetch_urls(self, urls: list[str]) -> dict[str, FetchResult]:
        tasks = [asyncio.create_task(self.fetch_url(url)) for url in urls]

        results = await asyncio.gather(*tasks)

        return dict(zip(urls, results))

    async def fetch_and_parse_url(self, url: str) -> FetchResult:
        result = await self.fetch_url(url)

        if result.status == FetchResultStatus.FINISHED:
            result.parsed = self.html_parser.parse_html(html=result.html, url=url)

        return result

    async def fetch_and_parse_urls(self, urls: list[str]) -> dict[str, FetchResult]:
        tasks = [asyncio.create_task(self.fetch_and_parse_url(url)) for url in urls]

        results = await asyncio.gather(*tasks)

        return dict(zip(urls, results))

    @staticmethod
    def same_domain(url1: str, url2: str) -> bool:
        return urlparse(url1).hostname == urlparse(url2).hostname

    def should_visit_url(
            self,
            url: str,
            depth: int,
    ) -> bool:
        if depth > self.max_depth:
            return False

        if self.same_domain_only and not any(self.same_domain(s_url, url)  for s_url in self.start_urls):
            return False

        if self.exclude_patern is not None and re.match(self.exclude_patern, url):
            return False

        if self.include_patern is not None and not re.match(self.include_patern, url):
            return False

        return True

    async def worker(self):
        while True:
            data = await self.crawler_queue.get_next()

            url = data.url
            depth = data.depth
            priority = data.priority

            if url is None:
                return

            self.visited_urls.add(url)
            result = await self.fetch_and_parse_url(url)

            if result.status == FetchResultStatus.FINISHED:
                self.processed_urls[url] = result
                await self.crawler_queue.mark_processed(url)

                for link in result.parsed.links:
                    if self.should_visit_url(url=link, depth=depth+1):
                        await self.crawler_queue.add_url(link, priority=priority+1, depth=depth+1)

            elif result.status == FetchResultStatus.FAILED:
                self.failed_urls[url] = result
                await self.crawler_queue.mark_failed(url, result.error)

    def reset(self):
        self.visited_urls = set()
        self.processed_urls = {}
        self.failed_urls = {}
        self.start_time = asyncio.get_event_loop().time()
        self.start_urls = []
        self.max_depth = 0
        self.same_domain_only = False
        self.exclude_patern = None
        self.include_patern = None
        self.user_agent = None
        self.crawler_queue = CrawlerQueue()

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            max_depth: int = 3,
            same_domain_only: bool = False,
            exclude_patern: Optional[str] = None,
            include_patern: Optional[str] = None,
            user_agent: Optional[str] = None,
    ) -> CrawlResult:
        self.reset()
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.exclude_patern = exclude_patern
        self.include_patern = include_patern
        self.user_agent = user_agent

        for url in start_urls:
            await self.crawler_queue.add_url(url, priority=0)

        workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.max_concurrent)
        ]

        # Скорость обработки (страниц/сек)
        while len(self.processed_urls) < max_pages:
            await asyncio.sleep(0.2)
            print(print(self.crawler_queue.get_stats()))
            elapsed = asyncio.get_event_loop().time() - self.start_time
            speed = len(self.processed_urls) / elapsed if elapsed > 0 else 0
            avg_interval = 1/speed if speed > 0 else 0
            blocked_by_robots_count = len([
                res for url, res in self.failed_urls.items()
                if res.error_type == ErrorTypes.BLOCKED_BY_ROBOTS
            ])

            print(f"Speed: {speed:.2f} pages/sec")
            print(f"Average interval: {avg_interval:.2f} sec")
            print(f"Blocked by robots.txt: {blocked_by_robots_count}")

            if (len(self.crawler_queue.created) + len(self.crawler_queue.running)) == 0:
                break

        for w in workers:
            w.cancel()

        return CrawlResult(failed_urls=self.failed_urls, processed_urls=self.processed_urls)

    async def close(self):
        if self.session:
            await self.session.close()

        await self.robots_parser.close()
