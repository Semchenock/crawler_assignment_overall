import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime

import aiohttp
import logging
import time
from typing import Optional
from urllib.parse import urlparse
import re

from src.async_crawler.models import FetchResult, CrawlResult, BlockedByRobots
from src.async_crawler.enums import FetchResultStatus, ErrorTypes
from src.crawler_stats.crawler_stats import CrawlerStats
from src.data_storage.base import BaseDataStorage
from src.error_log.error_log import ErrorLog
from src.html_parser.html_parser import HtmlParser
from src.crawler_queue.crawler_queue import CrawlerQueue
from src.rate_limiter.rate_limiter import RateLimiter
from src.retry_strategy.constants import STATUS_CODES_TO_ERROR, CUSTOM_ERRORS, STATUS_CODES_TO_ERROR_TYPES
from src.retry_strategy.models import PermanentError, TransientError, NetworkError, ParseError, RetryCountExceeded
from src.retry_strategy.retry_stategy import RetryStrategy
from src.robots_parser.robots_parser import RobotsParser
from src.semaphore_manager.semaphore_manager import SemaphoreManager
from src.sitemap_parser.sitemap_parser import SitemapParser
from src.timeout_manager.timeout_manager import TimeoutConfig
from src.data_storage.model import DataItem

logger = logging.getLogger(__name__)

class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_per_domain: int = 10,
        min_interval: Optional[float] = 0.0,
        requests_per_second: Optional[float] = 1.0,
        respect_robots = True,
        max_jitter: float = 0.0,
        storage: Optional[BaseDataStorage] = None,
        user_agent: Optional[str] = None
    ):
        self.user_agent: Optional[str] = user_agent
        self.max_concurrent = max_concurrent
        self.semaphore_manager = SemaphoreManager(max_global=max_concurrent, max_per_domain=max_per_domain)
        self.session = None
        self.html_parser = HtmlParser()
        self.rate_limiter = RateLimiter(
            per_domain=respect_robots,
            min_interval=min_interval,
            max_jitter=max_jitter,
            requests_per_second=requests_per_second
        )
        self.crawler_queue = CrawlerQueue()
        self.robots_parser = RobotsParser(respect_robots=respect_robots, user_agent=user_agent)
        self.sitemap_parser = SitemapParser(user_agent=user_agent)
        self.error_log = ErrorLog()
        self.stats = CrawlerStats()
        self.retry_strategy = RetryStrategy(error_log=self.error_log)
        self.timeout_config = TimeoutConfig()
        self.storage = storage
        self.visited_urls = set()
        self.processed_urls: dict[str, FetchResult] = {}
        self.failed_urls: dict[str, FetchResult] = {}
        self.start_urls: list[str] = []
        self.max_depth: int = 0
        self.same_domain_only: bool = False
        self.exclude_patern: Optional[str] = None
        self.include_patern: Optional[str] = None
        self.processed_sitemaps: list[str] = []
        self.max_pages: int = 0
        self.start_time = time.monotonic()

    async def _init_session(self):
        if self.session is not None:
            return

        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_read=5
        )
        headers = {}

        if self.user_agent is not None:
            headers["User-Agent"] = self.user_agent

        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def _process_robots_txt(self, url: str):
        if not self.robots_parser.respect_robots:
            print('disrespect')
            return

        try:
            await self.robots_parser.fetch_robots(url)

            sitemap_url = self.robots_parser.get_sitemap_url(url, self.user_agent)
            if sitemap_url is not None:
                await self._process_sitemap(sitemap_url)

            interval = self.robots_parser.get_crawl_delay(url, self.user_agent)
            domain = urlparse(url).hostname
            if interval:
                self.rate_limiter.set_domain_interval(domain, interval)

            is_allowed_url = self.robots_parser.can_fetch(url, self.user_agent)
            if not is_allowed_url:
                raise BlockedByRobots()

        except BlockedByRobots:
            raise
        except Exception as e:
            logger.warning("Failed crawling robots.txt: %s", e)
            raise

    def _completed_count(self) -> int:
        return len(self.processed_urls) + len(self.failed_urls)

    def _scheduled_count(self) -> int:
        return (
            self._completed_count()
            + len(self.crawler_queue.created)
            + len(self.crawler_queue.running)
        )

    def _has_capacity(self) -> bool:
        return self.max_pages <= 0 or self._scheduled_count() < self.max_pages

    async def _process_sitemap(self, url: str):
        if url in self.processed_sitemaps:
            return

        self.processed_sitemaps.append(url)
        links = await self.sitemap_parser.fetch_sitemap(url)

        for link in links:
            if self._has_capacity() and self.should_visit_url(url=link, depth=0):
                await self.crawler_queue.add_url(link, priority=0, depth=0)

    async def _save_to_storage(self, data: FetchResult):
        if data.status == FetchResultStatus.FAILED or data.parsed is None:
            return

        parsed_data = data.parsed

        data_item = DataItem(
            url=parsed_data.url,
            text=parsed_data.text,
            title=parsed_data.title if parsed_data.title is not None else "",
            links=parsed_data.links,
            metadata=asdict(parsed_data.metadata),
            crawled_at=datetime.now(),
            status_code=data.status_code if data.status_code is not None else 0,
            content_type=data.content_type if data.content_type is not None else "",
        )

        await self.storage.save(data_item)


    @asynccontextmanager
    async def _acquire(self, url: str):
        async with self.semaphore_manager.acquire(url):
            async with self.rate_limiter(url):
                yield

    async def fetch_url(self, url: str) -> str:
        await self._init_session()

        try:
            await self._process_robots_txt(url)
        except BlockedByRobots:
            logger.warning("Blocked by robots.txt: %s", url)
            return ""

        async with self._acquire(url):
            logger.info("Start fetching %s", url)

            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    logger.info("Finished fetching %s", url)
                    return text

            except aiohttp.ClientResponseError as e:
                logger.warning("HTTP error %s: %s", url, e.status)

            except asyncio.TimeoutError:
                logger.warning("Timeout: %s", url)

            except aiohttp.ClientError as e:
                logger.warning("Network error %s: %s", url, e)

            return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [asyncio.create_task(self.fetch_url(url)) for url in urls]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        return dict(zip(urls, results))

    async def _fetch_raw(self, url: str, timeout_cfg: dict) -> FetchResult:
        timeout = aiohttp.ClientTimeout(
            sock_connect=timeout_cfg["connect"],
            sock_read=timeout_cfg["read"],
            total=timeout_cfg["total"],
        )

        await self._init_session()

        try:
            await self._process_robots_txt(url)
            print("process robots.txt", url)
        except BlockedByRobots:
            self.stats.handle_failed_request()
            raise PermanentError(error_type=ErrorTypes.BLOCKED_BY_ROBOTS)

        async with self._acquire(url):
            try:
                async with self.session.get(url, timeout=timeout) as response:
                    response.raise_for_status()
                    response_content_type = response.headers.get("Content-Type")
                    status_code = response.status
                    text = await response.text()
                    self.stats.handle_successful_request(status_code)
                    return FetchResult(
                        url=url,
                        status=FetchResultStatus.FINISHED,
                        html=text,
                        content_type=response_content_type,
                        status_code=status_code
                    )
            except aiohttp.ClientResponseError as e:
                status_code = e.status
                error = STATUS_CODES_TO_ERROR.get(status_code, PermanentError)
                error_type = STATUS_CODES_TO_ERROR_TYPES.get(status_code, ErrorTypes.UNKNOWN)
                self.stats.handle_failed_request(status_code)
                raise error(error_type=error_type)
            except asyncio.TimeoutError:
                self.stats.handle_failed_request()
                raise TransientError(error_type=ErrorTypes.TIMEOUT)
            except aiohttp.ClientError:
                self.stats.handle_failed_request()
                raise NetworkError(error_type=ErrorTypes.NETWORK)


    async def _fetch_and_parse_inner(self, url: str, timeout_cfg: dict) -> FetchResult:
        result = await self._fetch_raw(url, timeout_cfg)

        try:
            parsed = self.html_parser.parse_html(html=result.html, url=url)
            result.parsed = parsed
        except Exception:
            raise ParseError(error_type=ErrorTypes.PARSE)

        return result

    async def _fetch_and_parse_url_raw(self, url: str, attempt: int = 0) -> FetchResult:
        timeout_cfg = self.timeout_config.for_attempt(attempt)

        try:
            result = await asyncio.wait_for(
                self._fetch_and_parse_inner(url, timeout_cfg),
                timeout=timeout_cfg["total"]
            )
            return result
        except asyncio.TimeoutError:
            self.stats.handle_failed_request()
            raise TransientError(error_type=ErrorTypes.TIMEOUT)

    async def fetch_and_parse_url(self, url: str) -> FetchResult:
        try:
            result = await self.retry_strategy.execute_with_retry(self._fetch_and_parse_url_raw, url)
            self.error_log.mark_finished(url)
            return result
        except RetryCountExceeded as e:
            self.error_log.mark_failed(url)
            return FetchResult(url=url, status=FetchResultStatus.FAILED, error=f"{e.__class__.__name__} in url {url}", error_type=ErrorTypes.RETRY_EXCEEDED)
        except CUSTOM_ERRORS as e:
            self.error_log.mark_failed(url)
            return FetchResult(url=url, status=FetchResultStatus.FAILED, error=f"{e.__class__.__name__} in url {url}", error_type=e.error_type)
        except Exception as e:
            self.error_log.mark_failed(url)
            return FetchResult(url=url, status=FetchResultStatus.FAILED, error=f"{e.__class__.__name__} in url {url}", error_type=ErrorTypes.UNKNOWN)

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

        if self.same_domain_only and self.start_urls and not any(self.same_domain(s_url, url)  for s_url in self.start_urls):
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
            marked_done = False

            if url is None:
                return

            try:
                self.visited_urls.add(url)
                logger.info("Start crawling %s", url)
                result = await self.fetch_and_parse_url(url)

                if result.status == FetchResultStatus.FINISHED:
                    logger.info("Finished crawling %s", url)
                    self.processed_urls[url] = result
                    self.stats.handle_processed_url(url)
                    await self.crawler_queue.mark_processed(url)
                    marked_done = True

                    for link in result.parsed.links:
                        if not self._has_capacity():
                            break

                        if self.should_visit_url(url=link, depth=depth+1):
                            await self.crawler_queue.add_url(link, priority=priority+1, depth=depth+1)

                    if self.storage:
                        try:
                            await self._save_to_storage(result)
                        except Exception as e:
                            logger.exception("Failed to save crawled data for %s: %s", url, e)

                elif result.status == FetchResultStatus.FAILED:
                    logger.warning("Failed crawling %s: %s", url, result.error)
                    self.failed_urls[url] = result
                    await self.crawler_queue.mark_failed(url, result.error)
                    marked_done = True

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("Unexpected worker error for %s: %s", url, e)
                self.failed_urls[url] = FetchResult(
                    url=url,
                    status=FetchResultStatus.FAILED,
                    error=f"{e.__class__.__name__} in url {url}",
                    error_type=ErrorTypes.UNKNOWN,
                )

                if not marked_done:
                    await self.crawler_queue.mark_failed(url, str(e))

    def reset(self):
        self.visited_urls = set()
        self.processed_urls = {}
        self.failed_urls = {}
        self.start_time = time.monotonic()
        self.start_urls = []
        self.max_depth = 0
        self.same_domain_only = False
        self.exclude_patern = None
        self.include_patern = None
        self.crawler_queue = CrawlerQueue()
        self.processed_sitemaps = []
        self.max_pages = 0

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            max_depth: int = 3,
            same_domain_only: bool = False,
            exclude_patern: Optional[str] = None,
            include_patern: Optional[str] = None,
            disable_speed_log: Optional[bool] = False,
            sitemap_urls: Optional[list[str]] = None,
    ) -> CrawlResult:
        self.reset()
        self.stats.start_crawling()
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.same_domain_only = same_domain_only
        self.exclude_patern = exclude_patern
        self.include_patern = include_patern

        for url in start_urls:
            if not self._has_capacity():
                break

            await self.crawler_queue.add_url(url, priority=0)

        for sitemap_url in sitemap_urls or []:
            await self._process_sitemap(sitemap_url)

        workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.max_concurrent)
        ]

        last_progress_log = 0.0

        while self._completed_count() < max_pages:
            await asyncio.sleep(0.2)
            queue_stats = self.crawler_queue.get_stats()
            now = time.monotonic()
            elapsed = now - self.start_time
            completed = self._completed_count()
            speed = completed / elapsed if elapsed > 0 else 0
            progress = min(completed / max_pages * 100, 100) if max_pages > 0 else 100
            remaining = max(max_pages - completed, 0)
            eta = remaining / speed if speed > 0 else None
            blocked_by_robots_count = len([
                res for url, res in self.failed_urls.items()
                if res.error_type == ErrorTypes.BLOCKED_BY_ROBOTS
            ])

            if not disable_speed_log and now - last_progress_log >= 1.0:
                last_progress_log = now
                eta_text = f"{eta:.1f}s" if eta is not None else "unknown"
                logger.info(
                    "Progress %.1f%% (%s/%s), speed %.2f pages/sec, eta %s, queued=%s, active=%s, failed=%s, blocked_by_robots=%s",
                    progress,
                    completed,
                    max_pages,
                    speed,
                    eta_text,
                    queue_stats.created,
                    queue_stats.running,
                    queue_stats.failed,
                    blocked_by_robots_count,
                )

            if (len(self.crawler_queue.created) + len(self.crawler_queue.running)) == 0:
                break

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        self.stats.end_crawling()

        return CrawlResult(failed_urls=self.failed_urls, processed_urls=self.processed_urls)

    async def close(self):
        if self.session:
            await self.session.close()

        if self.storage:
            await self.storage.close()

        await self.robots_parser.close()
        await self.sitemap_parser.close()
