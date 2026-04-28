import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime

import aiohttp
import logging
from typing import Optional
from urllib.parse import urlparse
import re

from src.async_crawler.models import FetchResult, CrawlResult, BlockedByRobots
from src.async_crawler.enums import FetchResultStatus, ErrorTypes
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
from src.timeout_manager.timeout_manager import TimeoutConfig
from src.data_storage.model import DataItem

logging.basicConfig(level=logging.INFO)

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
        self.error_log = ErrorLog()
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
        self.start_time = asyncio.get_event_loop().time()

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
        try:
            await self.robots_parser.fetch_robots(url)
            interval = self.robots_parser.get_crawl_delay(url, self.user_agent)
            domain = urlparse(url).hostname
            if interval:
                self.rate_limiter.set_domain_interval(domain, interval)

            is_allowed_url = self.robots_parser.can_fetch(url, self.user_agent)
            if not is_allowed_url:
                raise BlockedByRobots()

        except Exception as e:
            logging.warning(f"Failed crawling robots.txt {e}")
            raise

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
            logging.warning(f"🚫 Blocked by robots.txt {url}")
            return ""

        async with self._acquire(url):
            logging.info(f"▶️ Start {url}")

            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    logging.info(f"✅ Done {url}")
                    return text

            except aiohttp.ClientResponseError as e:
                logging.warning(f"🚫 HTTP error {url}: {e.status}")

            except asyncio.TimeoutError:
                logging.warning(f"⏰ Timeout {url}")

            except aiohttp.ClientError as e:
                logging.warning(f"❌ Network error {url}: {e}")

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
        except BlockedByRobots:
            raise PermanentError(error_type=ErrorTypes.BLOCKED_BY_ROBOTS)

        async with self._acquire(url):
            try:
                async with self.session.get(url, timeout=timeout) as response:
                    response.raise_for_status()
                    response_content_type = response.headers.get("Content-Type")
                    status_code = response.status
                    text = await response.text()
                    return FetchResult(
                        url=url,
                        status=FetchResultStatus.FINISHED,
                        html=text,
                        content_type=response_content_type,
                        status_code=status_code
                    )
            except aiohttp.ClientResponseError as e:
                error = STATUS_CODES_TO_ERROR.get(e.status, PermanentError)
                error_type = STATUS_CODES_TO_ERROR_TYPES.get(e.status, ErrorTypes.UNKNOWN)
                raise error(error_type=error_type)
            except asyncio.TimeoutError:
                raise TransientError(error_type=ErrorTypes.TIMEOUT)
            except aiohttp.ClientError:
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
            print(f"Start crawling {url}")
            result = await self.fetch_and_parse_url(url)

            if result.status == FetchResultStatus.FINISHED:
                print(f"Finished crawling {url}")
                self.processed_urls[url] = result
                await self.crawler_queue.mark_processed(url)

                for link in result.parsed.links:
                    if self.should_visit_url(url=link, depth=depth+1):
                        await self.crawler_queue.add_url(link, priority=priority+1, depth=depth+1)

                if self.storage:
                    await self._save_to_storage(result)

            elif result.status == FetchResultStatus.FAILED:
                print(f"Failed crawling {url}")
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
        self.crawler_queue = CrawlerQueue()

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            max_depth: int = 3,
            same_domain_only: bool = False,
            exclude_patern: Optional[str] = None,
            include_patern: Optional[str] = None,
            disable_speed_log: Optional[bool] = False,
    ) -> CrawlResult:
        self.reset()
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.exclude_patern = exclude_patern
        self.include_patern = include_patern

        for url in start_urls:
            await self.crawler_queue.add_url(url, priority=0)

        workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.max_concurrent)
        ]

        # Скорость обработки (страниц/сек)
        while len(self.processed_urls) < max_pages:
            await asyncio.sleep(0.2)
            if not disable_speed_log:
                print(print(self.crawler_queue.get_stats()))
            elapsed = asyncio.get_event_loop().time() - self.start_time
            speed = len(self.processed_urls) / elapsed if elapsed > 0 else 0
            avg_interval = 1/speed if speed > 0 else 0
            blocked_by_robots_count = len([
                res for url, res in self.failed_urls.items()
                if res.error_type == ErrorTypes.BLOCKED_BY_ROBOTS
            ])

            if not disable_speed_log:
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

        if self.storage:
            await self.storage.close()

        await self.robots_parser.close()
