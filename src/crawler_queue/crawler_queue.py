import logging
import asyncio
import itertools

from src.crawler_queue.models import QueueStats, QueueData

class CrawlerQueue:
    def __init__(self):
        self.queue: asyncio.PriorityQueue[tuple[int, int, QueueData]] = asyncio.PriorityQueue()
        self.counter = itertools.count()
        self.lock = asyncio.Lock()
        self.seen = set()
        self.created = set()
        self.running = set()
        self.failed = set()
        self.finished = set()

    # 0 - highest priority
    async def add_url(self, url: str, priority: int = 0, depth: int = 0):
        async with self.lock:
            if url in self.seen:
                logging.warning(f"Duplicate url: {url}")
                return

            self.seen.add(url)
            self.created.add(url)

        data = QueueData(url=url, depth=depth, priority=priority)

        await self.queue.put((priority, next(self.counter), data))

    async def get_next(self) -> QueueData | None:
        _, _, data = await self.queue.get()
        url = data.url

        async with self.lock:
            self.created.remove(url)
            self.running.add(url)
            return data

    async def mark_processed(self, url: str):
        async with self.lock:
            if url not in self.running:
                logging.warning(f"No running task with url: {url}")
                return

            self.running.remove(url)
            self.finished.add(url)

    async def mark_failed(self, url: str, error: str):
        async with self.lock:
            if url not in self.running:
                logging.warning(f"No task with url: {url}")
                return

            self.running.remove(url)
            self.failed.add(url)

    def get_stats(self) -> QueueStats:
        return QueueStats(
            total=len(self.seen),
            created=len(self.created),
            running=len(self.running),
            failed=len(self.failed),
            finished=len(self.finished),
        )
