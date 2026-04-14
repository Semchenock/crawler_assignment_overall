import asyncio
from contextlib import asynccontextmanager
from urllib.parse import urlparse

class SemaphoreManager:
    def __init__(self, max_global:int=10, max_per_domain:int=10):
        self.global_semaphore:asyncio.Semaphore = asyncio.Semaphore(max_global)
        self.max_per_domain:int = max_per_domain
        self.domain_semaphores: dict[str, asyncio.Semaphore] = {}

    def get_domain_semaphore(self, url:str) -> asyncio.Semaphore:
        domain = urlparse(url).hostname

        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.max_per_domain)

        return self.domain_semaphores[domain]

    @asynccontextmanager
    async def acquire(self, url: str):
        domain_sem = self.get_domain_semaphore(url)

        async with self.global_semaphore:
            async with domain_sem:
                yield