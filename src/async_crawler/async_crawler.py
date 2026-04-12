import asyncio
import aiohttp
import logging

logging.basicConfig(level=logging.INFO)

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def _init_session(self):
        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_read=5
        )
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def fetch_url(self, url: str) -> str:
        async with self.semaphore:
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
        if self.session is None:
            await self._init_session()

        tasks = [asyncio.create_task(self.fetch_url(url)) for url in urls]

        results = await asyncio.gather(*tasks)

        return dict(zip(urls, results))

    async def close(self):
        if self.session:
            await self.session.close()
