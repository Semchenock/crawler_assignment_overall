import asyncio
import logging
import xml.etree.ElementTree as ET
from typing import Optional
from xml.etree.ElementTree import Element

import aiohttp

from src.sitemap_parser.model import LinkData


logger = logging.getLogger(__name__)


class SitemapParser:
    def __init__(self, user_agent: Optional[str] = None, max_depth: int = 3):
        self.session = None
        self.user_agent = user_agent
        self.max_depth = max_depth

    async def _init_session(self):
        if self.session is not None:
            return

        timeout = aiohttp.ClientTimeout(total=10, connect=3, sock_read=5)
        headers = {}

        if self.user_agent is not None:
            headers["User-Agent"] = self.user_agent

        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    @staticmethod
    def _unique(urls: list[str]) -> list[str]:
        seen = set()
        result = []

        for url in urls:
            if url in seen:
                continue

            seen.add(url)
            result.append(url)

        return result

    async def parse_response(self, text: str, depth: int) -> list[str]:
        root = ET.fromstring(text)

        if root.tag.endswith("sitemapindex"):
            return await self.parse_index_sitemap(root, depth)
        if root.tag.endswith("urlset"):
            return self.parse_sitemap(root)

        logger.warning("Unknown sitemap root tag: %s", root.tag)
        return []

    async def parse_index_sitemap(self, root: Element, depth: int) -> list[str]:
        if not root.tag.endswith("sitemapindex"):
            return []

        tasks = []

        for elem in root.iter():
            if not elem.tag.endswith("loc") or not elem.text:
                continue

            tasks.append(asyncio.create_task(self.fetch_sitemap(elem.text.strip(), depth + 1)))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        urls: list[str] = []

        for result in results:
            if isinstance(result, Exception):
                logger.warning("Failed to parse nested sitemap: %s", result)
                continue

            urls.extend(result)

        return self._unique(urls)

    @staticmethod
    def parse_sitemap(root: Element) -> list[str]:
        if not root.tag.endswith("urlset"):
            return []

        return [
            elem.text.strip()
            for elem in root.iter()
            if elem.tag.endswith("loc") and elem.text
        ]

    async def fetch_sitemap(self, sitemap_url: str, depth: int = 0) -> list[str]:
        if depth > self.max_depth:
            return []

        await self._init_session()

        try:
            async with self.session.get(sitemap_url) as response:
                response.raise_for_status()
                text = await response.text()
                return await self.parse_response(text, depth)
        except Exception as e:
            logger.warning("Failed to fetch sitemap %s: %s", sitemap_url, e)
            return []

    async def fetch_sitemap_links(self, sitemap_url: str, depth: int = 0) -> list[LinkData]:
        urls = await self.fetch_sitemap(sitemap_url, depth)
        return [LinkData(link=url, priority=depth) for url in urls]

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
