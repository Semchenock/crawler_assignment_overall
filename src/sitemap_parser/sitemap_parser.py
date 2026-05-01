from typing import Optional

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element

import aiohttp
import asyncio

from src.sitemap_parser.model import LinkData


class SitemapParser:
    def __init__(self, user_agent: Optional[str] = None):
        self.session = None
        self.user_agent = user_agent
        self.max_depth = 3

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

    async def parse_response(self, text: str, depth: int) -> list[LinkData]:
        root = ET.fromstring(text)
        if root.tag.endswith("sitemapindex"):
            return await self.parse_index_sitemap(root, depth)
        elif root.tag.endswith("urlset"):
            return self.parse_sitemap(root, depth)
        else:
            return []

    async def parse_index_sitemap(self, root: Element[str], depth: int) -> list[LinkData]:
        if not root.tag.endswith("sitemapindex"):
           return []

        tasks = []

        for elem in root.iter():
            if not elem.tag.endswith("loc"):
                continue

            tasks.append(asyncio.create_task(self.fetch_sitemap(elem.text, depth+1)))

        results = await asyncio.gather(*tasks)

        return [item for sublist in results for item in sublist]

    @staticmethod
    def parse_sitemap(root: Element[str], depth: int) -> list[LinkData]:
        if not root.tag.endswith("urlset"):
            return []

        return [LinkData(link=elem.text, priority=depth) for elem in root.iter() if elem.tag.endswith("loc")]


    async def fetch_sitemap(self, sitemap_url: str, depth:int=0) -> list[LinkData]:
        if depth >= self.max_depth:
            return []

        await self._init_session()
        try:
            async with self.session.get(sitemap_url) as response:
                text = await response.text()
                return await self.parse_response(text, depth)
        except Exception:
            return []


    async def close(self):
        if self.session:
            await self.session.close()
