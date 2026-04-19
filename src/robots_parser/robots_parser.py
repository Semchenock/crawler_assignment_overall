from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

import aiohttp

from src.robots_parser.constants import DEFAULT_AGENT


class RobotsParser:
    def __init__(self,respect_robots: bool = True):
        self.session = None
        self.respect_robots = respect_robots
        self.robots_by_domain = {}

    async def _init_session(self):
        if self.session is not None:
            return

        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_read=5
        )
        self.session = aiohttp.ClientSession(timeout=timeout)

    @staticmethod
    def parse_robots_by_agents(text: str) -> dict:
        result = defaultdict(lambda: {
            "allow": [],
            "disallow": [],
            "crawl_delay": None
        })

        current_agents = []

        for line in text.splitlines():
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if line.lower().startswith("user-agent"):
                agent = line.split(":", 1)[1].strip().lower()
                current_agents = [agent]
                continue

            if line.lower().startswith("allow"):
                path = line.split(":", 1)[1].strip()
                for a in current_agents:
                    result[a]["allow"].append(path)

            elif line.lower().startswith("disallow"):
                path = line.split(":", 1)[1].strip()
                for a in current_agents:
                    result[a]["disallow"].append(path)

            elif line.lower().startswith("crawl-delay"):
                delay = line.split(":", 1)[1].strip()
                try:
                    for a in current_agents:
                        result[a]["crawl_delay"] = float(delay)
                except ValueError:
                    pass

        return dict(result)

    async def fetch_robots(self, url: str) -> dict:
        domain = urlparse(url).hostname
        robots_data_for_domain = self.robots_by_domain.get(domain)

        if robots_data_for_domain is not None:
           return robots_data_for_domain

        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        text=""

        await self._init_session()
        try:
            async with self.session.get(robots_url) as response:
                text = await response.text()
        except Exception as e:
            pass

        self.robots_by_domain[domain] = self.parse_robots_by_agents(text)

        return self.robots_by_domain.get(domain)

    def _get_robots_data(self, url: str, user_agent: str = DEFAULT_AGENT) -> Optional[dict]:
        domain = urlparse(url).hostname
        robots_data_for_domain = self.robots_by_domain.get(domain)
        robots_data = robots_data_for_domain.get(user_agent)

        if robots_data is None:
            robots_data = robots_data_for_domain.get(DEFAULT_AGENT)

        return robots_data

    def can_fetch(self, url: str, user_agent: str = DEFAULT_AGENT) -> bool:
        if not self.respect_robots:
            return True

        robots_data = self._get_robots_data(url, user_agent)

        if robots_data is None:
            return True

        path = urlparse(url).path

        allow_rules = robots_data.get("allow", [])
        disallow_rules = robots_data.get("disallow", [])

        matched_rule = None
        matched_type = None

        for rule in disallow_rules:
            if path.startswith(rule):
                if matched_rule is None or len(rule) > len(matched_rule):
                    matched_rule = rule
                    matched_type = "disallow"

        for rule in allow_rules:
            if path.startswith(rule):
                if matched_rule is None or len(rule) > len(matched_rule):
                    matched_rule = rule
                    matched_type = "allow"

        if matched_type == "disallow":
            return False

        return True

    def get_crawl_delay(self, url: str, user_agent: str = DEFAULT_AGENT) -> float:
        if not self.respect_robots:
            return 0

        robots_data = self._get_robots_data(url, user_agent)

        if robots_data is None:
            return 0

        return robots_data.get("crawl_delay", 0)


    async def close(self):
        if self.session:
            await self.session.close()
