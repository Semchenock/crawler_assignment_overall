import argparse
import asyncio
import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

import yaml

from src.async_crawler.async_crawler import AsyncCrawler
from src.async_crawler.models import CrawlResult
from src.data_storage.base import BaseDataStorage
from src.data_storage.csv_storage import CSVStorage
from src.data_storage.json_storage import JSONStorage
from src.data_storage.sq_lite_storage import SQLiteStorage
from src.logging_config import setup_logging


logger = logging.getLogger(__name__)


DEFAULT_CONFIG: dict[str, Any] = {
    "start_urls": [],
    "sitemap_urls": [],
    "crawler": {
        "max_concurrent": 10,
        "max_per_domain": 10,
        "min_interval": 0.0,
        "rate_limit": 1.0,
        "max_jitter": 0.0,
        "respect_robots": True,
        "user_agent": None,
    },
    "crawl": {
        "max_pages": 100,
        "max_depth": 3,
        "same_domain_only": False,
        "include_pattern": None,
        "exclude_pattern": None,
        "disable_speed_log": False,
    },
    "storage": {
        "type": None,
        "path": None,
        "batch_size": 100,
    },
    "stats": {
        "json": None,
        "html": None,
    },
    "logging": {
        "level": "INFO",
        "file": "crawler.log",
        "max_bytes": 1_000_000,
        "backup_count": 3,
    },
}


def _deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)

    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)

    return result


def _resolve_config_path(base_dir: Path, value: Optional[str]) -> Optional[str]:
    if not value:
        return value

    path = Path(value)
    if path.is_absolute():
        return value

    return str(base_dir / path)


def _resolve_relative_file_paths(config: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    storage = config.get("storage")
    if isinstance(storage, dict):
        for key in ("path", "file_path", "db_path"):
            storage[key] = _resolve_config_path(base_dir, storage.get(key))

    path_config = config.get("path")
    if isinstance(path_config, dict):
        path_config["stats_json"] = _resolve_config_path(base_dir, path_config.get("stats_json"))
        path_config["stats_html"] = _resolve_config_path(base_dir, path_config.get("stats_html"))

    stats = config.get("stats")
    if isinstance(stats, dict):
        stats["json"] = _resolve_config_path(base_dir, stats.get("json"))
        stats["html"] = _resolve_config_path(base_dir, stats.get("html"))

    logging_config = config.get("logging")
    if isinstance(logging_config, dict):
        logging_config["file"] = _resolve_config_path(base_dir, logging_config.get("file"))

    return config


def _load_config_file(filename: str) -> dict[str, Any]:
    path = Path(filename)

    with path.open("r", encoding="utf-8") as f:
        if path.suffix.lower() in {".yaml", ".yml"}:
            data = yaml.safe_load(f)
        elif path.suffix.lower() == ".json":
            data = json.load(f)
        else:
            raise ValueError("Config file must be YAML or JSON")

    return _resolve_relative_file_paths(data or {}, path.parent)


def load_config(filename: str) -> dict[str, Any]:
    return _load_config_file(filename)


def _normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = _deep_merge(DEFAULT_CONFIG, config)

    for key in (
        "max_concurrent",
        "max_per_domain",
        "min_interval",
        "max_jitter",
        "respect_robots",
        "user_agent",
    ):
        if key in config:
            normalized["crawler"][key] = config[key]

    if "rate_limit" in config:
        normalized["crawler"]["rate_limit"] = config["rate_limit"]
    if "requests_per_second" in config:
        normalized["crawler"]["rate_limit"] = config["requests_per_second"]

    for key in ("max_pages", "max_depth", "same_domain_only", "disable_speed_log"):
        if key in config:
            normalized["crawl"][key] = config[key]

    if "include_pattern" in config:
        normalized["crawl"]["include_pattern"] = config["include_pattern"]
    if "exclude_pattern" in config:
        normalized["crawl"]["exclude_pattern"] = config["exclude_pattern"]
    if "include_patern" in config:
        normalized["crawl"]["include_pattern"] = config["include_patern"]
    if "exclude_patern" in config:
        normalized["crawl"]["exclude_pattern"] = config["exclude_patern"]

    if "path" in config:
        paths = config["path"] or {}
        normalized["stats"]["json"] = paths.get("stats_json", normalized["stats"]["json"])
        normalized["stats"]["html"] = paths.get("stats_html", normalized["stats"]["html"])

    storage = normalized.get("storage") or {}
    if "file_path" in storage and not storage.get("path"):
        storage["path"] = storage["file_path"]
    if "db_path" in storage and not storage.get("path"):
        storage["path"] = storage["db_path"]
    normalized["storage"] = storage

    return normalized


def _infer_storage_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return "CSV"
    if suffix in {".db", ".sqlite", ".sqlite3"}:
        return "SQLite"
    return "JSON"


class AdvancedCrawler:
    """Final integration wrapper for config, storage, stats and logging."""

    def __init__(self, config: Optional[dict[str, Any]] = None, **overrides: Any):
        merged = _deep_merge(config or {}, overrides)
        self.config = _normalize_config(merged)

        logging_config = self.config["logging"]
        setup_logging(
            level=logging_config.get("level", "INFO"),
            log_file=logging_config.get("file", "crawler.log"),
            max_bytes=logging_config.get("max_bytes", 1_000_000),
            backup_count=logging_config.get("backup_count", 3),
        )

        self.storage = self._build_storage(self.config["storage"])
        crawler_config = self.config["crawler"]
        self._crawler = AsyncCrawler(
            max_concurrent=crawler_config["max_concurrent"],
            max_per_domain=crawler_config["max_per_domain"],
            min_interval=crawler_config["min_interval"],
            requests_per_second=crawler_config["rate_limit"],
            respect_robots=crawler_config["respect_robots"],
            max_jitter=crawler_config["max_jitter"],
            storage=self.storage,
            user_agent=crawler_config["user_agent"],
        )
        self.result: Optional[CrawlResult] = None

    @classmethod
    def from_config(cls, filename: str, **overrides: Any) -> "AdvancedCrawler":
        return cls(config=_load_config_file(filename), **overrides)

    @staticmethod
    def _build_storage(storage_config: dict[str, Any]) -> Optional[BaseDataStorage]:
        storage_type = (storage_config.get("type") or "").lower()
        if not storage_type or storage_type == "none":
            return None

        path = storage_config.get("path")
        batch_size = storage_config.get("batch_size", 100)

        if storage_type == "json":
            return JSONStorage(file_path=path or "data.jsonl")
        if storage_type == "csv":
            return CSVStorage(file_path=path or "data.csv")
        if storage_type in {"sqlite", "sql_lite", "sq_lite"}:
            return SQLiteStorage(db_path=path or "data.db", batch_size=batch_size)

        raise ValueError(f"Unknown storage type: {storage_config.get('type')}")

    @property
    def stats(self):
        return self._crawler.stats

    async def crawl(
        self,
        start_urls: Optional[list[str]] = None,
        max_pages: Optional[int] = None,
        max_depth: Optional[int] = None,
        same_domain_only: Optional[bool] = None,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
        sitemap_urls: Optional[list[str]] = None,
    ) -> CrawlResult:
        crawl_config = self.config["crawl"]

        urls = start_urls if start_urls is not None else self.config["start_urls"]
        sitemaps = sitemap_urls if sitemap_urls is not None else self.config["sitemap_urls"]

        if not urls and not sitemaps:
            raise ValueError("At least one start URL or sitemap URL is required")

        self.result = await self._crawler.crawl(
            start_urls=urls,
            max_pages=max_pages if max_pages is not None else crawl_config["max_pages"],
            max_depth=max_depth if max_depth is not None else crawl_config["max_depth"],
            same_domain_only=(
                same_domain_only
                if same_domain_only is not None
                else crawl_config["same_domain_only"]
            ),
            exclude_patern=(
                exclude_pattern
                if exclude_pattern is not None
                else crawl_config["exclude_pattern"]
            ),
            include_patern=(
                include_pattern
                if include_pattern is not None
                else crawl_config["include_pattern"]
            ),
            disable_speed_log=crawl_config["disable_speed_log"],
            sitemap_urls=sitemaps,
        )
        return self.result

    def get_stats(self) -> dict[str, Any]:
        return self.stats.get_stats()

    def export_to_json(self, filename: str) -> None:
        self.stats.export_to_json(filename)

    def export_to_html_report(self, filename: str) -> None:
        self.stats.export_to_html_report(filename)

    async def close(self) -> None:
        await self._crawler.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Asynchronous advanced web crawler")
    parser.add_argument("--urls", nargs="+", help="Start URLs for crawling")
    parser.add_argument("--sitemaps", nargs="+", help="Sitemap URLs for crawling")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages")
    parser.add_argument("--max-depth", type=int, help="Maximum crawl depth")
    parser.add_argument("--output", help="File for crawled data")
    parser.add_argument("--config", help="YAML or JSON configuration file")
    parser.add_argument(
        "--respect-robots",
        action="store_true",
        default=None,
        help="Respect robots.txt rules",
    )
    parser.add_argument("--rate-limit", type=float, help="Requests per second")
    parser.add_argument("--stats-json", help="Export statistics to JSON")
    parser.add_argument("--stats-html", help="Export statistics to HTML")
    parser.add_argument("--log-file", help="Crawler log file")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    return parser


async def _run_cli(args: argparse.Namespace) -> int:
    config = _load_config_file(args.config) if args.config else {}
    overrides: dict[str, Any] = {}

    if args.urls:
        overrides["start_urls"] = args.urls
    if args.sitemaps:
        overrides["sitemap_urls"] = args.sitemaps
    if args.max_pages is not None:
        overrides.setdefault("crawl", {})["max_pages"] = args.max_pages
    if args.max_depth is not None:
        overrides.setdefault("crawl", {})["max_depth"] = args.max_depth
    if args.respect_robots is not None:
        overrides.setdefault("crawler", {})["respect_robots"] = args.respect_robots
    if args.rate_limit is not None:
        overrides.setdefault("crawler", {})["rate_limit"] = args.rate_limit
    if args.output:
        overrides["storage"] = {
            "type": _infer_storage_type(args.output),
            "path": args.output,
        }
    if args.stats_json:
        overrides.setdefault("stats", {})["json"] = args.stats_json
    if args.stats_html:
        overrides.setdefault("stats", {})["html"] = args.stats_html
    if args.log_file:
        overrides.setdefault("logging", {})["file"] = args.log_file
    if args.log_level:
        overrides.setdefault("logging", {})["level"] = args.log_level

    crawler = AdvancedCrawler(config=config, **overrides)

    try:
        await crawler.crawl()

        stats = crawler.get_stats()
        logger.info(
            "Crawling finished: total=%s successful=%s failed=%s",
            stats["total_pages"],
            stats["successful"],
            stats["failed"],
        )

        stats_config = crawler.config["stats"]
        if stats_config.get("json"):
            crawler.export_to_json(stats_config["json"])
        if stats_config.get("html"):
            crawler.export_to_html_report(stats_config["html"])
    finally:
        await crawler.close()

    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not args.config and not args.urls and not args.sitemaps:
        parser.error("Provide --urls, --sitemaps or --config")

    return asyncio.run(_run_cli(args))
