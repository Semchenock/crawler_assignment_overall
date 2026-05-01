import asyncio

from src.async_crawler.async_crawler import AsyncCrawler
from src.data_storage.csv_storage import CSVStorage
from src.data_storage.json_storage import JSONStorage
from src.data_storage.sq_lite_storage import SQLiteStorage

STORAGE_TYPE="JSON"

async def main():
    start_urls = [
        "https://httpbin.org/",
    ]

    json_storage = JSONStorage()
    csv_storage = CSVStorage()
    sqlite_storage = SQLiteStorage(batch_size=10)

    storage = None

    match STORAGE_TYPE:
        case "JSON":
            storage=json_storage
        case "CSV":
            storage=csv_storage
        case "SQLite":
            storage=sqlite_storage
        case _:
            storage = None

    print(storage)

    crawler = AsyncCrawler(
        max_concurrent=3,
        max_per_domain=2,
        min_interval=1.0,
        max_jitter=0.5,
        respect_robots=True,
        storage=storage,
        user_agent="MyCrawlerBot/1.0",
    )

    print("🚀 Start crawling...\n")

    await crawler.crawl(
        start_urls=start_urls,
        max_pages=100,
        max_depth=20,
        same_domain_only=True,
        disable_speed_log=True
    )

    print("✅ Done crawling...\n")

    print(crawler.stats.get_stats())

    crawler.stats.export_to_json()
    crawler.stats.export_to_html_report()

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
