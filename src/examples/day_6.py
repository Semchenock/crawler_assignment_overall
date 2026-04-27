import asyncio

from src.async_crawler.async_crawler import AsyncCrawler
from src.data_storage.csv_storage import CSVStorage
from src.data_storage.json_storage import JSONStorage
from src.data_storage.sq_lite_storage import SQLiteStorage

STORAGE_TYPE="JSON"

async def main():
    start_urls = [
        # ✅ OK
        "https://httpbin.org/get",

        # # ⏱️ timeout / delay → retry
        # "https://httpbin.org/delay/3",
        #
        # # 🔄 503 → retry
        # "https://httpbin.org/status/503",
        #
        # # 🚫 429 → retry с увеличенным backoff
        # "https://httpbin.org/status/429",
        #
        # # ❌ 404 → no retry
        # "https://httpbin.org/status/404",
        #
        # # ❌ 403 → no retry
        # "https://httpbin.org/status/403",
        #
        # # ⚠️ 500 → retry (ограниченно)
        # "https://httpbin.org/status/500",
        #
        # # 🌐 Network error
        # "http://nonexistent-domain-12345.com",
        #
        # # robots.txt deny
        # "https://httpbin.org/deny",
        #
        # # Много старничек
        # "https://www.w3schools.com"
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
        storage=storage
    )

    print("🚀 Start crawling...\n")

    await crawler.crawl(
        start_urls=start_urls,
        max_pages=100,
        max_depth=20,
        same_domain_only=False,
        user_agent="MyCrawlerBot/1.0",
        disable_speed_log=True
    )

    print("✅ Done crawling...\n")

    print(crawler.storage.error_log)

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
