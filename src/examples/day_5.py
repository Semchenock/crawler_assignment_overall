import asyncio
import json
from dataclasses import asdict

from src.async_crawler.async_crawler import AsyncCrawler

async def main():
    start_urls = [
        # ✅ OK
        "https://httpbin.org/get",

        # ⏱️ timeout / delay → retry
        "https://httpbin.org/delay/3",

        # 🔄 503 → retry
        "https://httpbin.org/status/503",

        # 🚫 429 → retry с увеличенным backoff
        "https://httpbin.org/status/429",

        # ❌ 404 → no retry
        "https://httpbin.org/status/404",

        # ❌ 403 → no retry
        "https://httpbin.org/status/403",

        # ⚠️ 500 → retry (ограниченно)
        "https://httpbin.org/status/500",

        # 🌐 Network error
        "http://nonexistent-domain-12345.com",

        # robots.txt deny
        "https://httpbin.org/deny",
        #
        # flaky для проверки необходимо запустить ./mock_server.py
        "http://127.0.0.1:8080/"
    ]

    crawler = AsyncCrawler(
        max_concurrent=3,
        max_per_domain=2,
        min_interval=1.0,
        max_jitter=0.5,
        respect_robots=True,
        user_agent="MyCrawlerBot/1.0",
    )

    print("🚀 Start crawling...\n")

    result = await crawler.crawl(
        start_urls=start_urls,
        max_pages=20,
        max_depth=1,
        same_domain_only=False
    )

    print("\n📊 FINAL STATS")
    print(f"✅ Processed: {len(result.processed_urls)}")
    print(f"❌ Failed: {len(result.failed_urls)}")

    with open("output_day_5.json", "w", encoding="utf-8") as f:
        json.dump(
            asdict(result),
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )

    crawler.error_log.export_error_log("error_log_day_5.json")

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
