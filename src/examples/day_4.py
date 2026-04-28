import asyncio
import json
from dataclasses import asdict

from src.async_crawler.async_crawler import AsyncCrawler


async def main():
    crawler = AsyncCrawler(
        max_concurrent=3,
        max_per_domain=2,
        min_interval=1.0,
        max_jitter=0.5,
        respect_robots=True,
        user_agent="MyCrawlerBot/1.0",
    )

    start_urls = [
        "https://httpbin.org/",
        "https://httpbin.org/robots.txt",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/status/404",
        "https://example.com/",
        "https://httpbin.org/deny"
    ]

    print("🚀 Start crawling...\n")

    result = await crawler.crawl(
        start_urls=start_urls,
        max_pages=20,
        max_depth=1,
        same_domain_only=False,
    )

    print("\n📊 FINAL STATS")
    print(f"✅ Processed: {len(result.processed_urls)}")
    print(f"❌ Failed: {len(result.failed_urls)}")

    blocked = [
        url for url, res in result.failed_urls.items()
        if res.error_type.name == "BLOCKED_BY_ROBOTS"
    ]

    print(f"🚫 Blocked by robots.txt: {len(blocked)}")

    if blocked:
        print("\nBlocked URLs:")
        for url in blocked:
            print(f"  - {url}")

    with open("output_day_4.json", "w", encoding="utf-8") as f:
        json.dump(
            asdict(result),
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )

    await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
