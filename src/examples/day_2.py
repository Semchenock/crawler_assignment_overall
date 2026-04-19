import asyncio
from src.async_crawler.async_crawler import AsyncCrawler
import json
from dataclasses import asdict

async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = [
        "https://www.w3schools.com/html/html_tables.asp",
        "https://example.com/nonexistent-page",
        "https://httpbin.org/status/404",
        "https://www.w3schools.com/html/html_lists.asp",
        "https://httpbin.org/html",
        "https://httpbin.org/delay/10"
    ]

    data = await crawler.fetch_and_parse_urls(urls=urls)

    with open("output_day_2.json", "w", encoding="utf-8") as f:
        json.dump({url: asdict(result) for url, result in data.items()}, f, ensure_ascii=False, indent=2, default=str)

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
