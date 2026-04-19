import asyncio
from src.async_crawler.async_crawler import AsyncCrawler
import json
from dataclasses import asdict

async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = [
        "https://www.w3schools.com/html/html_tables.asp",
    ]

    data = await crawler.crawl(start_urls=urls, max_pages=100)

    print(f"Обработано: {len(data.processed_urls.items())} страниц")

    with open("output_day_3.json", "w", encoding="utf-8") as f:
        json.dump(
            asdict(data),
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
