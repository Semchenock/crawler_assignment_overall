import asyncio
import time
from src.async_crawler.async_crawler import AsyncCrawler

async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = [
        "https://example.com",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
    ]
    start_parallel = time.time()
    results = await crawler.fetch_urls(urls)
    end_parallel = time.time()
    print(f"Загружено {len(results)} страниц паралельно")
    print(f"Время паралельной загрузки: {end_parallel - start_parallel:.2f} сек")

    start_sequentially = time.time()
    result_1 = await crawler.fetch_url(urls[0])
    result_2 = await crawler.fetch_url(urls[1])
    result_3 = await crawler.fetch_url(urls[2])
    end_sequentially = time.time()
    print(f"Загружено 3 страниц последовательно")
    print(f"Время последовательной загрузки: {end_sequentially - start_sequentially:.2f} сек")

    print("Загрузка несуществующей страницы")
    not_found_url = await crawler.fetch_url("https://www.youtub.com/")

    print("Падение по таймауту (запрос 10+ сек)")
    start_timeout = time.time()
    long_url = await crawler.fetch_url("https://httpbin.org/delay/10")
    end_timeout = time.time()
    print(f"Время обработки запроса в 10 сек: {end_timeout - start_timeout:.2f} сек")

    await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())
