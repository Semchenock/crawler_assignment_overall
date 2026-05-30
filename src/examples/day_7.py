import asyncio
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from crawler import AdvancedCrawler


async def main():
    config_path = Path(__file__).with_name("config.yaml")
    crawler = AdvancedCrawler.from_config(str(config_path))

    try:
        await crawler.crawl()

        stats = crawler.get_stats()
        print(f"Processed: {stats['total_pages']} pages")
        print(f"Successful: {stats['successful']}")
        print(f"Failed: {stats['failed']}")

        if crawler.config["stats"]["json"]:
            crawler.export_to_json(crawler.config["stats"]["json"])

        if crawler.config["stats"]["html"]:
            crawler.export_to_html_report(crawler.config["stats"]["html"])
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
