from typing import Optional

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging

class HtmlParser:
    def parse_html(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "lxml")

        metadata = self.extract_metadata(soup)
        text = self.extract_text(soup=soup, selector="body")
        links = self.extract_links(soup=soup, base_url=url)
        imgs = self.extract_imgs(soup=soup, base_url=url)
        headers = self.extract_headers(soup=soup)
        tables = self.extract_tables(soup=soup)
        lists = self.extract_lists(soup=soup)

        return {
            "url": url,
            "title": metadata.get("title"),
            "text": text,
            "links": links,
            "metadata": metadata,
            "imgs": imgs,
            "headers": headers,
            "tables": tables,
            "lists": lists,
            "text_length": len(text),
            "links_count": len(links),
            "images_count": len(imgs),
        }

    @staticmethod
    def prepare_url(url: str) -> str:
        if url.endswith(".html"):
            return url[:-5]
        elif url.endswith("/"):
            return url[:-1]
        else:
            return url

    @staticmethod
    def get_absolute_link(base_url: str, link: str) -> str:
        if not base_url.endswith("/"):
            base_url += "/"

        return urljoin(base_url, link)

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        base_netloc = urlparse(base_url).netloc

        links = []

        for a in soup.find_all("a"):
            href = a.get("href")
            if not href:
                logging.warning(f"Href not found in a element: {a}")
                continue

            absolute = self.get_absolute_link(base_url, href)
            parsed = urlparse(absolute)

            if parsed.scheme not in ("http", "https"):
                logging.warning(f"Link is not http or https: {absolute}")
                continue

            is_external = parsed.netloc != base_netloc

            if is_external:
                logging.warning(f"Link is external: {absolute}")
                continue

            links.append(absolute)

        return list(set(links))


    @staticmethod
    def extract_text(soup: BeautifulSoup, selector: str = None) -> str:
        element = soup.select_one(selector)

        if element is None:
            logging.warning(f"Element not found by selector: {selector}")
            return ""

        return element.get_text(strip=True)

    @staticmethod
    def find_meta(soup: BeautifulSoup, name: str) -> Optional[str]:
        name = name.lower()

        for tag in soup.find_all("meta"):
            tag_name = tag.get("name")
            if tag_name and tag_name.lower() == name:
                return tag.get("content")

        return None

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        title_tag = soup.find("title")

        title = title_tag.get_text(strip=True) if title_tag else None
        description = self.find_meta(soup, "description")
        keywords = self.find_meta(soup, "keywords")

        return {
            "title": title,
            "description": description,
            "keywords": keywords,
        }

    def extract_imgs(self, soup: BeautifulSoup, base_url:str) -> list[dict]:
        imgs = soup.find_all("img")

        imgs_srcs = []
        for img in imgs:
            src = img.get("src")

            if not isinstance(src, str):
                logging.warning(f"Invalid src tag: {img}")
                continue

            absolute_src = self.get_absolute_link(base_url=base_url, link=src)

            alt = img.get("alt")

            imgs_srcs.append({"src": absolute_src, "alt": alt })

        return imgs_srcs

    @staticmethod
    def extract_headers(soup: BeautifulSoup) -> dict[str, list[str]]:
        result = {}

        for i in range(6):
            element_name = f"h{i+1}"
            h_elements = soup.find_all(element_name)
            h_texts = []
            for h_element in h_elements:
                h_texts.append(h_element.get_text(strip=True))

            result[element_name] = h_texts

        return result

    @staticmethod
    def extract_tables(soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = soup.find_all("table")
        tables_data = []

        for table in tables:
            table_data = []

            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                values = [c.get_text(strip=True) for c in cells]

                if values:
                    table_data.append(values)

            tables_data.append(table_data)



        return tables_data

    @staticmethod
    def extract_lists(soup: BeautifulSoup) -> list[list[str]]:
        lists = soup.find_all(["ul", "ol"])
        lists_data = []

        for list_el in lists:
            list_data = [li.get_text(strip=True) for li in list_el.find_all("li")]

            if list_data:
                lists_data.append(list_data)

        return lists_data
