from dataclasses import dataclass
from typing import Optional

@dataclass
class ParsedImage:
    src: str
    alt: Optional[str] = None

@dataclass
class ParsedHeaders:
    h1: list[str] = None
    h2: list[str] = None
    h3: list[str] = None
    h4: list[str] = None
    h5: list[str] = None
    h6: list[str] = None

@dataclass
class ParsedMetadata:
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[str] = None

@dataclass
class ParseResult:
    url: str
    text: str
    links: list[str]
    imgs: list[ParsedImage]
    headers: ParsedHeaders
    text_length: int
    links_count: int
    images_count: int
    metadata: ParsedMetadata
    tables: list[list[list[str]]]
    lists: list[list[str]]
    title: Optional[str] = None
