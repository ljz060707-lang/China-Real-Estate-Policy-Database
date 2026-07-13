from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import fitz
import httpx
import trafilatura
from bs4 import BeautifulSoup


@dataclass
class SourceItem:
    source: str
    url: str | None = None
    path: Path | None = None


class SourceAdapter(ABC):
    @abstractmethod
    def discover(self) -> list[SourceItem]: ...
    @abstractmethod
    def fetch(self, item: SourceItem) -> bytes: ...
    @abstractmethod
    def parse(self, document: bytes) -> dict: ...
    def normalize(self, record: dict) -> dict:
        record["content_hash"] = hashlib.sha256(str(record).encode("utf-8")).hexdigest()
        return record


class ManualURLAdapter(SourceAdapter):
    def __init__(self, url: str):
        self.url = url

    def discover(self):
        return [SourceItem(source="manual_url", url=self.url)]

    def fetch(self, item):
        return httpx.get(item.url, follow_redirects=True, timeout=30).content

    def parse(self, document):
        html = document.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        return {
            "title": soup.title.string.strip() if soup.title and soup.title.string else None,
            "full_text": trafilatura.extract(html),
            "html": html,
        }


class PDFAdapter(SourceAdapter):
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def discover(self):
        return [SourceItem(source="manual_pdf", path=self.path)]

    def fetch(self, item):
        return item.path.read_bytes()

    def parse(self, document):
        pdf = fitz.open(stream=document, filetype="pdf")
        return {"full_text": "\n".join(page.get_text() for page in pdf), "page_count": len(pdf)}


class LocalDirectoryAdapter(SourceAdapter):
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def discover(self):
        return [
            SourceItem(source="local_directory", path=p)
            for p in self.path.rglob("*")
            if p.is_file()
        ]

    def fetch(self, item):
        return item.path.read_bytes()

    def parse(self, document):
        return {"size": len(document)}


class HTMLListAdapter(ManualURLAdapter):
    def discover(self):
        body = self.fetch(SourceItem(source="html_list", url=self.url))
        soup = BeautifulSoup(body, "html.parser")
        return [SourceItem(source="html_list", url=a.get("href")) for a in soup.select("a[href]")]


class GovernmentSiteAdapter(HTMLListAdapter):
    """中国政府网、住建部、人行、金融监管总局及地方政府的配置化模板。"""


class CSVAdapter(LocalDirectoryAdapter):
    pass


class ExcelAdapter(LocalDirectoryAdapter):
    pass
