from __future__ import annotations

from io import BytesIO

import fitz
import trafilatura
from bs4 import BeautifulSoup


def parse_document(body: bytes, content_type: str | None) -> dict:
    content_type = (content_type or "").lower()
    if "pdf" in content_type or body.startswith(b"%PDF"):
        document = fitz.open(stream=BytesIO(body), filetype="pdf")
        return {
            "document_type": "pdf",
            "title": None,
            "full_text": "\n".join(page.get_text() for page in document),
            "page_count": len(document),
            "parse_status": "parsed",
        }
    html = body.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else None
    return {
        "document_type": "html",
        "title": title,
        "full_text": trafilatura.extract(html) or soup.get_text("\n", strip=True),
        "page_count": None,
        "parse_status": "parsed",
    }

