from __future__ import annotations

import re
import zipfile
from io import BytesIO
from urllib.parse import urljoin

import fitz
import trafilatura
from bs4 import BeautifulSoup, NavigableString, Tag
from openpyxl import load_workbook

_END_PUNCTUATION = set("。！？；.!?;：:")
_ATTACHMENT_SUFFIXES = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar")
PARSER_VERSION = "3"
_MAIN_SELECTORS = (
    "#UCAP-CONTENT",
    ".TRS_Editor",
    ".article-content",
    ".article_content",
    ".conBox",
    "article",
    "main",
    "#content",
    ".content",
)
_NOISE_SELECTORS = (
    "script,style,noscript,nav,footer,header,head,form,"
    ".nav,.navbar,.breadcrumb,.crumb,.related,.share-box,.wza-box,"
    "[role=navigation],.print-button,.print-btn"
)
_MAX_OFFICE_UNCOMPRESSED = 64 * 1024 * 1024


def _normalise_block(value: str) -> str:
    return re.sub(r"[ \t\u3000]+", " ", value.replace("\r", "")).strip()


_CHARSET_RE = re.compile(r"charset\s*=\s*[\"']?\s*([\w.-]+)", re.I)


def _decode_html(body: bytes) -> str:
    """Decode HTML bytes with charset detection (meta/BOM), UTF-8 first, GBK fallback.

    Chinese government pages are frequently GB2312/GBK-encoded. The previous
    unconditional ``utf-8`` with ``errors="replace"`` silently produced mojibake
    for those pages; detection is deterministic and UTF-8 output is unchanged.
    """
    if body.startswith(b"\xef\xbb\xbf"):
        return body.decode("utf-8")
    head = body[:4096].decode("ascii", errors="ignore")
    match = _CHARSET_RE.search(head)
    if match:
        charset = match.group(1).lower().replace("_", "-")
        if charset in {"gb2312", "gbk", "cp936"}:
            charset = "gb18030"
        try:
            return body.decode(charset)
        except (LookupError, UnicodeDecodeError):
            pass
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return body.decode("gb18030")
        except UnicodeDecodeError:
            return body.decode("utf-8", errors="replace")


def merge_semantic_blocks(blocks: list[dict | str], min_chars: int = 24) -> dict:
    """Join broken DOM/PDF blocks without inventing text.

    A block is joined only when it is short, when the previous block has no sentence-ending
    punctuation, or when a PDF sentence continues on the next page. The returned evidence
    describes every deterministic join.
    """
    merged: list[dict] = []
    repairs: list[dict] = []
    for index, raw in enumerate(blocks):
        item = {"text": raw, "page": None, "kind": "text"} if isinstance(raw, str) else raw
        text = _normalise_block(str(item.get("text") or ""))
        if not text:
            continue
        page = item.get("page")
        if not merged:
            merged.append({"text": text, "page": page, "kind": item.get("kind", "text")})
            continue
        previous = merged[-1]
        cross_page = page is not None and previous.get("page") not in (None, page)
        continues_sentence = previous["text"][-1:] not in _END_PUNCTUATION
        short_fragment = len(text) < min_chars or len(previous["text"]) < min_chars
        heading = item.get("kind") == "heading"
        if not heading and item.get("kind") != "table" and previous.get("kind") not in {"heading", "table"} and (continues_sentence or (cross_page and short_fragment)):
            separator = "" if cross_page or continues_sentence else "\n"
            previous["text"] += separator + text
            repairs.append(
                {
                    "left_index": index - 1,
                    "right_index": index,
                    "cross_page": cross_page,
                    "reason": "cross_page_sentence" if cross_page else "semantic_continuity",
                }
            )
        elif short_fragment and item.get("kind") not in {"heading", "table"}:
            previous["text"] += "\n" + text
            repairs.append(
                {
                    "left_index": index - 1,
                    "right_index": index,
                    "cross_page": cross_page,
                    "reason": "short_block",
                }
            )
        else:
            merged.append({"text": text, "page": page, "kind": item.get("kind", "text")})
    return {
        "text": "\n\n".join(item["text"] for item in merged),
        "blocks": merged,
        "repairs": repairs,
    }


def _dom_blocks(root: Tag) -> list[dict]:
    """Visit content once, including bare div text; never duplicate nested p/tr nodes."""
    blocks: list[dict] = []
    for child in root.children:
        if isinstance(child, NavigableString):
            text = str(child).strip()
            if text:
                blocks.append({"text": text, "page": 1, "kind": "text"})
        elif isinstance(child, Tag):
            if child.name in {"p", "li", "tr", "h1", "h2", "h3", "h4"}:
                text = child.get_text(" | " if child.name == "tr" else " ", strip=True)
                if text:
                    blocks.append({"text": text, "page": 1, "kind": "table" if child.name == "tr" else "heading" if child.name.startswith("h") else "text"})
            else:
                blocks.extend(_dom_blocks(child))
    return blocks


def _empty_parse(kind: str, status: str, reason: str) -> dict:
    return {
        "document_type": kind, "title": None, "full_text": "", "page_count": None,
        "parse_status": status, "text_blocks": [], "repair_actions": [],
        "attachments": [], "dynamic_page_hint": False, "completeness_score": 0.0,
        "parser_error": reason,
    }


def _html_parse(body: bytes, base_url: str | None) -> dict:
    html = _decode_html(body)
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else None
    if re.search(r"^(?:404\b|页面不存在|页面未找到|访问出错|access denied|403 forbidden)", title or "", re.I):
        result = _empty_parse("html", "soft_error", "HTTP_200_ERROR_PAGE")
        result["title"] = title
        return result
    attachments = []
    for link in soup.select("a[href]"):
        href = str(link.get("href") or "").strip()
        label = link.get_text(" ", strip=True)
        clean_href = href.lower().split("?", 1)[0]
        if href and (clean_href.endswith(_ATTACHMENT_SUFFIXES) or "附件" in label):
            attachments.append(
                {"url": urljoin(base_url or "", href), "label": label or href, "source": "html_link"}
            )
    script_text = " ".join(node.get_text(" ", strip=True) for node in soup.select("script"))
    for node in soup.select(_NOISE_SELECTORS):
        node.decompose()
    root = None
    selector_used = None
    for selector in _MAIN_SELECTORS:
        candidates = [node for node in soup.select(selector) if node.get_text(strip=True)]
        if candidates:
            root = max(candidates, key=lambda node: len(node.get_text(strip=True)))
            selector_used = selector
            break
    repaired = merge_semantic_blocks(_dom_blocks(root or soup))
    # A known site body is authoritative. Generic pages use the installed main-text
    # extractor first, instead of concatenating every menu and sidebar paragraph.
    extracted = repaired["text"]
    if root is None:
        extracted = trafilatura.extract(str(soup), include_tables=True, include_comments=False, favor_precision=True) or extracted
    dynamic_hint = bool(
        re.search(r"(__NEXT_DATA__|webpack|ajax|iframe|加载中|请开启JavaScript)", html + script_text, re.I)
    )
    completeness = min(1.0, len(extracted) / 800)
    if title and title in extracted:
        completeness = min(1.0, completeness + 0.1)
    status = "parsed" if len(extracted) >= 40 else "partial"
    return {
        "document_type": "html",
        "title": title,
        "full_text": extracted,
        "page_count": None,
        "parse_status": status,
        "text_blocks": repaired["blocks"],
        "repair_actions": repaired["repairs"],
        "attachments": attachments,
        "dynamic_page_hint": dynamic_hint,
        "completeness_score": round(completeness, 4),
        "body_selector": selector_used,
    }


def _pdf_parse(body: bytes) -> dict:
    document = fitz.open(stream=BytesIO(body), filetype="pdf")
    blocks: list[dict] = []
    table_count = 0
    for page_number, page in enumerate(document, start=1):
        for block in page.get_text("blocks"):
            text = _normalise_block(str(block[4]))
            if text:
                blocks.append({"text": text, "page": page_number, "kind": "text"})
        try:
            tables = page.find_tables().tables
        except (AttributeError, RuntimeError, ValueError):
            tables = []
        for table in tables:
            rows = table.extract()
            table_text = "\n".join(
                " | ".join(str(cell or "").strip() for cell in row) for row in rows
            ).strip()
            if table_text:
                blocks.append({"text": table_text, "page": page_number, "kind": "table"})
                table_count += 1
    repaired = merge_semantic_blocks(blocks)
    embedded = [
        {"url": None, "label": name, "source": "pdf_embedded"}
        for name in document.embfile_names()
    ]
    text = repaired["text"]
    completeness = min(1.0, len(text) / max(500, len(document) * 300))
    return {
        "document_type": "pdf",
        "title": document.metadata.get("title") or None,
        "full_text": text,
        "page_count": len(document),
        "parse_status": "parsed" if len(text) >= 40 else "partial",
        "text_blocks": repaired["blocks"],
        "repair_actions": repaired["repairs"],
        "attachments": embedded,
        "dynamic_page_hint": False,
        "table_count": table_count,
        "completeness_score": round(completeness, 4),
    }


def extract_pdf_embedded(body: bytes) -> list[tuple[str, bytes]]:
    """Return embedded files verbatim so the Raw layer can store them append-only."""
    document = fitz.open(stream=BytesIO(body), filetype="pdf")
    return [(name, document.embfile_get(name)) for name in document.embfile_names()]


def _office_parse(body: bytes, content_type: str) -> dict:
    with zipfile.ZipFile(BytesIO(body)) as archive:
        if sum(info.file_size for info in archive.infolist()) > _MAX_OFFICE_UNCOMPRESSED:
            return _empty_parse("office", "deferred_unsupported", "OFFICE_SIZE_LIMIT")
    blocks: list[dict] = []
    if "spreadsheet" in content_type or "excel" in content_type:
        workbook = load_workbook(BytesIO(body), read_only=True, data_only=False)
        for sheet in workbook.worksheets:
            for row in sheet.iter_rows(values_only=True):
                values = [str(value).strip() for value in row if value not in (None, "")]
                if values:
                    blocks.append({"text": " | ".join(values), "page": None, "kind": "table"})
        workbook.close()
        document_type = "xlsx"
    else:
        with zipfile.ZipFile(BytesIO(body)) as archive:
            xml = archive.read("word/document.xml")
        soup = BeautifulSoup(xml, "xml")
        for paragraph in soup.find_all("w:p"):
            text = "".join(node.get_text() for node in paragraph.find_all("w:t")).strip()
            if text:
                blocks.append({"text": text, "page": None, "kind": "text"})
        document_type = "docx"
    repaired = merge_semantic_blocks(blocks)
    return {
        "document_type": document_type,
        "title": None,
        "full_text": repaired["text"],
        "page_count": None,
        "parse_status": "parsed" if repaired["text"] else "partial",
        "text_blocks": repaired["blocks"],
        "repair_actions": repaired["repairs"],
        "attachments": [],
        "dynamic_page_hint": False,
        "completeness_score": min(1.0, len(repaired["text"]) / 500),
    }


def parse_document(body: bytes, content_type: str | None, base_url: str | None = None) -> dict:
    content_type = (content_type or "").lower()
    kind = "binary"
    try:
        # Magic precedes MIME: government download endpoints frequently report HTML
        # or octet-stream for Office files, and may serve RAR under a .zip URL.
        if body.startswith(b"%PDF"):
            kind = "pdf"
            return _pdf_parse(body)
        if body.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
            return _empty_parse("ole", "deferred_unsupported", "LEGACY_OFFICE_CONVERTER_REQUIRED")
        if body.startswith(b"Rar!"):
            return _empty_parse("rar", "deferred_unsupported", "ARCHIVE_MEMBER_EXTRACTION_REQUIRED")
        if body.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
            kind = "zip"
            with zipfile.ZipFile(BytesIO(body)) as archive:
                names = set(archive.namelist())
            if "word/document.xml" in names:
                return _office_parse(body, "wordprocessingml")
            if "xl/workbook.xml" in names:
                return _office_parse(body, "spreadsheetml")
            return _empty_parse("zip", "deferred_unsupported", "ARCHIVE_MEMBER_EXTRACTION_REQUIRED")
        if body.startswith((b"MZ", b"7z\xbc\xaf\x27\x1c", b"\x89PNG", b"\xff\xd8\xff", b"GIF8")) or b"\0" in body[:4096]:
            return _empty_parse("binary", "deferred_unsupported", "NON_TEXT_BINARY")
        looks_html = bool(re.search(br"<(?:!doctype|html|head|body|article|div|p|title)\b", body[:4096], re.I))
        if not looks_html and any(token in content_type for token in ("pdf", "msword", "wordprocessingml", "spreadsheetml", "ms-excel", "zip", "rar", "octet-stream", "image/")):
            return _empty_parse("binary", "deferred_unsupported", "UNSUPPORTED_OR_INVALID_SIGNATURE")
        kind = "html"
        return _html_parse(body, base_url)
    except (RuntimeError, ValueError, TypeError, KeyError, OSError, zipfile.BadZipFile, RecursionError) as error:
        return _empty_parse(kind, "parser_error", f"{type(error).__name__}: {error}")
