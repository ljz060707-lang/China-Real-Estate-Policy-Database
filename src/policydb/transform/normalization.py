from __future__ import annotations

import hashlib
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value)).replace("\u200b", "").strip()
    return re.sub(r"[ \t]+", " ", text) or None


def normalize_title(value: object) -> str | None:
    text = clean_text(value)
    return re.sub(r"[《》〈〉\s'\"“”‘’]", "", text).lower() if text else None


def normalize_url(value: object) -> str | None:
    text = clean_text(value)
    if not text or not text.lower().startswith(("http://", "https://")):
        return text
    parts = urlsplit(text)
    query = urlencode(
        sorted((k, v) for k, v in parse_qsl(parts.query) if not k.lower().startswith("utm_"))
    )
    return urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), query, "")
    )


def stable_id(*values: object, prefix: str = "REC") -> str:
    raw = "|".join(clean_text(v) or "" for v in values)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:20].upper()}"


def content_hash(*values: object) -> str:
    raw = "\n".join(clean_text(v) or "" for v in values)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
