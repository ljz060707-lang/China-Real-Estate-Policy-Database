from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from rapidfuzz.fuzz import ratio

TRACKING_QUERY_KEYS = {"from", "spm", "source", "share", "scene", "timestamp"}
MOBILE_HOST_PREFIXES = ("m.", "wap.", "mobile.")
RULES_VERSION = "v2.0.0"


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    host = parts.netloc.lower().removeprefix("www.")
    for prefix in MOBILE_HOST_PREFIXES:
        if host.startswith(prefix):
            host = host.removeprefix(prefix)
            break
    query = urlencode(
        sorted(
            (key, value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
            if not key.lower().startswith("utm_")
            and key.lower() not in TRACKING_QUERY_KEYS
        )
    )
    return urlunsplit(
        (
            parts.scheme.lower(),
            host,
            re.sub(r"/{2,}", "/", parts.path).rstrip("/") or "/",
            query,
            "",
        )
    )


def content_sha256(content: bytes | str) -> str:
    value = content.encode("utf-8") if isinstance(content, str) else content
    return hashlib.sha256(value).hexdigest()


def normalize_policy_text(text: str) -> str:
    value = re.sub(r"[\u200b\ufeff]", "", text or "")
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"(?:责任编辑|来源|打印|关闭本页)[:：]?[^。；\n]{0,40}", "", value)
    return value.strip()


def normalized_text_hash(text: str) -> str:
    return content_sha256(normalize_policy_text(text))


def simhash64(text: str) -> str:
    tokens = re.findall(r"[\u4e00-\u9fff]{1,4}|[A-Za-z0-9]+", normalize_policy_text(text))
    vector = [0] * 64
    for token in tokens:
        digest = int.from_bytes(hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest())
        for bit in range(64):
            vector[bit] += 1 if digest & (1 << bit) else -1
    value = sum((1 << bit) for bit, weight in enumerate(vector) if weight >= 0)
    return f"{value:016x}"


def simhash_similarity(left: str, right: str) -> float:
    distance = (int(left, 16) ^ int(right, 16)).bit_count()
    return 1 - distance / 64


def policy_identity_key(
    *, title: str | None, document_number: str | None = None,
    agency: str | None = None, publication_date: str | None = None,
    jurisdiction: str | None = None,
) -> str:
    normalized = [
        normalize_policy_text(value or "")
        for value in (document_number, title, agency, publication_date, jurisdiction)
    ]
    return content_sha256("|".join(normalized))


def glm_cache_key(
    text_hash: str, model: str, prompt_version: str, schema_version: str
) -> str:
    return content_sha256("|".join((text_hash, model, prompt_version, schema_version)))


@dataclass(frozen=True)
class DedupDecision:
    level: str
    decision: str
    reason: str
    score: float = 1.0
    threshold: float = 1.0
    rules_version: str = RULES_VERSION

    def evidence_json(self, **extra: object) -> str:
        return json.dumps({**asdict(self), **extra}, ensure_ascii=False, sort_keys=True)


def classify_text_pair(
    left: str,
    right: str,
    *,
    left_numbers: list[str] | None = None,
    right_numbers: list[str] | None = None,
) -> DedupDecision:
    if normalized_text_hash(left) == normalized_text_hash(right):
        return DedupDecision("L4", "duplicate_content", "normalized text hashes match")
    lexical = ratio(normalize_policy_text(left), normalize_policy_text(right)) / 100
    semantic = simhash_similarity(simhash64(left), simhash64(right))
    score = 0.6 * lexical + 0.4 * semantic
    numeric_conflict = bool(
        left_numbers is not None
        and right_numbers is not None
        and set(left_numbers) != set(right_numbers)
    )
    if numeric_conflict:
        return DedupDecision("L6", "material_change", "critical numeric values conflict", score, 0.92)
    if score >= 0.96:
        return DedupDecision("L6", "possible_reprint", "high text similarity", score, 0.96)
    if score >= 0.86:
        return DedupDecision("L6", "possible_version", "similar text requires identity check", score, 0.86)
    return DedupDecision("L6", "new_document", "text similarity below duplicate threshold", score, 0.86)
