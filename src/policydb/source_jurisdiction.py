"""Auditable jurisdiction mappings and source-bundle identity helpers.

This module is deliberately deterministic.  A mapping can explain why a
provincial or centralized authority covers a city slot, but it never replaces
the network, parser, pagination, or official-host gates.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from policydb.crawl.dedup import canonicalize_url
from policydb.settings import Settings


@dataclass(frozen=True)
class JurisdictionMapping:
    mapping_id: str
    authority_level: str
    authority_name: str
    source_role: str
    authority_domain: str
    homepage_url: str | None
    list_page_urls: tuple[str, ...]
    covered_city_ids: tuple[str, ...]
    approval_status: str
    evidence_type: str | None = None
    evidence_url: str | None = None
    evidence_text: str | None = None
    effective_from: str | None = None
    effective_to: str | None = None
    notes: str | None = None
    source_bundle_id: str | None = None

    @property
    def evidence_id(self) -> str:
        return f"jurisdiction_mapping:{self.mapping_id}"

    @property
    def bundle_id(self) -> str:
        if self.source_bundle_id:
            return self.source_bundle_id
        digest = hashlib.sha256(
            "|".join(
                (
                    self.authority_domain.lower(),
                    self.authority_name,
                    self.source_role,
                )
            ).encode("utf-8")
        ).hexdigest()[:20].upper()
        return f"BUNDLE_{digest}"

    @property
    def identity_urls(self) -> tuple[str, ...]:
        return tuple(
            value
            for value in (self.homepage_url, *self.list_page_urls)
            if value
        )


def _as_list(value: object) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    return tuple(str(part).strip() for part in value if str(part).strip())


def _identity_key(url: str | None) -> tuple[str, str, str] | None:
    if not url:
        return None
    try:
        canonical = canonicalize_url(str(url))
        parsed = urlsplit(canonical)
    except Exception:
        return None
    host = (parsed.hostname or "").lower().rstrip(".")
    if not host or parsed.scheme not in {"http", "https"}:
        return None
    return host, parsed.path.rstrip("/") or "/", parsed.query


def _host(url: str | None) -> str:
    try:
        return (urlsplit(str(url or "")).hostname or "").lower().rstrip(".")
    except Exception:
        return ""


def load_jurisdiction_mappings(
    settings: Settings | None = None,
    *,
    path: Path | None = None,
) -> list[JurisdictionMapping]:
    """Load only explicitly configured mappings; provinces are never inferred."""

    settings = settings or Settings.discover()
    config_path = path or settings.root / "config" / "source_jurisdiction_overrides.yaml"
    if not config_path.exists():
        return []
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    mappings: list[JurisdictionMapping] = []
    for item in raw.get("mappings", []) or []:
        if not isinstance(item, Mapping):
            continue
        required = (
            "mapping_id",
            "authority_level",
            "authority_name",
            "source_role",
            "authority_domain",
            "covered_city_ids",
            "approval_status",
        )
        if any(not str(item.get(key) or "").strip() for key in required):
            continue
        mappings.append(
            JurisdictionMapping(
                mapping_id=str(item["mapping_id"]),
                authority_level=str(item["authority_level"]),
                authority_name=str(item["authority_name"]),
                source_role=str(item["source_role"]),
                authority_domain=str(item["authority_domain"]).lower().rstrip("."),
                homepage_url=str(item["homepage_url"]) if item.get("homepage_url") else None,
                list_page_urls=_as_list(item.get("list_page_urls")),
                covered_city_ids=_as_list(item.get("covered_city_ids")),
                approval_status=str(item["approval_status"]).lower(),
                evidence_type=str(item["evidence_type"]) if item.get("evidence_type") else None,
                evidence_url=str(item["evidence_url"]) if item.get("evidence_url") else None,
                evidence_text=str(item["evidence_text"]) if item.get("evidence_text") else None,
                effective_from=str(item["effective_from"]) if item.get("effective_from") else None,
                effective_to=str(item["effective_to"]) if item.get("effective_to") else None,
                notes=str(item["notes"]) if item.get("notes") else None,
                source_bundle_id=str(item["source_bundle_id"]) if item.get("source_bundle_id") else None,
            )
        )
    return mappings


def mapping_for_candidate(
    row: Mapping[str, Any],
    slot: Mapping[str, Any],
    *,
    settings: Settings | None = None,
    mappings: Iterable[JurisdictionMapping] | None = None,
) -> dict[str, Any]:
    """Return a mapping decision for one candidate without mutating anything."""

    city_id = str(slot.get("city_id") or row.get("city_id") or "")
    role = str(slot.get("source_role") or row.get("source_role") or "")
    candidate_url = str(row.get("canonical_url") or row.get("candidate_url") or "")
    candidate_host = _host(candidate_url)
    candidates = list(mappings) if mappings is not None else load_jurisdiction_mappings(settings)
    role_city = [
        item
        for item in candidates
        if item.source_role == role and city_id in item.covered_city_ids
    ]
    host_candidates = [item for item in candidates if item.authority_domain == candidate_host]
    if not role_city:
        if host_candidates:
            return {
                "status": "FAIL",
                "reason_code": "jurisdiction_mapping_city_or_role_mismatch",
                "mapping_id": host_candidates[0].mapping_id,
                "evidence_id": host_candidates[0].evidence_id,
                "source_bundle_id": host_candidates[0].bundle_id,
            }
        return {
            "status": "UNKNOWN",
            "reason_code": "jurisdiction_mapping_absent",
            "mapping_id": None,
            "evidence_id": None,
            "source_bundle_id": None,
        }
    mapping = next(
        (item for item in role_city if item.authority_domain == candidate_host),
        role_city[0],
    )
    mapping_data = {
        "mapping_id": mapping.mapping_id,
        "evidence_id": mapping.evidence_id,
        "source_bundle_id": mapping.bundle_id,
        "authority_level": mapping.authority_level,
        "approval_status": mapping.approval_status,
        "authority_domain": mapping.authority_domain,
    }
    if mapping.approval_status != "approved":
        return {"status": "UNKNOWN", "reason_code": "jurisdiction_mapping_not_approved", **mapping_data}
    if candidate_host != mapping.authority_domain:
        return {"status": "FAIL", "reason_code": "jurisdiction_mapping_domain_mismatch", **mapping_data}
    identity = _identity_key(candidate_url)
    identity_keys = {_identity_key(value) for value in mapping.identity_urls}
    # A mapping must identify the exact homepage/list boundary.  Scheme changes
    # (HTTP -> HTTPS) are tolerated, but host/path/query identity is not guessed.
    if identity not in identity_keys:
        return {"status": "FAIL", "reason_code": "jurisdiction_mapping_entry_identity_mismatch", **mapping_data}
    return {"status": "PASS", "reason_code": "centralized_authority_city_coverage", **mapping_data}


def source_bundle_metadata(
    row: Mapping[str, Any],
    *,
    slot: Mapping[str, Any] | None = None,
    settings: Settings | None = None,
    mappings: Iterable[JurisdictionMapping] | None = None,
) -> dict[str, Any]:
    slot = slot or row
    decision = mapping_for_candidate(row, slot, settings=settings, mappings=mappings)
    bundle_id = str(row.get("source_bundle_id") or decision.get("source_bundle_id") or "") or None
    return {
        "source_bundle_id": bundle_id,
        "jurisdiction_mapping_id": decision.get("mapping_id"),
        "jurisdiction_evidence_id": decision.get("evidence_id"),
        "jurisdiction_mapping_status": decision.get("status"),
        "jurisdiction_mapping_reason_code": decision.get("reason_code"),
        "authority_level": decision.get("authority_level"),
        "approval_status": decision.get("approval_status"),
    }


def source_covers_city(source: object, city_id: str) -> bool:
    return city_id in {
        *(str(value) for value in getattr(source, "city_ids", []) or []),
        *(str(value) for value in getattr(source, "coverage_city_ids", []) or []),
    }


def is_central_authority_host(host: str | None) -> bool:
    normalized = str(host or "").lower().rstrip(".")
    return normalized in {
        "gov.cn",
        "ccdi.gov.cn",
        "www.gov.cn",
    }


def is_clear_detail_url(url: str | None) -> bool:
    """Reject obvious article/detail/legal documents while keeping list indexes."""

    value = str(url or "")
    parsed = urlsplit(value)
    path = parsed.path.lower()
    if path.endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
        return True
    if re.search(r"/(?:law_display|art|article|content|detail|news|notice|info|show)/", path):
        return True
    if re.search(r"/(?:zwgk/)?public/[^/]+/\d+\.(?:s?html?|jhtml)$", path):
        return True
    if re.search(r"/col/[^/]+/\d+\.(?:s?html?|jhtml)$", path):
        return True
    basename = path.rstrip("/").rsplit("/", 1)[-1]
    if re.search(r"(?:^|[_-])t?20\d{6,}(?:[_-]|\.|$)", basename):
        return True
    if re.search(r"(?:^|[?&])(?:id|articleid|infoid|docid|contentid)=", parsed.query, re.I):
        return True
    return False
