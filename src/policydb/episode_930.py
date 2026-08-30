"""Auditable 2016-09/10 property-policy episode supplement.

This module is deliberately a bounded, episode-scoped workflow.  It reuses the
project's curated snapshots, search providers, direct government HTTP client,
AI provider and atomic Parquet store.  It never promotes a search result or an
AI suggestion to a formal policy record by itself.

The formal episode layer is stored in separate curated snapshots so the
existing policy and crawl tables remain immutable.  A later, explicitly
reviewed migration can merge rows into the general research model without
losing this provenance.
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import time
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from policydb.ai import classify_ai_failure, get_ai_provider, validate_structured_payload
from policydb.ai_audit import AIAuditStore
from policydb.config.providers import SearchResult, build_search_fallback, build_search_provider
from policydb.crawl.dedup import canonicalize_url
from policydb.network import GovernmentDirectClient
from policydb.parquet_store import (
    atomic_write_parquet,
    merge_and_replace_parquet,
    read_parquet_snapshot,
)
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import clean_text

EPISODE_ID = "EP_2016_930_TIGHTENING"
EP930_ACTION_SCHEMA_VERSION = "episode_930_action_classification_v1"
EPISODE_NAME = "2016年930楼市调控潮"
EPISODE_DIRECTION = "TIGHTENING"
CORE_START = date(2016, 9, 25)
CORE_END = date(2016, 10, 10)
EXTENDED_START = date(2016, 9, 20)
EXTENDED_END = date(2016, 10, 15)
PROVENANCE_START = date(2016, 9, 1)
PROVENANCE_END = date(2016, 10, 31)

POLICY_TOOLS = (
    "LIMIT_PURCHASE",
    "LIMIT_RESALE",
    "COMMERCIAL_DOWNPAYMENT",
    "PF_DOWNPAYMENT",
    "PF_LOAN_CEILING",
    "PF_OTHER",
    "LAND_SUPPLY",
    "PRICE_REGULATION",
    "MARKET_SUPERVISION",
)

SEED_CITIES = {
    "北京市",
    "天津市",
    "深圳市",
    "广州市",
    "南京市",
    "苏州市",
    "无锡市",
    "合肥市",
    "郑州市",
    "成都市",
    "济南市",
    "厦门市",
    "杭州市",
    "武汉市",
    "珠海市",
    "东莞市",
    "佛山市",
    "福州市",
    "南昌市",
    "惠州市",
}

KEYWORDS = (
    "房地产市场平稳健康发展",
    "房地产市场调控",
    "进一步促进房地产",
    "住房限购",
    "住房限贷",
    "购房资格",
    "首付款比例",
    "差别化住房信贷",
    "住宅用地供应",
    "预售资金监管",
    "价格备案",
    "房地产市场监管",
)

TOOL_PATTERNS: dict[str, tuple[str, ...]] = {
    "LIMIT_PURCHASE": ("限购", "购房资格", "住房套数", "本市户籍", "社保", "纳税", "不得购买"),
    "LIMIT_RESALE": ("限售", "限制转让", "取得不动产权证", "上市交易", "网签备案"),
    "COMMERCIAL_DOWNPAYMENT": ("首付款", "首付", "商业性个人住房贷款", "认房认贷", "二套房贷", "贷款比例"),
    "PF_DOWNPAYMENT": ("公积金" , "住房公积金") ,
    "PF_LOAN_CEILING": ("公积金贷款", "贷款额度", "最高贷款额"),
    "PF_OTHER": ("公积金缴存", "公积金提取", "公积金中心"),
    "LAND_SUPPLY": ("土地供应", "住宅用地", "住房用地", "土地出让", "竞自持", "中小套型"),
    "PRICE_REGULATION": ("价格备案", "明码标价", "预售许可", "限价", "商品房价格"),
    "MARKET_SUPERVISION": ("预售资金监管", "虚假宣传", "捂盘", "房地产经纪", "中介", "资金监管", "市场秩序"),
}

SEARCH_TERMS = (
    "房地产市场平稳健康发展",
    "房地产市场调控",
    "限购",
    "首付比例",
    "住房信贷",
    "土地供应",
    "价格备案",
    "预售资金监管",
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _id(*parts: object, prefix: str) -> str:
    return f"{prefix}_{_sha("|".join(str(part or "") for part in parts))[:20].upper()}"


def _text(value: object) -> str:
    return clean_text(value) or ""


def _host(url: object) -> str:
    try:
        return (urlsplit(str(url or "")).hostname or "").lower()
    except ValueError:
        return ""


def _official_url(url: object) -> bool:
    host = _host(url)
    return bool(host) and (host == "gov.cn" or host.endswith(".gov.cn") or host.endswith(".gov.cn.cn"))


def _date_value(value: object) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f".{path.name}.part")
    part.write_text(_json(payload) + "\n", encoding="utf-8")
    part.replace(path)


@dataclass(frozen=True)
class EpisodeConfig:
    max_search_queries: int = 120
    search_results_per_query: int = 8
    max_official_fetches: int = 120
    max_ai_calls: int = 50
    run_search: bool = True
    run_ai: bool = True
    apply: bool = False
    ai_request_timeout_override: float | None = None
    ai_connect_timeout_override: float | None = None
    ai_max_retries_override: int | None = None
    bypass_ai_cache: bool = False


class ActionClassification(BaseModel):
    action_id: str
    policy_type: str = "OTHER"
    policy_subtype: str | None = None
    direction: str = "UNKNOWN"
    mechanism_labels: list[str] = Field(default_factory=list)
    target_population: str | None = None
    geographic_scope: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    unit: str | None = None
    effective_date_candidate: str | None = None
    confidence: float = 0.0
    reason: str = ""


class ActionClassificationPayload(BaseModel):
    actions: list[ActionClassification]


def _empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _frame(rows: list[dict[str, Any]], schema: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
    if rows:
        return pl.DataFrame(rows, infer_schema_length=None)
    return _empty_frame(schema or {})


def _xlsx_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Excel does not reliably accept list/object columns; preserve them as JSON."""
    result = frame
    for name, dtype in frame.schema.items():
        if dtype == pl.List(pl.String) or str(dtype).startswith("List"):
            result = result.with_columns(pl.col(name).map_elements(_json, return_dtype=pl.String))
    return result


def _write_xlsx(frame: pl.DataFrame, path: Path, sheet: str = "data") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _xlsx_frame(frame).write_excel(path, worksheet=sheet[:31], autofit=True)


def _atomic_bytes(path: Path, content: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(content).hexdigest()
    target = path.with_name(f"{path.stem}_{digest[:16]}{path.suffix}")
    if not target.exists():
        part = target.with_suffix(target.suffix + ".part")
        part.write_bytes(content)
        part.replace(target)
    return digest


def _city_aliases(cities: pl.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in cities.iter_rows(named=True):
        values = {str(row.get("city_name") or ""), str(row.get("city_name_short") or "")}
        values.update(str(row.get("aliases") or "").split("|"))
        for value in values:
            value = value.strip()
            if value:
                result[value] = row
    return result


def _find_city(value: object, cities: pl.DataFrame, aliases: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    text = _text(value)
    if not text:
        return None
    exact = aliases.get(text) or aliases.get(text.removesuffix("市"))
    if exact:
        return exact
    candidates = [(alias, row) for alias, row in aliases.items() if alias and alias in text]
    return max(candidates, key=lambda item: len(item[0]))[1] if candidates else None


def _record_text(row: dict[str, Any], version: dict[str, Any] | None = None) -> str:
    return _text(row.get("full_text") or (version or {}).get("extracted_text") or row.get("summary") or row.get("title"))


def _episode_match(row: dict[str, Any], text: str) -> bool:
    record_date = _date_value(row.get("record_date") or row.get("publication_date"))
    if record_date is None or not (PROVENANCE_START <= record_date <= PROVENANCE_END):
        return False
    haystack = " ".join((_text(row.get("title")), _text(text), _text(row.get("legacy_category"))))
    return any(term in haystack for term in KEYWORDS)


def _extract_number_pair(text: str) -> tuple[str | None, str | None, str | None]:
    number = r"\d+(?:\.\d+)?"
    matches = re.findall(number + r"\s*(?:%|％|年|个月|个月以上|套|万元|万|平方米|平米)?", text)
    if len(matches) < 2:
        return None, None, None
    unit_match = re.search(r"(百分比|%|％|年|个月|套|万元|万|平方米|平米)", " ".join(matches[:2]))
    unit = unit_match.group(1) if unit_match else None
    return matches[0], matches[1], unit


def _mechanisms(text: str) -> list[str]:
    return [tool for tool, terms in TOOL_PATTERNS.items() if any(term in text for term in terms)]


def _action_direction(text: str, fallback: object = None) -> str:
    if any(term in text for term in ("增加供应", "增加住宅用地", "扩大供应", "降低", "放宽", "取消", "支持", "优化")):
        return "SUPPORTIVE"
    if any(term in text for term in ("提高", "增加", "收紧", "限制", "不得", "暂停", "严禁", "加强监管", "限购", "限售")):
        return "TIGHTENING"
    value = str(fallback or "").lower()
    return {"tightening": "TIGHTENING", "loosening": "SUPPORTIVE", "supportive": "SUPPORTIVE"}.get(value, "UNKNOWN")


def _policy_type(mechanisms: Sequence[str]) -> str:
    if not mechanisms:
        return "OTHER"
    return mechanisms[0]


def _parse_effective_evidence(
    text: str,
    publication: date | None,
    *,
    action_specific: bool = False,
) -> tuple[date | None, str, str, str | None]:
    """Parse only explicit effective-date language.

    Publication date is used as an effective date only when the text explicitly
    says that the document takes effect from publication/issuance.  A bare
    publication date, a date in a title, or a general "effective" keyword is
    never inferred as an effective date.
    """

    value = _text(text)
    explicit = re.search(
        r"自\s*(20\d{2})\s*(?:年|[-/.])\s*(\d{1,2})\s*(?:月|[-/.])\s*(\d{1,2})\s*日?\s*"
        r"(?:起|开始)?\s*(?:施行|执行|实施|生效)",
        value,
    )
    if explicit:
        try:
            parsed = date(int(explicit.group(1)), int(explicit.group(2)), int(explicit.group(3)))
        except ValueError:
            parsed = None
        if parsed is not None:
            return (
                parsed,
                "HIGH",
                "ACTION_SPECIFIC_EFFECTIVE_DATE" if action_specific else "EXPLICIT_EFFECTIVE_DATE",
                explicit.group(0),
            )

    publication_based = re.search(
        r"(?:自|从)(?:发布|公布|印发|颁布|下发)之日起(?:施行|执行|实施|生效)"
        r"|本(?:通知|办法|规定|意见|方案|细则)?\s*自印发之日起(?:施行|执行|实施|生效)"
        r"|从即日起(?:施行|执行|实施|生效)",
        value,
    )
    if publication_based and publication is not None:
        return publication, "HIGH", "PUBLICATION_DATE_EFFECTIVE", publication_based.group(0)
    return None, "LOW", "NO_EXPLICIT_EFFECTIVE_DATE", None


def _parse_effective(text: str, publication: date | None) -> tuple[date | None, str]:
    """Backward-compatible two-value wrapper used by older callers/tests."""

    value, confidence, _basis, _evidence = _parse_effective_evidence(text, publication)
    return value, confidence


def _document_number(title: object) -> str | None:
    match = re.search(r"(?:[〔\[]\s*)?[^〔\[]{0,8}(?:\d{4}|20\d{2})[^〕\]]{0,12}[〕\]]", str(title or ""))
    return match.group(0).strip() if match else None


def _issuer(title: object) -> str | None:
    value = str(title or "").strip()
    if not value:
        return None
    value = re.sub(r"^[【\[].*?[】\]]", lambda match: match.group(0).strip("【】[]"), value)
    value = value.split("《", 1)[0].strip(" ：:")
    return value or None


def _split_clauses(text: str) -> list[str]:
    clauses = [part.strip() for part in re.split(r"[。；;！？!\n]+", text) if part.strip()]
    return [part[:1800] for part in clauses]


def _png_timeline(path: Path, rows: list[dict[str, Any]]) -> None:
    """Create a dependency-free PNG timeline; matplotlib is optional."""
    try:
        import matplotlib.pyplot as plt  # type: ignore

        ordered = sorted(rows, key=lambda row: (str(row.get("date") or "9999"), str(row.get("city") or "")))
        cities = list(dict.fromkeys(str(row.get("city") or "UNKNOWN") for row in ordered))
        fig, ax = plt.subplots(figsize=(13, max(4, len(cities) * 0.28)))
        for idx, city in enumerate(cities):
            for item in [row for row in ordered if str(row.get("city") or "UNKNOWN") == city]:
                day = _date_value(item.get("date")) or EXTENDED_START
                ax.scatter(day, idx, s=32, label=item.get("policy_type") if idx == 0 else None)
        ax.set_xlim(EXTENDED_START, EXTENDED_END)
        ax.set_yticks(range(len(cities)), cities)
        ax.set_title(EPISODE_NAME)
        ax.grid(axis="x", alpha=0.25)
        fig.tight_layout()
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return
    except Exception:
        pass
    width, height = 1200, max(260, 24 * max(1, len({str(row.get("city") or "") for row in rows})))
    raw = bytearray(width * height * 3)
    for x in range(width):
        for y in range(height):
            raw[(y * width + x) * 3 : (y * width + x) * 3 + 3] = b"\xf5\xf3\xee"
    def line(x1: int, y1: int, x2: int, y2: int) -> None:
        steps = max(abs(x2 - x1), abs(y2 - y1), 1)
        for step in range(steps + 1):
            x = int(x1 + (x2 - x1) * step / steps)
            y = int(y1 + (y2 - y1) * step / steps)
            if 0 <= x < width and 0 <= y < height:
                raw[(y * width + x) * 3 : (y * width + x) * 3 + 3] = b"\x67\x4a\x8e"
    for idx, row in enumerate(rows[:40]):
        day = _date_value(row.get("date")) or EXTENDED_START
        x = 40 + int((day - EXTENDED_START).days / max(1, (EXTENDED_END - EXTENDED_START).days) * (width - 80))
        y = 20 + idx * 20
        line(x - 4, y, x + 4, y)
    def chunk(kind: bytes, payload: bytes) -> bytes:
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
    scan = b"".join(b"\x00" + bytes(raw[y * width * 3 : (y + 1) * width * 3]) for y in range(height))
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(scan, 9)) + chunk(b"IEND", b"")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(png)


class Episode930Pipeline:
    def __init__(self, settings: Settings | None = None, *, config: EpisodeConfig | None = None, output: Path | None = None) -> None:
        self.settings = settings or Settings.discover()
        self.config = config or EpisodeConfig()
        self.output = (output or self.settings.outputs / "special_projects" / "2016_930").resolve()
        self.phase_dirs = {f"{index:02d}_{name}": self.output / f"{index:02d}_{name}" for index, name in enumerate(("SCOPE", "DISCOVERY", "OFFICIAL_RECOVERY", "GAP_AUDIT", "ACTION_EXTRACTION", "API_CLASSIFICATION", "DATE_VERIFICATION", "DEDUP", "MANUAL_REVIEW", "IMPORT", "DASHBOARD", "FINAL_AUDIT"))}
        for path in self.phase_dirs.values():
            path.mkdir(parents=True, exist_ok=True)
        self.state_path = self.output / "STATE.json"
        self._records: pl.DataFrame | None = None
        self._cities: pl.DataFrame | None = None
        self._aliases: dict[str, dict[str, Any]] | None = None

    @property
    def records(self) -> pl.DataFrame:
        if self._records is None:
            path = self.settings.curated / "records.parquet"
            cols = ["record_id", "record_type", "title", "record_date", "publication_date", "issuance_date", "effective_date", "expiry_date", "record_date_original", "status", "direction", "summary", "full_text", "official_status", "official_level", "primary_source_url", "landing_page_url", "document_url", "geography_original", "legacy_category"]
            self._records = read_parquet_snapshot(path, columns=[c for c in cols if c in pl.read_parquet_schema(path)])
        return self._records

    @property
    def cities(self) -> pl.DataFrame:
        if self._cities is None:
            self._cities = load_cities_105(self.settings)
        return self._cities

    @property
    def aliases(self) -> dict[str, dict[str, Any]]:
        if self._aliases is None:
            self._aliases = _city_aliases(self.cities)
        return self._aliases

    def _write_state(self, state: dict[str, Any]) -> None:
        state = {**state, "updated_at": _now(), "episode_id": EPISODE_ID}
        temp = self.state_path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        temp.replace(self.state_path)

    def scope(self) -> dict[str, Any]:
        scope = {
            "episode_id": EPISODE_ID,
            "episode_name": EPISODE_NAME,
            "episode_direction": EPISODE_DIRECTION,
            "core_window": {"start": CORE_START.isoformat(), "end": CORE_END.isoformat()},
            "extended_window": {"start": EXTENDED_START.isoformat(), "end": EXTENDED_END.isoformat()},
            "provenance_window": {"start": PROVENANCE_START.isoformat(), "end": PROVENANCE_END.isoformat()},
            "seed_cities": sorted(SEED_CITIES),
            "policy_tools": list(POLICY_TOOLS),
            "ai_policy": "AI may classify and review only after official recovery; deterministic evidence controls dates, URLs and import eligibility.",
            "formal_import_policy": "episode-scoped curated snapshots; existing records/actions are not overwritten",
        }
        (self.phase_dirs["00_SCOPE"] / "2016_930_SCOPE.json").write_text(json.dumps(scope, ensure_ascii=False, indent=2), encoding="utf-8")
        return scope

    def discover(self) -> dict[str, Any]:
        now = _now()
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        versions = read_parquet_snapshot(versions_path, columns=["record_id", "canonical_url", "final_url", "content_sha256", "local_path", "content_type", "http_status", "title", "extracted_text", "parse_status", "last_seen_at"]) if versions_path.exists() else pl.DataFrame()
        version_by_record = {}
        for row in versions.sort("last_seen_at", descending=True, nulls_last=True).iter_rows(named=True) if not versions.is_empty() else []:
            version_by_record.setdefault(str(row.get("record_id")), row)
        candidates: list[dict[str, Any]] = []
        references: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self.records.iter_rows(named=True):
            version = version_by_record.get(str(row.get("record_id")))
            text = _record_text(row, version)
            if not _episode_match(row, text):
                continue
            city = _find_city(row.get("geography_original") or row.get("title"), self.cities, self.aliases)
            city_name = str(city.get("city_name") if city else row.get("geography_original") or "UNKNOWN")
            city_id = str(city.get("city_id") if city else "UNKNOWN")
            url = row.get("primary_source_url") or row.get("landing_page_url") or row.get("document_url") or (version or {}).get("canonical_url")
            canonical = canonicalize_url(str(url)) if url else None
            official = str(row.get("official_status") or "").lower() in {"official", "official_reprint"} or _official_url(canonical)
            source_kind = "OFFICIAL_POLICY" if str(row.get("official_status") or "").lower() == "official" else "SECONDARY_DISCOVERY" if str(row.get("official_status") or "").lower() in {"general_media", "rumour"} else "CURATED_DISCOVERY"
            reference = {"record_id": row.get("record_id"), "url": canonical, "host": _host(canonical), "source_kind": source_kind, "title": row.get("title"), "record_date": row.get("record_date")}
            references[city_name].append(reference)
            candidates.append({
                "candidate_id": _id(EPISODE_ID, row.get("record_id"), canonical, prefix="CAND930"),
                "episode_id": EPISODE_ID,
                "record_id": row.get("record_id"),
                "city_id": city_id,
                "city": city_name,
                "province": city.get("province_name") if city else None,
                "document_title": row.get("title"),
                "record_date": row.get("record_date"),
                "candidate_url": str(url) if url else None,
                "canonical_url": canonical,
                "official_status": row.get("official_status"),
                "official_candidate": official,
                "source_kind": source_kind,
                "discovery_method": "curated_time_keyword_scan",
                "discovery_evidence": "; ".join(term for term in KEYWORDS if term in " ".join((_text(row.get("title")), text)))[:1000],
                "candidate_status": "proposed",
                "created_at": now,
            })
        cities_rows: list[dict[str, Any]] = []
        all_city_names = set(references) | SEED_CITIES
        for city_name in sorted(all_city_names):
            city = _find_city(city_name, self.cities, self.aliases)
            refs = references.get(city_name, [])
            official_count = sum(bool(ref.get("source_kind") == "OFFICIAL_POLICY" or _official_url(ref.get("url"))) for ref in refs)
            hosts = sorted({ref.get("host") for ref in refs if ref.get("host")})
            status = "SEED" if city_name in SEED_CITIES else "DISCOVERED_3_PLUS_REFERENCES" if len(refs) >= 3 and len(hosts) >= 2 else "REFERENCE_PENDING"
            cities_rows.append({
                "episode_id": EPISODE_ID, "city_id": city.get("city_id") if city else None, "city": city_name,
                "province": city.get("province_name") if city else None, "mentioned_as_930_city": status != "REFERENCE_PENDING",
                "reference_count": len(refs), "independent_source_count": len(hosts), "earliest_known_policy_date": min((ref.get("record_date") for ref in refs if ref.get("record_date") is not None), default=None),
                "latest_policy_date": max((ref.get("record_date") for ref in refs if ref.get("record_date") is not None), default=None),
                "official_policy_found": official_count > 0, "official_policy_count": official_count, "secondary_sources_count": sum(ref.get("source_kind") == "SECONDARY_DISCOVERY" for ref in refs),
                "expected_policy_types": _json(list(POLICY_TOOLS)), "actual_policy_types": None,
                "coverage_status": "OFFICIAL_POLICY_FOUND" if official_count else "UNRESOLVED_REASON", "missing_reason": None if official_count else "no_official_evidence_in_curated_snapshot",
                "notes": "seed city" if city_name in SEED_CITIES else "requires three independent references before episode scope inclusion",
            })
        city_frame = _frame(cities_rows)
        candidate_frame = _frame(candidates)
        _write_xlsx(city_frame, self.output / "2016_930_CITY_DISCOVERY.xlsx", "cities")
        _write_xlsx(candidate_frame, self.output / "2016_930_DISCOVERY_CANDIDATES.xlsx", "candidates")
        atomic_write_parquet(candidate_frame, self.phase_dirs["01_DISCOVERY"] / "930_DISCOVERY_CANDIDATES.parquet", {"module": "episode_930", "phase": "discovery"}, key_columns=("candidate_id",))
        atomic_write_parquet(city_frame, self.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet", {"module": "episode_930", "phase": "discovery"}, key_columns=("city",))
        result = {"candidate_count": candidate_frame.height, "reference_city_count": city_frame.filter(pl.col("mentioned_as_930_city")).height, "discovered_city_count": city_frame.height, "seed_city_count": len(SEED_CITIES), "official_candidates": candidate_frame.filter(pl.col("official_candidate")).height, "secondary_candidates": candidate_frame.filter(pl.col("source_kind") == "SECONDARY_DISCOVERY").height}
        self._write_state({"stage": "DISCOVERY", **result})
        return result

    def _search_missing(self, cities_frame: pl.DataFrame, candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not self.config.run_search or self.config.max_search_queries <= 0:
            return candidates, []
        provider = build_search_fallback(self.settings)
        if getattr(provider, "name", "None") == "None":
            provider = build_search_provider("ddg", None)
        queries: list[tuple[str, str, str]] = []
        for row in cities_frame.filter(pl.col("mentioned_as_930_city")).iter_rows(named=True):
            city = str(row.get("city") or "")
            for term in SEARCH_TERMS:
                queries.append((city, term, f"{city} {term} 2016 site:gov.cn"))
        queries = queries[: self.config.max_search_queries]
        evidence: list[dict[str, Any]] = []
        known = {str(row.get("canonical_url")) for row in candidates if row.get("canonical_url")}
        for city, term, query in queries:
            try:
                results = provider.search(query, max_results=self.config.search_results_per_query)
            except Exception as exc:
                evidence.append({"episode_id": EPISODE_ID, "city": city, "term": term, "query": query, "provider": getattr(provider, "name", type(provider).__name__), "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)[:500], "created_at": _now()})
                continue
            for item in results:
                canonical = canonicalize_url(item.url)
                evidence.append({"episode_id": EPISODE_ID, "city": city, "term": term, "query": query, "provider": getattr(provider, "name", type(provider).__name__), "result_url": item.url, "canonical_url": canonical, "title": item.title, "snippet": item.snippet, "official_candidate": _official_url(canonical), "status": "result", "created_at": _now()})
                if _official_url(canonical) and canonical not in known:
                    city_row = _find_city(city, self.cities, self.aliases)
                    candidates.append({"candidate_id": _id(EPISODE_ID, city, canonical, prefix="CAND930"), "episode_id": EPISODE_ID, "record_id": None, "city_id": city_row.get("city_id") if city_row else None, "city": city, "province": city_row.get("province_name") if city_row else None, "document_title": item.title, "record_date": None, "candidate_url": item.url, "canonical_url": canonical, "official_status": "discovery_only", "official_candidate": True, "source_kind": "OFFICIAL_SEARCH_CANDIDATE", "discovery_method": "configured_search_provider", "discovery_evidence": item.snippet, "candidate_status": "proposed", "created_at": _now()})
                    known.add(canonical)
        return candidates, evidence

    def official_recovery(self, candidates: list[dict[str, Any]] | None = None, cities_frame: pl.DataFrame | None = None) -> tuple[pl.DataFrame, dict[str, Any]]:
        if candidates is None:
            candidate_path = self.phase_dirs["01_DISCOVERY"] / "930_DISCOVERY_CANDIDATES.parquet"
            candidates = read_parquet_snapshot(candidate_path).to_dicts() if candidate_path.exists() else []
        if cities_frame is None:
            city_path = self.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet"
            cities_frame = read_parquet_snapshot(city_path) if city_path.exists() else pl.DataFrame()
        candidates, search_evidence = self._search_missing(cities_frame, candidates)
        if search_evidence:
            search_frame = _frame(search_evidence)
            atomic_write_parquet(search_frame, self.phase_dirs["01_DISCOVERY"] / "930_SEARCH_EVIDENCE.parquet", {"module": "episode_930", "phase": "discovery"}, key_columns=("query", "canonical_url", "status"))
            _write_xlsx(search_frame, self.output / "2016_930_SEARCH_EVIDENCE.xlsx", "search")
        rows: list[dict[str, Any]] = []
        fetched = 0
        client = GovernmentDirectClient(timeout=self.settings.request_timeout, connect_timeout=self.settings.connect_timeout, user_agent=self.settings.user_agent)
        try:
            for candidate in candidates[: self.config.max_official_fetches]:
                url = candidate.get("candidate_url") or candidate.get("canonical_url")
                if not url or not _official_url(url):
                    continue
                record = next((row for row in self.records.iter_rows(named=True) if row.get("record_id") == candidate.get("record_id")), None)
                version = None
                if record is not None:
                    version_path = self.settings.curated / "policy_document_versions.parquet"
                    if version_path.exists():
                        versions = read_parquet_snapshot(version_path, columns=["record_id", "canonical_url", "final_url", "content_sha256", "local_path", "content_type", "http_status", "title", "extracted_text", "parse_status"])
                        matches = versions.filter(pl.col("record_id") == record.get("record_id"))
                        version = matches.head(1).to_dicts()[0] if matches.height else None
                retrieved_at = _now()
                live_status = "not_attempted"
                final_url = candidate.get("canonical_url") or url
                content_hash = None
                content_type = None
                text = _record_text(record, version) if record else ""
                raw_path = version.get("local_path") if version else None
                try:
                    response = client.get(str(url), headers={"Referer": str(url)})
                    live_status = "recovered" if response.status_code == 200 else f"http_{response.status_code}"
                    final_url = response.final_url
                    suffix = ".pdf" if "pdf" in str(response.headers.get("content-type", "")).lower() or str(final_url).lower().endswith(".pdf") else ".html"
                    target = self.phase_dirs["02_OFFICIAL_RECOVERY"] / "raw" / f"{_id(EPISODE_ID, candidate.get('candidate_id'), prefix='RAW930')}{suffix}"
                    content_hash = _atomic_bytes(target, response.content)
                    content_type = response.headers.get("content-type")
                    if suffix == ".html":
                        soup = BeautifulSoup(response.content, "html.parser")
                        text = soup.get_text(" ", strip=True)[:200000]
                    raw_path = str(target)
                    fetched += 1
                except Exception as exc:
                    live_status = "fetch_failed"
                    error_type = type(exc).__name__
                else:
                    error_type = None
                doc_id = _id(EPISODE_ID, candidate.get("record_id"), candidate.get("canonical_url"), prefix="DOC930")
                publication_date = (record or {}).get("publication_date") or (record or {}).get("record_date")
                announcement_date = (record or {}).get("record_date")
                effective_date, date_confidence, effective_date_basis, date_evidence_text = _parse_effective_evidence(
                    text,
                    _date_value(publication_date),
                )
                rows.append(
                    {
                        "episode_id": EPISODE_ID,
                        "episode_name": EPISODE_NAME,
                        "document_id": doc_id,
                        "record_id": candidate.get("record_id"),
                        "city_id": candidate.get("city_id"),
                        "city": candidate.get("city"),
                        "province": candidate.get("province"),
                        "document_title": candidate.get("document_title") or (record or {}).get("title"),
                        "document_number": _document_number((record or {}).get("title") or candidate.get("document_title")),
                        "issuer": _issuer((record or {}).get("title") or candidate.get("document_title")),
                        "document_type": "OFFICIAL_POLICY" if candidate.get("source_kind") in {"OFFICIAL_POLICY", "OFFICIAL_SEARCH_CANDIDATE"} else "OFFICIAL_REPRINT" if candidate.get("official_status") == "official_reprint" else "MEDIA_REPORT",
                        "official_url": str(url),
                        "canonical_url": canonicalize_url(str(url)),
                        "final_url": final_url,
                        "official_source": _official_url(url),
                        "official_evidence_status": "LIVE_HTTP_200" if live_status == "recovered" else "CURATED_OFFICIAL" if record is not None and candidate.get("official_candidate") else "UNRESOLVED",
                        "live_status": live_status,
                        "http_status": 200 if live_status == "recovered" else None,
                        "content_type": content_type or (version or {}).get("content_type"),
                        "content_hash": content_hash or (version or {}).get("content_sha256"),
                        "raw_path": raw_path,
                        "official_text": text[:200000] if text else None,
                        "publication_date": publication_date,
                        "announcement_date": announcement_date,
                        "effective_date": effective_date,
                        "implementation_date": effective_date,
                        "date_confidence": date_confidence,
                        "effective_date_basis": effective_date_basis,
                        "date_evidence_text": date_evidence_text,
                        "expiry_date": (record or {}).get("expiry_date"),
                        "source_confidence": 0.95 if live_status == "recovered" else 0.75 if record is not None else 0.25,
                        "retrieved_at": retrieved_at,
                        "error_type": error_type,
                        "is_formal_eligible": bool(_official_url(url) and candidate.get("official_candidate") and (live_status == "recovered" or record is not None)),
                        "created_at": retrieved_at,
                    }
                )
        finally:
            client.close()
        frame = _frame(rows)
        atomic_write_parquet(frame, self.phase_dirs["02_OFFICIAL_RECOVERY"] / "930_OFFICIAL_RECOVERY.parquet", {"module": "episode_930", "phase": "official_recovery"}, key_columns=("document_id",))
        _write_xlsx(frame, self.output / "2016_930_DOCUMENTS.xlsx", "documents")
        result = {"official_candidate_count": len(candidates), "official_recovered": int(frame.filter(pl.col("official_evidence_status") == "LIVE_HTTP_200").height) if not frame.is_empty() else 0, "curated_official": int(frame.filter(pl.col("official_evidence_status") == "CURATED_OFFICIAL").height) if not frame.is_empty() else 0, "unresolved_documents": int(frame.filter(pl.col("official_evidence_status") == "UNRESOLVED").height) if not frame.is_empty() else 0, "http_fetches": fetched, "search_evidence": len(search_evidence)}
        self._write_state({"stage": "OFFICIAL_RECOVERY", **result})
        return frame, result

    def gap_audit(self, documents: pl.DataFrame, *, pass_number: int) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        reference_path = self.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet"
        cities = read_parquet_snapshot(reference_path) if reference_path.exists() else pl.DataFrame()
        doc_records = documents.to_dicts()
        for city_row in cities.filter(pl.col("mentioned_as_930_city")).iter_rows(named=True) if not cities.is_empty() else []:
            city = city_row.get("city")
            city_docs = [doc for doc in doc_records if doc.get("city") == city and doc.get("is_formal_eligible")]
            combined = " ".join(str(doc.get("official_text") or "") for doc in city_docs)
            for tool in POLICY_TOOLS:
                found = any(term in combined for term in TOOL_PATTERNS[tool])
                state = "POLICY_FOUND" if found else "INSUFFICIENT_SOURCE" if not city_docs else "POSSIBLE_MISSING"
                rows.append({"episode_id": EPISODE_ID, "audit_pass": pass_number, "city_id": city_row.get("city_id"), "city": city, "province": city_row.get("province"), "policy_tool": tool, "state": state, "document_count": len(city_docs), "evidence": next((doc.get("official_url") for doc in city_docs if any(term in str(doc.get("official_text") or "") for term in TOOL_PATTERNS[tool])), None), "gap_type": "CITY_GAP" if not city_docs else "POLICY_TYPE_GAP" if not found else None, "severity": "HIGH" if not city_docs else "MEDIUM" if not found else None, "audited_at": _now()})
            for doc in city_docs:
                if doc.get("effective_date") is None:
                    rows.append({"episode_id": EPISODE_ID, "audit_pass": pass_number, "city_id": city_row.get("city_id"), "city": city, "province": city_row.get("province"), "policy_tool": "DATE", "state": "POSSIBLE_MISSING", "document_count": 1, "evidence": doc.get("official_url"), "gap_type": "DATE_GAP", "severity": "HIGH", "audited_at": _now()})
                if not doc.get("official_url"):
                    rows.append({"episode_id": EPISODE_ID, "audit_pass": pass_number, "city_id": city_row.get("city_id"), "city": city, "province": city_row.get("province"), "policy_tool": "SOURCE", "state": "POSSIBLE_MISSING", "document_count": 1, "evidence": None, "gap_type": "OFFICIAL_SOURCE_GAP", "severity": "HIGH", "audited_at": _now()})
        matrix_rows = [row for row in rows if row.get("policy_tool") in POLICY_TOOLS]
        gap_rows = [row for row in rows if row.get("state") != "POLICY_FOUND" or row.get("gap_type")]
        for index, row in enumerate(gap_rows, 1):
            row["gap_id"] = _id(EPISODE_ID, pass_number, index, row.get("city"), row.get("policy_tool"), row.get("gap_type"), row.get("evidence"), prefix="GAP930")
        gap_schema = {
            "episode_id": pl.String,
            "audit_pass": pl.Int64,
            "city_id": pl.String,
            "city": pl.String,
            "province": pl.String,
            "policy_tool": pl.String,
            "state": pl.String,
            "document_count": pl.Int64,
            "evidence": pl.String,
            "gap_type": pl.String,
            "severity": pl.String,
            "audited_at": pl.String,
        }
        matrix = _frame(matrix_rows, gap_schema)
        gaps = _frame(gap_rows, {**gap_schema, "gap_id": pl.String})
        atomic_write_parquet(matrix, self.phase_dirs["03_GAP_AUDIT"] / f"2016_930_CITY_POLICY_MATRIX_PASS_{pass_number}.parquet", {"module": "episode_930", "phase": "gap_audit", "pass": pass_number}, key_columns=("audit_pass", "city", "policy_tool"))
        atomic_write_parquet(gaps, self.phase_dirs["03_GAP_AUDIT"] / f"2016_930_GAP_AUDIT_PASS_{pass_number}.parquet", {"module": "episode_930", "phase": "gap_audit", "pass": pass_number}, key_columns=("gap_id",))
        _write_xlsx(matrix, self.output / "2016_930_CITY_POLICY_MATRIX.xlsx", "matrix")
        _write_xlsx(gaps, self.output / "2016_930_GAP_AUDIT.xlsx", "gaps")
        result = {"pass": pass_number, "matrix_cells": matrix.height, "policy_found": int(matrix.filter(pl.col("state") == "POLICY_FOUND").height) if not matrix.is_empty() else 0, "unresolved_cells": int(matrix.filter(pl.col("state") != "POLICY_FOUND").height) if not matrix.is_empty() else 0, "gap_rows": gaps.height, "gap_type_counts": Counter(str(value) for value in gaps.get_column("gap_type").drop_nulls().to_list()).most_common() if not gaps.is_empty() and "gap_type" in gaps.columns else []}
        self._write_state({"stage": f"GAP_AUDIT_{pass_number}", **result})
        return matrix, gaps, result

    def build_gap_register(
        self,
        documents: pl.DataFrame,
        actions: pl.DataFrame,
        params: pl.DataFrame,
        base_gaps: pl.DataFrame,
        *,
        attachment_metrics: dict[str, Any] | None = None,
        ai_rows: pl.DataFrame | None = None,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Materialise the typed episode gap register after extraction.

        The crawler's two city/tool audits remain intact.  This final register
        adds document/action-level gaps without converting advisory AI output
        into formal eligibility.
        """

        rows = base_gaps.to_dicts() if not base_gaps.is_empty() else []
        known: set[tuple[str, str, str, str]] = set()
        for row in rows:
            known.add((str(row.get("city") or ""), str(row.get("document_id") or ""), str(row.get("action_id") or ""), str(row.get("gap_type") or row.get("policy_tool") or "")))

        def add_gap(
            gap_type: str,
            *,
            city: object = None,
            document_id: object = None,
            action_id: object = None,
            evidence: object = None,
            severity: str = "MEDIUM",
            reason: str = "",
        ) -> None:
            key = (str(city or ""), str(document_id or ""), str(action_id or ""), gap_type)
            if key in known:
                return
            known.add(key)
            rows.append({
                "episode_id": EPISODE_ID,
                "audit_pass": 2,
                "city_id": None,
                "city": city,
                "province": None,
                "policy_tool": gap_type,
                "state": "POSSIBLE_MISSING",
                "document_count": 1 if document_id else 0,
                "evidence": evidence,
                "gap_type": gap_type,
                "severity": severity,
                "reason": reason,
                "document_id": document_id,
                "action_id": action_id,
                "audited_at": _now(),
            })

        parameter_tools = {"LIMIT_PURCHASE", "LIMIT_RESALE", "COMMERCIAL_DOWNPAYMENT", "PF_DOWNPAYMENT", "PF_LOAN_CEILING"}
        parameter_action_ids = set(params.get_column("action_id").cast(pl.String).to_list()) if not params.is_empty() and "action_id" in params.columns else set()
        classified_action_ids = set()
        if ai_rows is not None and not ai_rows.is_empty() and "action_id" in ai_rows.columns:
            classified_action_ids = set(ai_rows.get_column("action_id").drop_nulls().cast(pl.String).to_list())
        for doc in documents.iter_rows(named=True) if not documents.is_empty() else []:
            doc_id = doc.get("document_id")
            if not doc.get("city"):
                add_gap("CITY_GAP", document_id=doc_id, evidence=doc.get("official_url"), severity="HIGH", reason="document has no city linkage")
            if not doc.get("official_url"):
                add_gap("OFFICIAL_SOURCE_GAP", city=doc.get("city"), document_id=doc_id, severity="HIGH", reason="official URL missing")
            if not doc.get("effective_date"):
                add_gap("DATE_GAP", city=doc.get("city"), document_id=doc_id, evidence=doc.get("official_url"), severity="HIGH", reason="no explicit effective-date evidence")
            doc_actions = actions.filter(pl.col("document_id") == doc_id) if not actions.is_empty() else pl.DataFrame()
            if doc_actions.is_empty():
                add_gap("ACTION_IDENTITY_GAP", city=doc.get("city"), document_id=doc_id, evidence=doc.get("official_url"), severity="HIGH", reason="no deterministic action extracted")
            for action in doc_actions.iter_rows(named=True):
                action_id = action.get("action_id")
                if not action.get("action_text") or not action_id:
                    add_gap("ACTION_IDENTITY_GAP", city=action.get("city"), document_id=doc_id, action_id=action_id, evidence=action.get("official_text_excerpt"), severity="HIGH", reason="action identity is incomplete")
                if str(action.get("policy_type") or "") in parameter_tools and str(action_id) not in parameter_action_ids:
                    add_gap("PARAMETER_GAP", city=action.get("city"), document_id=doc_id, action_id=action_id, evidence=action.get("official_text_excerpt"), reason="quantitative mechanism has no parameter row")
                if str(action_id) not in classified_action_ids:
                    add_gap("CLASSIFICATION_GAP", city=action.get("city"), document_id=doc_id, action_id=action_id, evidence=action.get("official_text_excerpt"), reason="no successful advisory classification row")

        attachments = attachment_metrics or {}
        attachment_pending = int(attachments.get("pending", 0) or 0) + int(attachments.get("retryable_failure", 0) or 0)
        if attachment_pending > 0:
            add_gap("ATTACHMENT_GAP", evidence=str(attachments), severity="MEDIUM", reason="attachment discovery completed but archive is incomplete")
        for row in rows:
            if not row.get("gap_id"):
                row["gap_id"] = _id(EPISODE_ID, row.get("audit_pass"), row.get("city"), row.get("document_id"), row.get("action_id"), row.get("gap_type"), row.get("evidence"), prefix="GAP930")
        frame = _frame(rows)
        atomic_write_parquet(frame, self.phase_dirs["03_GAP_AUDIT"] / "2016_930_GAP_REGISTER.parquet", {"module": "episode_930", "phase": "typed_gap_register"}, key_columns=("gap_id",))
        _write_xlsx(frame, self.output / "2016_930_GAP_REGISTER.xlsx", "gaps")
        metrics = {"gap_rows": frame.height, "gap_types": Counter(str(value) for value in frame.get_column("gap_type").drop_nulls().to_list()).most_common() if not frame.is_empty() and "gap_type" in frame.columns else []}
        return frame, metrics

    def extract_actions(self, documents: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        params: list[dict[str, Any]] = []
        for doc in documents.filter(pl.col("is_formal_eligible")).iter_rows(named=True) if not documents.is_empty() else []:
            text = _text(doc.get("official_text"))
            publication = _date_value(doc.get("publication_date") or doc.get("announcement_date"))
            for clause_index, clause in enumerate(_split_clauses(text), 1):
                mechanisms = _mechanisms(clause)
                if not mechanisms:
                    continue
                action_id = _id(EPISODE_ID, doc.get("document_id"), clause_index, clean_text(clause)[:240], prefix="ACT930")
                effective, date_conf, effective_basis, date_evidence_text = _parse_effective_evidence(
                    clause,
                    publication,
                    action_specific=True,
                )
                old_value, new_value, unit = _extract_number_pair(clause)
                direction = _action_direction(clause, doc.get("direction"))
                actions.append({"episode_id": EPISODE_ID, "episode_name": EPISODE_NAME, "document_id": doc.get("document_id"), "record_id": doc.get("record_id"), "city": doc.get("city"), "province": doc.get("province"), "action_id": action_id, "clause_id": f"CLAUSE_{clause_index:04d}", "action_text": clause, "policy_type": _policy_type(mechanisms), "policy_subtype": None, "mechanism_labels": mechanisms, "episode_direction": EPISODE_DIRECTION, "action_direction": direction, "target_population": None, "geographic_scope": "全市" if "全市" in clause else "部分区域" if "部分区域" in clause else None, "announcement_date": doc.get("announcement_date"), "publication_date": doc.get("publication_date"), "effective_date": effective, "implementation_date": effective, "effective_date_basis": effective_basis, "date_evidence_text": date_evidence_text, "expiry_date": doc.get("expiry_date"), "date_confidence": date_conf, "old_value": old_value, "new_value": new_value, "unit": unit, "parameter_confidence": 0.8 if old_value or new_value else 0.0, "official_url": doc.get("official_url"), "source_confidence": doc.get("source_confidence"), "classification_confidence": 0.75, "episode_confidence": 0.9, "official_text_excerpt": clause[:500], "is_formal_eligible": True, "extraction_method": "deterministic_keyword_clause_split", "created_at": _now()})
                if old_value or new_value:
                    params.append({"episode_id": EPISODE_ID, "action_id": action_id, "document_id": doc.get("document_id"), "city": doc.get("city"), "parameter_name": _policy_type(mechanisms), "old_value": old_value, "new_value": new_value, "unit": unit, "evidence_text": clause[:1000], "parameter_confidence": 0.8, "extraction_method": "deterministic_number_pair", "created_at": _now()})
        # Empty bounded batches are valid outcomes.  Preserve the keyed table
        # schemas so an empty action/parameter result is still an auditable
        # snapshot rather than a schema-less frame that cannot be atomically
        # written with its declared key columns.
        action_frame = _frame(
            actions,
            {
                "episode_id": pl.String,
                "episode_name": pl.String,
                "document_id": pl.String,
                "record_id": pl.String,
                "city": pl.String,
                "province": pl.String,
                "action_id": pl.String,
                "clause_id": pl.String,
                "action_text": pl.String,
                "policy_type": pl.String,
                "policy_subtype": pl.String,
                "mechanism_labels": pl.List(pl.String),
                "episode_direction": pl.String,
                "action_direction": pl.String,
                "target_population": pl.String,
                "geographic_scope": pl.String,
                "announcement_date": pl.Date,
                "publication_date": pl.Date,
                "effective_date": pl.Date,
                "implementation_date": pl.Date,
                "effective_date_basis": pl.String,
                "date_evidence_text": pl.String,
                "expiry_date": pl.Date,
                "date_confidence": pl.String,
                "old_value": pl.String,
                "new_value": pl.String,
                "unit": pl.String,
                "parameter_confidence": pl.Float64,
                "official_url": pl.String,
                "source_confidence": pl.Float64,
                "classification_confidence": pl.Float64,
                "episode_confidence": pl.Float64,
                "official_text_excerpt": pl.String,
                "is_formal_eligible": pl.Boolean,
                "extraction_method": pl.String,
                "created_at": pl.String,
            },
        )
        param_frame = _frame(
            params,
            {
                "episode_id": pl.String,
                "action_id": pl.String,
                "document_id": pl.String,
                "city": pl.String,
                "parameter_name": pl.String,
                "old_value": pl.String,
                "new_value": pl.String,
                "unit": pl.String,
                "evidence_text": pl.String,
                "parameter_confidence": pl.Float64,
                "extraction_method": pl.String,
                "created_at": pl.String,
            },
        )
        atomic_write_parquet(action_frame, self.phase_dirs["04_ACTION_EXTRACTION"] / "2016_930_ACTIONS.parquet", {"module": "episode_930", "phase": "action_extraction"}, key_columns=("action_id",))
        atomic_write_parquet(param_frame, self.phase_dirs["04_ACTION_EXTRACTION"] / "2016_930_PARAMETERS.parquet", {"module": "episode_930", "phase": "action_extraction"}, key_columns=("episode_id", "action_id", "parameter_name"))
        action_counts = Counter(
            str(value)
            for value in action_frame.get_column("document_id").drop_nulls().to_list()
        ) if not action_frame.is_empty() else Counter()
        coverage_rows = []
        eligible_documents = (
            documents.filter(pl.col("is_formal_eligible"))
            if not documents.is_empty() and "is_formal_eligible" in documents.columns
            else documents
        )
        for doc in eligible_documents.iter_rows(named=True) if not eligible_documents.is_empty() else []:
            document_id = str(doc.get("document_id") or "")
            action_count = int(action_counts.get(document_id, 0))
            coverage_rows.append(
                {
                    "episode_id": EPISODE_ID,
                    "document_id": document_id,
                    "city": doc.get("city"),
                    "eligible": True,
                    "status": "COMPLETED" if action_count > 0 else "EXCLUDED_WITH_REASON",
                    "action_count": action_count,
                    "excluded_reason": None if action_count > 0 else "NO_DETERMINISTIC_POLICY_CLAUSE",
                    "updated_at": _now(),
                }
            )
        coverage = _frame(
            coverage_rows,
            {
                "episode_id": pl.String,
                "document_id": pl.String,
                "city": pl.String,
                "eligible": pl.Boolean,
                "status": pl.String,
                "action_count": pl.Int64,
                "excluded_reason": pl.String,
                "updated_at": pl.String,
            },
        )
        atomic_write_parquet(
            coverage,
            self.phase_dirs["04_ACTION_EXTRACTION"] / "2016_930_ACTION_EXTRACTION_COVERAGE.parquet",
            {"module": "episode_930", "phase": "action_extraction_coverage"},
            key_columns=("document_id",),
        )
        _write_xlsx(action_frame, self.output / "2016_930_ACTIONS.xlsx", "actions")
        _write_xlsx(param_frame, self.output / "2016_930_PARAMETERS.xlsx", "parameters")
        result = {
            "actions_extracted": action_frame.height,
            "parameterized_actions": int(action_frame.filter(pl.col("parameter_confidence") > 0).height) if not action_frame.is_empty() else 0,
            "documents_with_actions": action_frame.get_column("document_id").n_unique() if not action_frame.is_empty() else 0,
            "eligible_total": coverage.height,
            "completed": int(coverage.filter(pl.col("status") == "COMPLETED").height) if not coverage.is_empty() else 0,
            "remaining": int(coverage.filter(pl.col("status") != "COMPLETED").height) if not coverage.is_empty() else 0,
            "excluded_with_reason": int(coverage.filter(pl.col("status") == "EXCLUDED_WITH_REASON").height) if not coverage.is_empty() else 0,
        }
        self._write_state({"stage": "ACTION_EXTRACTION", **result})
        return action_frame, param_frame, result

    def action_count_audit(self, documents: pl.DataFrame, actions: pl.DataFrame) -> dict[str, Any]:
        """Audit deterministic clause splitting on named cities plus 10 stable samples."""

        target_cities = ("北京", "南京", "深圳", "广州", "苏州", "合肥")
        document_rows = documents.to_dicts() if not documents.is_empty() else []
        selected: list[dict[str, Any]] = []
        selected_ids: set[str] = set()
        for city in target_cities:
            match = next((row for row in document_rows if city in str(row.get("city") or "") or city in str(row.get("document_title") or "")), None)
            if match is not None:
                selected.append(match)
                selected_ids.add(str(match.get("document_id")))
        remaining = sorted(
            (row for row in document_rows if str(row.get("document_id")) not in selected_ids),
            key=lambda row: _sha(row.get("document_id")),
        )
        for row in remaining[:10]:
            selected.append(row)
            selected_ids.add(str(row.get("document_id")))
        sample_ids = {str(row.get("document_id")) for row in selected}
        sampled_actions = actions.filter(pl.col("document_id").cast(pl.String).is_in(sample_ids)) if not actions.is_empty() else pl.DataFrame()
        audit_rows: list[dict[str, Any]] = []
        duplicate_groups = 0
        for doc in selected:
            doc_actions = sampled_actions.filter(pl.col("document_id") == doc.get("document_id")) if not sampled_actions.is_empty() else pl.DataFrame()
            normalized: dict[str, list[str]] = defaultdict(list)
            for action in doc_actions.iter_rows(named=True):
                key = re.sub(r"\s+", "", _text(action.get("action_text")))[:500]
                if key:
                    normalized[key].append(str(action.get("action_id")))
            duplicates = sum(1 for values in normalized.values() if len(values) > 1)
            duplicate_groups += duplicates
            audit_rows.append({"episode_id": EPISODE_ID, "document_id": doc.get("document_id"), "city": doc.get("city"), "actions": doc_actions.height, "duplicate_action_text_groups": duplicates, "over_split_suspected": duplicates > 0, "audited_at": _now()})
        frame = _frame(audit_rows)
        atomic_write_parquet(frame, self.phase_dirs["04_ACTION_EXTRACTION"] / "930_ACTION_COUNT_AUDIT.parquet", {"module": "episode_930", "phase": "action_count_audit"}, key_columns=("episode_id", "document_id"))
        result = {"documents_sampled": len(selected), "actions_sampled": sampled_actions.height if not sampled_actions.is_empty() else 0, "target_city_documents": sum(1 for row in selected if any(city in str(row.get("city") or "") for city in target_cities)), "random_documents": max(0, len(selected) - sum(1 for row in selected if any(city in str(row.get("city") or "") for city in target_cities))), "duplicate_action_text_groups": duplicate_groups, "over_split_suspected": duplicate_groups > 0}
        _atomic_json(self.output / "930_ACTION_COUNT_AUDIT.json", result)
        return result

    def targeted_recovery(self, documents: pl.DataFrame, gaps: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Run the second, gap-driven official recovery pass.

        The first pass is broad and starts from the curated corpus.  This pass
        only asks for terms implied by unresolved city/tool cells, so it does
        not repeat the full discovery matrix blindly.
        """
        if gaps.is_empty() or not self.config.run_search:
            return documents, {"targeted_queries": 0, "targeted_candidates": 0, "targeted_recovered": 0}
        provider = build_search_fallback(self.settings)
        if getattr(provider, "name", "None") == "None":
            provider = build_search_provider("ddg", None)
        terms = {
            "LIMIT_PURCHASE": "住房限购 购房资格",
            "LIMIT_RESALE": "限售 转让期限",
            "COMMERCIAL_DOWNPAYMENT": "首付比例 住房贷款",
            "PF_DOWNPAYMENT": "住房公积金 首付",
            "PF_LOAN_CEILING": "公积金贷款额度",
            "PF_OTHER": "住房公积金 政策",
            "LAND_SUPPLY": "住宅用地供应 土地出让",
            "PRICE_REGULATION": "商品房价格备案 预售",
            "MARKET_SUPERVISION": "预售资金监管 房地产市场监管",
            "DATE": "实施日期 生效",
            "SOURCE": "政府信息公开 政策文件",
        }
        queries: list[tuple[str, str, str, str]] = []
        seen: set[tuple[str, str]] = set()
        for row in gaps.iter_rows(named=True):
            city = str(row.get("city") or "")
            tool = str(row.get("policy_tool") or "")
            key = (city, tool)
            if not city or key in seen:
                continue
            seen.add(key)
            queries.append((city, tool, terms.get(tool, "房地产调控"), f"{city} {terms.get(tool, '房地产调控')} 2016 site:gov.cn"))
            if len(queries) >= self.config.max_search_queries:
                break
        current = documents.to_dicts()
        known_urls = {str(row.get("canonical_url")) for row in current if row.get("canonical_url")}
        candidates: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []
        for city, tool, term, query in queries:
            try:
                result_rows: Iterable[SearchResult] = provider.search(query, max_results=self.config.search_results_per_query)
            except Exception as exc:
                evidence.append({"episode_id": EPISODE_ID, "city": city, "policy_tool": tool, "query": query, "provider": getattr(provider, "name", type(provider).__name__), "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)[:500], "created_at": _now()})
                continue
            for item in result_rows:
                canonical = canonicalize_url(item.url)
                evidence.append({"episode_id": EPISODE_ID, "city": city, "policy_tool": tool, "query": query, "provider": getattr(provider, "name", type(provider).__name__), "result_url": item.url, "canonical_url": canonical, "title": item.title, "snippet": item.snippet, "official_candidate": _official_url(canonical), "status": "result", "created_at": _now()})
                if not _official_url(canonical) or canonical in known_urls:
                    continue
                city_row = _find_city(city, self.cities, self.aliases)
                candidates.append({"candidate_id": _id(EPISODE_ID, city, canonical, prefix="CAND930"), "episode_id": EPISODE_ID, "record_id": None, "city_id": city_row.get("city_id") if city_row else None, "city": city, "province": city_row.get("province_name") if city_row else None, "document_title": item.title, "record_date": None, "candidate_url": item.url, "canonical_url": canonical, "official_status": "targeted_search", "official_candidate": True, "source_kind": "OFFICIAL_SEARCH_CANDIDATE", "discovery_method": "targeted_gap_recovery", "discovery_evidence": f"{term}; {item.snippet}", "candidate_status": "proposed", "created_at": _now()})
                known_urls.add(canonical)
        if evidence:
            evidence_frame = _frame(evidence)
            atomic_write_parquet(evidence_frame, self.phase_dirs["03_GAP_AUDIT"] / "2016_930_TARGETED_SEARCH_EVIDENCE.parquet", {"module": "episode_930", "phase": "targeted_recovery"}, key_columns=("query", "canonical_url", "status"))
            _write_xlsx(evidence_frame, self.output / "2016_930_TARGETED_SEARCH_EVIDENCE.xlsx", "targeted_search")
        if not candidates:
            return documents, {"targeted_queries": len(queries), "targeted_candidates": 0, "targeted_recovered": 0}
        recovered, result = self.official_recovery(candidates=candidates, cities_frame=pl.DataFrame())
        merged = pl.concat([documents, recovered], how="diagonal_relaxed") if not documents.is_empty() else recovered
        merged = merged.unique(subset=["document_id"], keep="last")
        return merged, {"targeted_queries": len(queries), "targeted_candidates": len(candidates), "targeted_recovered": int(result.get("official_recovered", 0)), "targeted_search_evidence": len(evidence)}

    def classify_actions(self, documents: pl.DataFrame, actions: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Run bounded first-pass and independent review classification.

        All AI requests are persisted through the shared crash-safe audit store.
        The returned rows are advisory metadata only; deterministic action rows
        remain the formal source of dates, URLs, mechanisms and eligibility.
        """
        fields = ["episode_id", "document_id", "action_id", "pass_name", "policy_type", "policy_subtype", "direction", "mechanism_labels", "target_population", "geographic_scope", "old_value", "new_value", "unit", "effective_date_candidate", "confidence", "reason", "request_id", "request_hash", "provider", "model", "prompt_version", "prompt_hash", "prompt_tokens", "completion_tokens", "total_tokens", "estimated_cost_usd", "usage_status", "cache_hit", "status", "conflict", "created_at"]
        if actions.is_empty() or not self.config.run_ai or not self.settings.siliconflow_api_key or not self.settings.siliconflow_chat_model:
            result = {"ai_status": "not_configured" if not self.settings.siliconflow_api_key else "disabled", "ai_first_pass": 0, "ai_second_pass": 0, "ai_conflicts": 0, "ai_calls": 0, "api_cache_hits": 0, "api_cache_hit_document_ids": [], "api_network_attempts": 0, "tokens": None, "cost": None, "usage_status": "unavailable"}
            empty = _frame([], {field: pl.String for field in fields})
            atomic_write_parquet(empty, self.phase_dirs["05_API_CLASSIFICATION"] / "2016_930_API_CLASSIFICATION.parquet", {"module": "episode_930", "phase": "api_classification"}, key_columns=("request_id",))
            self._write_state({"stage": "API_CLASSIFICATION", **result})
            return empty, result
        provider = get_ai_provider(
            self.settings,
            request_timeout_override=self.config.ai_request_timeout_override,
            connect_timeout_override=self.config.ai_connect_timeout_override,
            max_retries_override=self.config.ai_max_retries_override,
        )
        audit = AIAuditStore(self.phase_dirs["05_API_CLASSIFICATION"], global_root=self.settings.outputs)
        rows: list[dict[str, Any]] = []
        call_count = 0
        first_count = 0
        second_count = 0
        conflicts = 0
        cache_hit_calls = 0
        cache_hit_document_ids: set[str] = set()
        network_attempts = 0
        token_values: list[int] = []
        for doc in documents.filter(pl.col("is_formal_eligible")).iter_rows(named=True):
            if call_count >= self.config.max_ai_calls:
                break
            doc_actions = actions.filter(pl.col("document_id") == doc.get("document_id"))
            if doc_actions.is_empty():
                continue
            action_payload = [{"action_id": row.get("action_id"), "action_text": row.get("action_text"), "deterministic_policy_type": row.get("policy_type"), "deterministic_mechanisms": row.get("mechanism_labels"), "deterministic_direction": row.get("action_direction")} for row in doc_actions.iter_rows(named=True)]
            base_input = {"document_id": doc.get("document_id"), "title": doc.get("document_title"), "issuer": doc.get("issuer"), "publication_date": doc.get("publication_date"), "actions": action_payload, "official_text_excerpt": str(doc.get("official_text") or "")[:12000]}
            first = self._ai_call(
                provider,
                audit,
                doc,
                base_input,
                "first_pass",
                ActionClassificationPayload,
                token_values,
                bypass_cache=self.config.bypass_ai_cache,
            )
            call_count += int(first[1])
            if first[3]:
                cache_hit_calls += 1
                cache_hit_document_ids.add(str(doc.get("document_id")))
            elif first[1]:
                network_attempts += int(first[1])
            first_rows = first[0]
            first_count += int(first[1])
            rows.extend(first_rows)
            if call_count >= self.config.max_ai_calls:
                break
            if not first[2]:
                continue
            review_input = {"document_id": doc.get("document_id"), "title": doc.get("document_title"), "official_text_excerpt": str(doc.get("official_text") or "")[:12000], "first_pass": first_rows}
            second = self._ai_call(
                provider,
                audit,
                doc,
                review_input,
                "second_review",
                ActionClassificationPayload,
                token_values,
                bypass_cache=self.config.bypass_ai_cache,
            )
            call_count += int(second[1])
            if second[3]:
                cache_hit_calls += 1
                cache_hit_document_ids.add(str(doc.get("document_id")))
            elif second[1]:
                network_attempts += int(second[1])
            second_count += int(second[1])
            rows.extend(second[0])
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("action_id"))].append(row)
        for _action_id, values in grouped.items():
            if len(values) >= 2 and (values[0].get("policy_type") != values[1].get("policy_type") or values[0].get("direction") != values[1].get("direction")):
                conflicts += 1
                for value in values:
                    value["conflict"] = True
            else:
                for value in values:
                    value["conflict"] = False
        frame = _frame(rows)
        if frame.is_empty():
            frame = _frame([], {field: pl.String for field in fields})
        atomic_write_parquet(frame, self.phase_dirs["05_API_CLASSIFICATION"] / "2016_930_API_CLASSIFICATION.parquet", {"module": "episode_930", "phase": "api_classification"}, key_columns=("request_id", "action_id", "pass_name"))
        _write_xlsx(frame, self.output / "2016_930_API_CLASSIFICATION.xlsx", "classification")
        usage_status = "available" if token_values else "unavailable"
        result = {"ai_status": "operational", "ai_first_pass": first_count, "ai_second_pass": second_count, "ai_conflicts": conflicts, "ai_calls": call_count, "api_cache_hits": cache_hit_calls, "api_cache_hit_document_ids": sorted(cache_hit_document_ids), "api_network_attempts": network_attempts, "tokens": sum(token_values) if token_values else None, "cost": None, "usage_status": usage_status}
        self._write_state({"stage": "API_CLASSIFICATION", **result})
        return frame, result

    def _ai_call(
        self,
        provider: Any,
        audit: AIAuditStore,
        doc: dict[str, Any],
        payload: dict[str, Any],
        pass_name: str,
        schema: type[BaseModel],
        token_values: list[int],
        *,
        bypass_cache: bool = False,
    ) -> tuple[list[dict[str, Any]], int, bool, bool]:
        prompt_version = "episode_930_actions_v1"
        user = _json(payload)
        prompt_hash = _sha(user)
        cache_key = _sha("|".join((EPISODE_ID, str(doc.get("document_id")), pass_name, prompt_version, prompt_hash, self.settings.siliconflow_chat_model)))
        request_hash = cache_key
        if bypass_cache:
            request_hash = _sha("|".join((cache_key, "RECOVERY_NETWORK_PROBE", _now())))
        request_id = _id(EPISODE_ID, doc.get("document_id"), pass_name, request_hash, prefix="AI930")
        audit_payload = {"request_id": request_id, "run_id": EPISODE_ID, "slot_id": str(doc.get("document_id")), "city_id": str(doc.get("city_id") or ""), "source_role": "historical_episode", "provider": "siliconflow", "model": self.settings.siliconflow_chat_model, "model_version": self.settings.siliconflow_chat_model, "prompt_version": prompt_version, "schema_version": EP930_ACTION_SCHEMA_VERSION, "prompt_hash": prompt_hash, "request_hash": request_hash, "cache_key": cache_key, "cache_bypassed": bypass_cache, "probe_type": "RECOVERY_NETWORK_PROBE" if bypass_cache else None, "input_summary": {"pass_name": pass_name, "document_id": doc.get("document_id"), "action_count": len(payload.get("actions", payload.get("first_pass", [])))}}
        reservation, existing = audit.reserve(audit_payload)
        if not bypass_cache and reservation == "reused" and existing:
            response_payload = existing.get("response_payload") or {}
            parsed = validate_structured_payload(response_payload, ActionClassificationPayload)
            return [self._classification_row(item.model_dump(mode="json"), request_id, request_hash, pass_name, existing, cache_hit=True) for item in parsed.actions], 1, True, True
        if reservation == "in_flight":
            return [], 0, False, False
        audit.start(audit_payload)
        system = "You classify already recovered official policy text for a historical research database. Return JSON matching the schema. Do not invent dates, URLs, or facts; use null when not evidenced. The deterministic action_id and mechanism labels are authoritative; flag disagreement in reason."
        started = time.perf_counter()
        try:
            value, trace = provider.structured(model=self.settings.siliconflow_chat_model, system=system, user=user, schema=schema)
            payload_json = value.model_dump(mode="json")
            total = (trace.prompt_tokens or 0) + (trace.completion_tokens or 0) if trace.prompt_tokens is not None and trace.completion_tokens is not None else None
            if total is not None:
                token_values.append(int(total))
            completed = audit.complete(
                request_id,
                response_hash=trace.raw_response_hash,
                response_payload=payload_json,
                prompt_tokens=trace.prompt_tokens,
                completion_tokens=trace.completion_tokens,
                total_tokens=total,
                estimated_cost_usd=None,
                cache_hit=False,
                usage_status="available" if total is not None else "unavailable",
                transport_started=trace.transport_started,
                dns_ok=True if trace.http_status is not None else None,
                connect_ok=True if trace.http_status is not None else None,
                http_status=trace.http_status,
                response_received=trace.response_received,
                response_bytes=trace.response_bytes,
                latency_ms=round(trace.latency_seconds * 1000, 3),
                timeout_type=None,
                json_parse_ok=trace.json_parse_ok,
                schema_valid=trace.schema_valid,
                schema_errors=[],
                provider_error_code=None,
                provider_error_message_sanitized=None,
                failure_class=None,
                configured_read_timeout=trace.configured_read_timeout,
                configured_connect_timeout=trace.configured_connect_timeout,
                max_retries=trace.max_retries,
            )
            return [self._classification_row(item.model_dump(mode="json"), request_id, request_hash, pass_name, completed, cache_hit=False) for item in value.actions], 1, True, False
        except Exception as exc:
            error_type = getattr(exc, "parse_status", None) or type(exc).__name__
            diagnostics = classify_ai_failure(
                exc,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
            diagnostics.update({
                "configured_read_timeout": getattr(provider, "configured_read_timeout", None),
                "configured_connect_timeout": getattr(provider, "configured_connect_timeout", None),
                "max_retries": getattr(provider, "configured_max_retries", None),
            })
            audit.fail(
                request_id,
                error_type=str(error_type),
                error_message=diagnostics["provider_error_message_sanitized"],
                diagnostics=diagnostics,
            )
            return [], 1, False, False

    @staticmethod
    def _classification_row(item: dict[str, Any], request_id: str, request_hash: str, pass_name: str, audit_record: dict[str, Any], *, cache_hit: bool) -> dict[str, Any]:
        return {"episode_id": EPISODE_ID, "document_id": audit_record.get("slot_id"), "action_id": item.get("action_id"), "pass_name": pass_name, "policy_type": item.get("policy_type"), "policy_subtype": item.get("policy_subtype"), "direction": item.get("direction"), "mechanism_labels": item.get("mechanism_labels") or [], "target_population": item.get("target_population"), "geographic_scope": item.get("geographic_scope"), "old_value": item.get("old_value"), "new_value": item.get("new_value"), "unit": item.get("unit"), "effective_date_candidate": item.get("effective_date_candidate"), "confidence": item.get("confidence"), "reason": item.get("reason"), "request_id": request_id, "request_hash": request_hash, "provider": audit_record.get("provider"), "model": audit_record.get("model"), "prompt_version": audit_record.get("prompt_version"), "prompt_hash": audit_record.get("prompt_hash"), "prompt_tokens": audit_record.get("prompt_tokens"), "completion_tokens": audit_record.get("completion_tokens"), "total_tokens": audit_record.get("total_tokens"), "estimated_cost_usd": audit_record.get("estimated_cost_usd"), "usage_status": audit_record.get("usage_status", "unavailable"), "cache_hit": cache_hit, "status": "advisory", "conflict": False, "created_at": _now()}

    def date_audit(self, documents: pl.DataFrame, actions: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for doc in documents.iter_rows(named=True) if not documents.is_empty() else []:
            effective_date = doc.get("effective_date")
            basis = str(doc.get("effective_date_basis") or ("NO_EXPLICIT_EFFECTIVE_DATE" if effective_date is None else "EXPLICIT_EFFECTIVE_DATE"))
            rows.append({"episode_id": EPISODE_ID, "document_id": doc.get("document_id"), "action_id": None, "city": doc.get("city"), "announcement_date": doc.get("announcement_date"), "publication_date": doc.get("publication_date"), "effective_date": effective_date, "implementation_date": doc.get("implementation_date"), "expiry_date": doc.get("expiry_date"), "source_date": doc.get("publication_date") or doc.get("announcement_date"), "date_confidence": "HIGH" if effective_date is not None else "LOW", "effective_date_basis": basis, "date_evidence_text": doc.get("date_evidence_text"), "notes": "publication date is effective only when the text explicitly says so" if basis == "PUBLICATION_DATE_EFFECTIVE" else "explicit effective-date wording" if effective_date is not None else "no explicit effective-date evidence", "created_at": _now()})
        for action in actions.iter_rows(named=True) if not actions.is_empty() else []:
            effective_date = action.get("effective_date")
            basis = str(action.get("effective_date_basis") or ("NO_EXPLICIT_EFFECTIVE_DATE" if effective_date is None else "ACTION_SPECIFIC_EFFECTIVE_DATE"))
            rows.append({"episode_id": EPISODE_ID, "document_id": action.get("document_id"), "action_id": action.get("action_id"), "city": action.get("city"), "announcement_date": action.get("announcement_date"), "publication_date": action.get("publication_date"), "effective_date": effective_date, "implementation_date": action.get("implementation_date"), "expiry_date": action.get("expiry_date"), "source_date": action.get("publication_date") or action.get("announcement_date"), "date_confidence": action.get("date_confidence") or ("HIGH" if effective_date is not None else "LOW"), "effective_date_basis": basis, "date_evidence_text": action.get("date_evidence_text"), "notes": "action-specific evidence is retained separately from document date" if basis == "ACTION_SPECIFIC_EFFECTIVE_DATE" else "publication date is effective only when the text explicitly says so" if basis == "PUBLICATION_DATE_EFFECTIVE" else "no explicit effective-date evidence", "created_at": _now()})
        frame = _frame(rows)
        atomic_write_parquet(frame, self.phase_dirs["06_DATE_VERIFICATION"] / "2016_930_DATE_AUDIT.parquet", {"module": "episode_930", "phase": "date_verification"}, key_columns=("episode_id", "document_id", "action_id", "date_confidence"))
        _write_xlsx(frame, self.output / "2016_930_DATE_AUDIT.xlsx", "date_audit")
        counts = Counter(str(row.get("date_confidence") or "LOW") for row in rows)
        basis_counts = Counter(str(row.get("effective_date_basis") or "NO_EXPLICIT_EFFECTIVE_DATE") for row in rows)
        result = {"date_rows": len(rows), "high_confidence_dates": counts.get("HIGH", 0), "medium_confidence_dates": counts.get("MEDIUM", 0), "low_confidence_dates": counts.get("LOW", 0), "explicit_effective_dates": basis_counts.get("EXPLICIT_EFFECTIVE_DATE", 0), "publication_date_effective": basis_counts.get("PUBLICATION_DATE_EFFECTIVE", 0), "action_specific_effective_dates": basis_counts.get("ACTION_SPECIFIC_EFFECTIVE_DATE", 0), "effective_date_missing": basis_counts.get("NO_EXPLICIT_EFFECTIVE_DATE", 0)}
        self._write_state({"stage": "DATE_VERIFICATION", **result})
        return frame, result

    def deduplicate(self, documents: pl.DataFrame, actions: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
        doc_rows = documents.to_dicts()
        doc_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in doc_rows:
            key = (str(row.get("canonical_url") or ""), str(row.get("content_hash") or ""))
            doc_groups[key].append(row)
        for group in doc_groups.values():
            group_id = _id(EPISODE_ID, group[0].get("canonical_url"), group[0].get("content_hash"), prefix="DUP930")
            for index, row in enumerate(group):
                row["duplicate_group_id"] = group_id
                row["dedup_status"] = "canonical" if index == 0 else "official_reprint_or_duplicate"
        action_rows = actions.to_dicts()
        action_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in action_rows:
            key = (str(row.get("document_id") or ""), _text(row.get("action_text"))[:500], ",".join(sorted(row.get("mechanism_labels") or [])))
            action_groups[key].append(row)
        for group in action_groups.values():
            group_id = _id(EPISODE_ID, group[0].get("document_id"), group[0].get("action_text"), prefix="ACTDUP930")
            for index, row in enumerate(group):
                row["action_duplicate_group_id"] = group_id
                row["dedup_status"] = "canonical" if index == 0 else "duplicate_action"
        doc_frame = _frame(doc_rows)
        action_frame = _frame(action_rows)
        atomic_write_parquet(doc_frame, self.phase_dirs["07_DEDUP"] / "2016_930_DOCUMENTS_DEDUP.parquet", {"module": "episode_930", "phase": "dedup"}, key_columns=("document_id",))
        atomic_write_parquet(action_frame, self.phase_dirs["07_DEDUP"] / "2016_930_ACTIONS_DEDUP.parquet", {"module": "episode_930", "phase": "dedup"}, key_columns=("action_id",))
        result = {"documents": len(doc_rows), "document_duplicate_groups": len(doc_groups), "duplicate_documents": sum(max(0, len(group) - 1) for group in doc_groups.values()), "actions": len(action_rows), "action_duplicate_groups": len(action_groups), "duplicate_actions": sum(max(0, len(group) - 1) for group in action_groups.values())}
        self._write_state({"stage": "DEDUP", **result})
        return doc_frame, action_frame, result

    def timeline_and_manual_queue(self, documents: pl.DataFrame, actions: pl.DataFrame, gaps: pl.DataFrame, ai_rows: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        timeline_rows = []
        for row in actions.iter_rows(named=True) if not actions.is_empty() else []:
            timeline_rows.append({"date": row.get("announcement_date") or row.get("publication_date"), "city": row.get("city"), "document_id": row.get("document_id"), "action_id": row.get("action_id"), "policy_type": row.get("policy_type"), "action_direction": row.get("action_direction"), "announcement_date": row.get("announcement_date"), "effective_date": row.get("effective_date")})
        timeline = _frame(timeline_rows)
        atomic_write_parquet(timeline, self.phase_dirs["06_DATE_VERIFICATION"] / "2016_930_TIMELINE.parquet", {"module": "episode_930", "phase": "timeline"}, key_columns=("document_id", "action_id"))
        _write_xlsx(timeline, self.output / "2016_930_TIMELINE.xlsx", "timeline")
        _png_timeline(self.output / "2016_930_DIFFUSION_TIMELINE.png", timeline_rows)
        manual: list[dict[str, Any]] = []
        for row in documents.iter_rows(named=True) if not documents.is_empty() else []:
            if float(row.get("source_confidence") or 0) < 0.7 or row.get("effective_date") is None:
                manual.append({"review_id": _id(EPISODE_ID, row.get("document_id"), "document", prefix="REV930"), "episode_id": EPISODE_ID, "city": row.get("city"), "document_id": row.get("document_id"), "action_id": None, "question": "请确认官方原文、公告日期与明确生效/实施日期；不能以发布日期替代生效日期。", "ai_suggestion": None, "deterministic_evidence": row.get("official_url"), "options": "ACCEPT_OFFICIAL;REJECT;EDIT_DATES", "impact": "影响文档纳入、时间线和计量日期。", "status": "PENDING", "created_at": _now()})
        for row in actions.iter_rows(named=True) if not actions.is_empty() else []:
            if float(row.get("parameter_confidence") or 0) < 0.7 or row.get("action_direction") == "UNKNOWN":
                manual.append({"review_id": _id(EPISODE_ID, row.get("action_id"), "action", prefix="REV930"), "episode_id": EPISODE_ID, "city": row.get("city"), "document_id": row.get("document_id"), "action_id": row.get("action_id"), "question": "请确认原子动作拆分、方向和 old/new 参数；AI 结果只能作为建议。", "ai_suggestion": None, "deterministic_evidence": row.get("official_text_excerpt"), "options": "ACCEPT;REJECT;EDIT", "impact": "影响动作分类和参数化结果。", "status": "PENDING", "created_at": _now()})
        if not gaps.is_empty():
            for row in gaps.filter(pl.col("state") != "POLICY_FOUND").head(500).iter_rows(named=True):
                manual.append({"review_id": _id(EPISODE_ID, row.get("city"), row.get("policy_tool"), row.get("audit_pass"), prefix="REV930"), "episode_id": EPISODE_ID, "city": row.get("city"), "document_id": None, "action_id": None, "question": f"请判断 {row.get('city')} 的 {row.get('policy_tool')} 是否确实未发现正式政策，或需要继续恢复历史官方入口。", "ai_suggestion": None, "deterministic_evidence": row.get("evidence"), "options": "CONFIRMED_NOT_FOUND;POSSIBLE_MISSING;INSUFFICIENT_SOURCE", "impact": "影响城市×政策工具覆盖矩阵和后续补搜范围。", "status": "PENDING", "created_at": _now()})
        seen_review_ids: set[str] = set()
        for index, row in enumerate(manual, 1):
            review_id = str(row.get("review_id") or "")
            if review_id in seen_review_ids:
                row["review_id"] = _id(EPISODE_ID, review_id, index, prefix="REV930")
            seen_review_ids.add(str(row.get("review_id")))
        manual_frame = _frame(manual)
        atomic_write_parquet(manual_frame, self.phase_dirs["08_MANUAL_REVIEW"] / "2016_930_MANUAL_REVIEW.parquet", {"module": "episode_930", "phase": "manual_review"}, key_columns=("review_id",))
        _write_xlsx(manual_frame, self.output / "2016_930_MANUAL_REVIEW.xlsx", "review_queue")
        result = {"timeline_rows": timeline.height, "manual_review_pending": manual_frame.height, "manual_review_document_rows": manual_frame.filter(pl.col("action_id").is_null()).height if not manual_frame.is_empty() else 0, "manual_review_action_rows": manual_frame.filter(pl.col("action_id").is_not_null()).height if not manual_frame.is_empty() else 0}
        self._write_state({"stage": "MANUAL_REVIEW_QUEUE", **result})
        return timeline, {**result, "manual_frame": manual_frame}

    def formal_import(self, documents: pl.DataFrame, actions: pl.DataFrame, params: pl.DataFrame, gaps: pl.DataFrame, matrix: pl.DataFrame) -> dict[str, Any]:
        """Publish only this episode's validated, keyed curated snapshots."""
        if not self.config.apply:
            return {"formal_import": "SKIPPED_APPLY_FALSE", "rows": {}}
        context = {"module": "episode_930", "episode_id": EPISODE_ID, "writer": "single_episode_import"}
        targets = {
            "documents": (self.settings.curated / "policy_episode_documents.parquet", documents, "document_id"),
            "actions": (self.settings.curated / "policy_episode_actions.parquet", actions, "action_id"),
            "parameters": (self.settings.curated / "policy_episode_parameters.parquet", params, "action_id"),
            "gaps": (self.settings.curated / "policy_episode_gaps.parquet", gaps, "gap_key"),
            "matrix": (self.settings.curated / "policy_episode_city_policy_matrix.parquet", matrix, "matrix_key"),
        }
        rows: dict[str, int] = {}
        new_rows: dict[str, int] = {}
        for name, (path, frame, key) in targets.items():
            publish = frame
            if name == "gaps" and not frame.is_empty():
                publish = frame.with_columns(pl.col("gap_id").alias("gap_key"))
            if name == "matrix" and not frame.is_empty():
                publish = frame.with_columns(pl.concat_str([pl.col("audit_pass").cast(pl.String), pl.col("city").fill_null(""), pl.col("policy_tool").fill_null("")], separator="|").alias("matrix_key"))
            if publish.is_empty() and path.exists():
                rows[name] = read_parquet_snapshot(path).height
                new_rows[name] = 0
                continue
            existing = read_parquet_snapshot(path) if path.exists() else pl.DataFrame()
            existing_keys = (
                set(existing.get_column(key).drop_nulls().cast(pl.String).to_list())
                if not existing.is_empty() and key in existing.columns
                else set()
            )
            incoming_keys = (
                set(publish.get_column(key).drop_nulls().cast(pl.String).to_list())
                if not publish.is_empty() and key in publish.columns
                else set()
            )
            merged = merge_and_replace_parquet(path, publish, (key,), context)
            rows[name] = merged.height
            new_rows[name] = len(incoming_keys - existing_keys)
        index = pl.DataFrame([{"episode_id": EPISODE_ID, "episode_name": EPISODE_NAME, "episode_direction": EPISODE_DIRECTION, "document_count": documents.height, "action_count": actions.height, "parameter_count": params.height, "updated_at": _now()}])
        atomic_write_parquet(index, self.settings.curated / "policy_episode_index.parquet", context, key_columns=("episode_id",))
        index.write_json(self.phase_dirs["10_DASHBOARD"] / "POLICY_EPISODE_INDEX.json")
        result = {
            "formal_import": "APPLIED",
            "rows": rows,
            "new_rows": new_rows,
            "new_action_rows": int(new_rows.get("actions", 0)),
            "curated_tables": [str(path) for path, _, _ in targets.values()]
            + [str(self.settings.curated / "policy_episode_index.parquet")],
        }
        self._write_state({"stage": "FORMAL_IMPORT", **result})
        return result

    def final_export(
        self,
        documents: pl.DataFrame,
        actions: pl.DataFrame,
        params: pl.DataFrame,
        date_frame: pl.DataFrame,
        manual: pl.DataFrame,
        gap2: pl.DataFrame,
        metrics: dict[str, Any],
        *,
        api_rows: pl.DataFrame | None = None,
    ) -> dict[str, Any]:
        docs = documents.select([name for name in ("document_id", "record_id", "city", "province", "document_title", "document_number", "issuer", "official_url", "canonical_url", "official_source", "source_confidence", "publication_date", "announcement_date", "effective_date", "implementation_date", "expiry_date", "effective_date_basis", "date_evidence_text", "official_text") if name in documents.columns]) if not documents.is_empty() else pl.DataFrame()
        export = actions.join(docs, on="document_id", how="left", suffix="_document") if not actions.is_empty() else pl.DataFrame()
        if not params.is_empty() and "action_id" in params.columns and not export.is_empty():
            parameter_columns = [name for name in ("parameter_name", "old_value", "new_value", "unit") if name in params.columns]
            if parameter_columns:
                parameter_summary = params.select(["action_id", *parameter_columns]).unique(subset=["action_id"], keep="first")
                export = export.join(parameter_summary, on="action_id", how="left", suffix="_parameter")
        if api_rows is not None and not api_rows.is_empty() and "action_id" in api_rows.columns and "pass_name" in api_rows.columns:
            api_status_rows: list[dict[str, Any]] = []
            for row in api_rows.to_dicts():
                action_id = row.get("action_id")
                if action_id in (None, ""):
                    continue
                target = next((item for item in api_status_rows if item["action_id"] == action_id), None)
                if target is None:
                    target = {"action_id": action_id, "api_pass1_status": None, "api_pass2_status": None}
                    api_status_rows.append(target)
                status = "SUCCESS" if str(row.get("status") or "").lower() == "advisory" else str(row.get("status") or "FAILED")
                if row.get("pass_name") == "first_pass":
                    target["api_pass1_status"] = status
                elif row.get("pass_name") == "second_review":
                    target["api_pass2_status"] = status
            if api_status_rows and not export.is_empty():
                export = export.join(pl.DataFrame(api_status_rows), on="action_id", how="left", suffix="_api")
        if export.is_empty():
            export = pl.DataFrame(schema={"episode_id": pl.String, "episode_name": pl.String})
        if "direction" not in export.columns and "action_direction" in export.columns:
            export = export.with_columns(pl.col("action_direction").alias("direction"))
        if "date_type" not in export.columns and "effective_date_basis" in export.columns:
            export = export.with_columns(pl.col("effective_date_basis").alias("date_type"))
        required = {
            "episode_id": EPISODE_ID,
            "episode_name": EPISODE_NAME,
            "province": None,
            "city": None,
            "document_id": None,
            "action_id": None,
            "document_title": None,
            "document_number": None,
            "issuer": None,
            "policy_type": None,
            "policy_subtype": None,
            "mechanism_labels": None,
            "direction": None,
            "announcement_date": None,
            "publication_date": None,
            "effective_date": None,
            "implementation_date": None,
            "recommended_treatment_date": None,
            "date_type": None,
            "date_evidence_text": None,
            "date_confidence": None,
            "parameter_name": None,
            "old_value": None,
            "new_value": None,
            "unit": None,
            "target_population": None,
            "geographic_scope": None,
            "bundle_id": None,
            "bundle_size": None,
            "co_treatment_types": None,
            "official_url": None,
            "canonical_url": None,
            "source_confidence": None,
            "classification_confidence": None,
            "episode_confidence": None,
            "api_pass1_status": None,
            "api_pass2_status": None,
            "manual_review_required": False,
            "export_status": str(metrics.get("export_status") or ("ANALYSIS_READY" if metrics.get("analysis_ready") else "PROVISIONAL")),
        }
        for column, default in required.items():
            if column not in export.columns:
                export = export.with_columns(pl.lit(default).alias(column))
        export = export.select(list(required))
        _write_xlsx(export, self.output / "2016_930_FINAL_EXPORT.xlsx", "action_export")
        _xlsx_frame(export).write_csv(self.output / "2016_930_FINAL_EXPORT.csv")
        atomic_write_parquet(export, self.phase_dirs["09_IMPORT"] / "2016_930_FINAL_EXPORT.parquet", {"module": "episode_930", "phase": "export"}, key_columns=("action_id",))
        export_bytes = (self.output / "2016_930_FINAL_EXPORT.csv").read_bytes()
        metadata = {
            "episode_id": EPISODE_ID,
            "export_status": required["export_status"],
            "generated_at": _now(),
            "row_count": export.height,
            "city_count": export.get_column("city").drop_nulls().n_unique() if "city" in export.columns else 0,
            "document_count": export.get_column("document_id").drop_nulls().n_unique() if "document_id" in export.columns else 0,
            "action_count": export.get_column("action_id").drop_nulls().n_unique() if "action_id" in export.columns else 0,
            "effective_date_coverage": round(float(export.get_column("effective_date").is_not_null().sum()) / max(export.height, 1), 6),
            "high_conf_date_share": round(float((export.get_column("date_confidence") == "HIGH").sum()) / max(export.height, 1), 6),
            "sha256": hashlib.sha256(export_bytes).hexdigest(),
            "source_run_ids": metrics.get("source_run_ids") or [],
        }
        _atomic_json(self.output / "2016_930_FINAL_EXPORT_METADATA.json", metadata)
        if required["export_status"] == "ANALYSIS_READY":
            _xlsx_frame(export).write_csv(self.output / "2016_930_ANALYSIS_READY.csv")
            _atomic_json(self.output / "2016_930_ANALYSIS_READY_METADATA.json", metadata)
        if required["export_status"] == "FINAL":
            _xlsx_frame(export).write_csv(self.output / "2016_930_FINAL.csv")
        report = self._final_report(documents, actions, params, date_frame, manual, gap2, metrics, export)
        (self.output / "2016_930_FINAL_REPORT.md").write_text(report, encoding="utf-8")
        readme = self._episode_readme()
        (self.output / "2016_930_README.md").write_text(readme, encoding="utf-8")
        return {"export_rows": export.height, "export_status": required["export_status"], "final_report": str(self.output / "2016_930_FINAL_REPORT.md"), "final_export": str(self.output / "2016_930_FINAL_EXPORT.xlsx"), "metadata": metadata}

    def _final_report(self, documents: pl.DataFrame, actions: pl.DataFrame, params: pl.DataFrame, dates: pl.DataFrame, manual: pl.DataFrame, gaps: pl.DataFrame, metrics: dict[str, Any], export: pl.DataFrame) -> str:
        type_counts = Counter(str(value or "OTHER") for value in actions.get_column("policy_type").to_list()) if not actions.is_empty() and "policy_type" in actions.columns else Counter()
        formal_eligible = (
            int(documents.get_column("is_formal_eligible").fill_null(False).sum())
            if "is_formal_eligible" in documents.columns
            else 0
        )
        evidence_line = (
            f"- 本轮 live GovernmentDirectClient HTTP 200 恢复: `{metrics.get('official_recovered', 0)}`；"
            f"Curated 官方版本证据: `{metrics.get('curated_official', 0)}`；未把超时或失败响应宣称为原始证据。"
            if not metrics.get("official_recovered", 0)
            else "- live HTTP 200 官方证据按 content hash、URL、状态码和检索时间保留；写入使用 `.part` 后原子替换。"
        )
        lines = [
            f"# {EPISODE_NAME} 专项补库最终报告",
            "",
            f"- episode_id: `{EPISODE_ID}`",
            f"- direction: `{EPISODE_DIRECTION}`（episode方向与action方向分离）",
            f"- 核心窗口: `{CORE_START}` — `{CORE_END}`；扩展窗口: `{EXTENDED_START}` — `{EXTENDED_END}`；来源窗口: `{PROVENANCE_START}` — `{PROVENANCE_END}`",
            "",
            "## 覆盖与证据",
            "",
            f"- reference_city_count: `{metrics.get('reference_city_count', 0)}`（不是预设总数）",
            f"- official_recovered_city_count: `{metrics.get('official_recovered_city_count', 0)}`",
            f"- unresolved_city_count: `{metrics.get('unresolved_city_count', 0)}`",
            f"- document_count: `{documents.height}`；formal eligible: `{formal_eligible}`",
            f"- action_count: `{actions.height}`；parameterized_actions: `{params.select('action_id').n_unique() if not params.is_empty() else 0}`",
            f"- high/medium/low date rows: `{metrics.get('high_confidence_dates', 0)}` / `{metrics.get('medium_confidence_dates', 0)}` / `{metrics.get('low_confidence_dates', 0)}`",
            f"- effective-date basis: explicit=`{metrics.get('explicit_effective_dates', 0)}`; publication-based=`{metrics.get('publication_date_effective', 0)}`; action-specific=`{metrics.get('action_specific_effective_dates', 0)}`; missing=`{metrics.get('effective_date_missing', 0)}`",
            f"- manual_review_pending: `{manual.height}`",
            f"- gap_audit_pass_1 unresolved: `{metrics.get('gap1_unresolved_cells', 0)}`；pass_2 unresolved: `{metrics.get('gap2_unresolved_cells', 0)}`",
            f"- typed gap register: `{metrics.get('gap_register')}`",
            "",
            "## 动作类型",
            "",
        ]
        for tool in POLICY_TOOLS:
            lines.append(f"- {tool}: `{type_counts.get(tool, 0)}`")
        lines.extend([
            f"- OTHER: `{type_counts.get('OTHER', 0)}`",
            "",
            "## API 与质量门禁",
            "",
            f"- AI first pass: `{metrics.get('ai_first_pass', 0)}`；second pass: `{metrics.get('ai_second_pass', 0)}`；conflicts: `{metrics.get('ai_conflicts', 0)}`",
            f"- AI tokens: `{metrics.get('tokens')}`；estimated cost: `{metrics.get('cost')}`；usage_status: `{metrics.get('usage_status')}`",
            "- AI 仅产生结构化分类建议；URL、官方性、日期和正式纳入由确定性证据控制。",
            "- 媒体与搜索结果只作为 discovery evidence；没有官方原文的城市/工具单元保持 POSSIBLE_MISSING 或 INSUFFICIENT_SOURCE。",
            "- 生效日期只有原文明确表达时才填充；不能由发布日期静默推断。",
            "",
            "## 入库与 Dashboard",
            "",
            f"- formal_import: `{metrics.get('formal_import')}`",
            f"- dashboard episode filter/export: `{metrics.get('dashboard_episode_filter', 'available after Curated index refresh')}`",
            f"- action-level export rows: `{export.height}`",
            evidence_line,
            "",
            "## 未完成事项",
            "",
            "- 本专项不宣称算法意义上的全国历史完整；缺口、未知日期、低置信动作和需要人工判断的城市保留在审核队列。",
            "- 现有生产 crawler / Dashboard 未被停止；专项数据通过独立 episode Curated 表进入正式数据层。",
        ])
        return "\n".join(lines) + "\n"

    @staticmethod
    def _episode_readme() -> str:
        return f"""# {EPISODE_NAME}\n\n本目录是 `{EPISODE_ID}` 的可审计专项补库输出。\n\n## 流程\n\n`Discovery → Official Recovery → Gap Audit 1 → Targeted Recovery → Gap Audit 2 → Action Extraction → Date Verification → AI Classification/Review → Dedup → Formal Import → Dashboard Export`\n\n搜索和媒体材料只是发现证据；正式动作必须绑定官方证据。AI 不能写入官方性、生效日期或正式资格。生效日期没有原文明确依据时保持空值并进入人工队列。\n\n## 主要文件\n\n- `2016_930_DOCUMENTS.xlsx`：文档级官方恢复结果。\n- `2016_930_ACTIONS.xlsx`：原子动作级结果；一个动作可以有多个 mechanism label。\n- `2016_930_DATE_AUDIT.xlsx`：announcement/publication/effective/implementation/expiry 分离审计。\n- `2016_930_FINAL_EXPORT.xlsx`：后续计量使用的动作级导出。\n- `2016_930_MANUAL_REVIEW.xlsx`：机器无法可靠决定的事项。\n- `2016_930_DIFFUSION_TIMELINE.png`：公告/实施扩散时间线。\n\n## 状态边界\n\n`POLICY_FOUND` 不等于每一个工具都存在；`POSSIBLE_MISSING`、`INSUFFICIENT_SOURCE` 和 `CONFIRMED_NOT_FOUND` 保持区分。\n"""

    def run(self) -> dict[str, Any]:
        started = _now()
        self.scope()
        discovery = self.discover()
        city_path = self.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet"
        candidate_path = self.phase_dirs["01_DISCOVERY"] / "930_DISCOVERY_CANDIDATES.parquet"
        cities = read_parquet_snapshot(city_path)
        candidates = read_parquet_snapshot(candidate_path).to_dicts()
        documents, recovery = self.official_recovery(candidates=candidates, cities_frame=cities)
        matrix1, gap1, gap1_metrics = self.gap_audit(documents, pass_number=1)
        targeted_docs, targeted = self.targeted_recovery(documents, gap1)
        matrix2, gap2, gap2_metrics = self.gap_audit(targeted_docs, pass_number=2)
        actions, params, extraction = self.extract_actions(targeted_docs)
        ai_rows, ai_metrics = self.classify_actions(targeted_docs, actions)
        dates, date_metrics = self.date_audit(targeted_docs, actions)
        dedup_docs, dedup_actions, dedup_metrics = self.deduplicate(targeted_docs, actions)
        timeline, manual_metrics = self.timeline_and_manual_queue(dedup_docs, dedup_actions, gap2, ai_rows)
        import_metrics = self.formal_import(dedup_docs, dedup_actions, params, gap2, matrix2)
        metrics: dict[str, Any] = {**discovery, **recovery, **gap1_metrics, **gap2_metrics, **targeted, **extraction, **ai_metrics, **date_metrics, **dedup_metrics, **{key: value for key, value in manual_metrics.items() if key != "manual_frame"}, **import_metrics, "gap1_unresolved_cells": gap1_metrics.get("unresolved_cells", 0), "gap2_unresolved_cells": gap2_metrics.get("unresolved_cells", 0), "reference_city_count": discovery.get("reference_city_count", 0), "official_recovered_city_count": len({row.get("city") for row in dedup_docs.filter(pl.col("is_formal_eligible")).iter_rows(named=True)}) if not dedup_docs.is_empty() else 0, "unresolved_city_count": len({row.get("city") for row in cities.filter(pl.col("mentioned_as_930_city")).iter_rows(named=True) if row.get("city") not in set(dedup_docs.get_column("city").drop_nulls().to_list())}) if not dedup_docs.is_empty() else int(cities.filter(pl.col("mentioned_as_930_city")).height), "started_at": started, "completed_at": _now(), "dashboard_episode_filter": "available"}
        final = self.final_export(dedup_docs, dedup_actions, params, dates, manual_metrics["manual_frame"], gap2, metrics)
        metrics.update(final)
        manifest = {"episode_id": EPISODE_ID, "episode_name": EPISODE_NAME, "generated_at": _now(), "metrics": {key: value for key, value in metrics.items() if key != "manual_frame"}, "files": {str(path.relative_to(self.output)): _sha(path.read_bytes()) for path in sorted(self.output.rglob("*")) if path.is_file() and path.name != "STATE.json"}}
        (self.phase_dirs["11_FINAL_AUDIT"] / "2016_930_MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        (self.output / "2016_930_FINAL_AUDIT.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        self._write_state({"stage": "COMPLETE", **{key: value for key, value in metrics.items() if key != "manual_frame"}, "manifest": str(self.output / "2016_930_FINAL_AUDIT.json")})
        return metrics

    def status(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"episode_id": EPISODE_ID, "status": "NOT_STARTED", "output": str(self.output)}
        return json.loads(self.state_path.read_text(encoding="utf-8"))


def episode_930_actions_for_dashboard(settings: Settings | None = None, *, episode_id: str = EPISODE_ID) -> pl.DataFrame:
    settings = settings or Settings.discover()
    path = settings.curated / "policy_episode_actions.parquet"
    if not path.exists():
        return pl.DataFrame()
    frame = read_parquet_snapshot(path)
    if "episode_id" in frame.columns:
        frame = frame.filter(pl.col("episode_id") == episode_id)
    return frame


__all__ = [
    "CORE_END",
    "CORE_START",
    "EPISODE_DIRECTION",
    "EPISODE_ID",
    "EPISODE_NAME",
    "Episode930Pipeline",
    "EpisodeConfig",
    "POLICY_TOOLS",
    "episode_930_actions_for_dashboard",
]
