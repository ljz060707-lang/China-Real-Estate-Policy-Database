from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import yaml

from policydb.settings import Settings
from policydb.transform.normalization import normalize_url, stable_id

URL_PATTERN = re.compile(r"https?://[^\s<>\"'，；]+", re.IGNORECASE)
OFFICIAL_DOMAINS = {
    "gov.cn": ("中国政府网", "central_government"),
    "mohurd.gov.cn": ("住房和城乡建设部", "housing"),
    "pbc.gov.cn": ("中国人民银行", "finance"),
    "nfra.gov.cn": ("国家金融监督管理总局", "finance"),
    "cbirc.gov.cn": ("原中国银保监会", "finance"),
    "csrc.gov.cn": ("中国证监会", "finance"),
    "ndrc.gov.cn": ("国家发展和改革委员会", "development_reform"),
    "chinatax.gov.cn": ("国家税务总局", "tax"),
}
MEDIA_DOMAINS = {
    "mp.weixin.qq.com",
    "baijiahao.baidu.com",
    "sohu.com",
    "qq.com",
    "163.com",
}


def _domain(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def _classify_domain(domain: str) -> dict:
    exact = next(
        ((name, agency) for suffix, (name, agency) in OFFICIAL_DOMAINS.items() if domain == suffix),
        None,
    )
    if exact:
        name, agency = exact
        return {
            "source_name": name,
            "source_type": "government",
            "source_role": "canonical_candidate",
            "official_status": "official",
            "agency_type": agency,
            "priority": 0,
        }
    if domain.endswith(".gov.cn"):
        return {
            "source_name": domain,
            "source_type": "government",
            "source_role": "canonical_candidate",
            "official_status": "official",
            "agency_type": "local_government",
            "priority": 0,
        }
    if domain in MEDIA_DOMAINS or any(domain.endswith("." + item) for item in MEDIA_DOMAINS):
        return {
            "source_name": domain,
            "source_type": "lead",
            "source_role": "discovery_only",
            "official_status": "secondary_only",
            "agency_type": "media_or_aggregator",
            "priority": 3,
        }
    return {
        "source_name": domain,
        "source_type": "media_or_unknown",
        "source_role": "supporting",
        "official_status": "unknown",
        "agency_type": "unknown",
        "priority": 2,
    }


def _configured_url_columns(settings: Settings) -> dict[str, set[str]]:
    config = yaml.safe_load(
        (settings.root / "config" / "excel_sheet_map.yaml").read_text(encoding="utf-8")
    )
    return {
        sheet: set(spec.get("url_columns", []))
        for sheet, spec in config["sheets"].items()
        if spec.get("url_columns")
    }


def bootstrap_sources_from_excel(
    workbook: Path | None = None, settings: Settings | None = None
) -> dict:
    settings = settings or Settings.discover()
    mappings = _configured_url_columns(settings)
    urls_by_domain: dict[str, set[str]] = {}
    provenance: dict[str, set[str]] = {}
    for parquet in (settings.root / "data" / "staging" / "excel").glob("*.parquet"):
        frame = pl.read_parquet(parquet)
        sheet = frame["source_sheet_name"][0]
        columns = mappings.get(sheet)
        if not columns:
            continue
        candidates = frame.filter(pl.col("source_column_letter").is_in(columns))
        for cell in candidates.iter_rows(named=True):
            for found in URL_PATTERN.findall(str(cell["cell_value"] or "")):
                url = normalize_url(found.rstrip("。).）"))
                domain = _domain(url or "")
                if not domain:
                    continue
                urls_by_domain.setdefault(domain, set()).add(url)
                provenance.setdefault(domain, set()).add(f"{sheet}!{cell['source_cell']}")
    now = datetime.now(UTC).isoformat()
    sources = []
    for domain, urls in sorted(urls_by_domain.items()):
        classification = _classify_domain(domain)
        sources.append(
            {
                "source_id": "SRC_" + hashlib.sha256(domain.encode()).hexdigest()[:16].upper(),
                **classification,
                "domain": domain,
                "jurisdiction_level": "unknown",
                "province": None,
                "city_id": None,
                "agency_name": classification["source_name"],
                "seed_urls": sorted(urls),
                "list_page_urls": [],
                "search_url_template": None,
                "parser_adapter": "generic_government"
                if classification["priority"] == 0
                else "list_page",
                "crawl_enabled": False,
                "rate_limit": 0.5,
                "last_success_at": None,
                "notes": "Excel来源位置：" + "；".join(sorted(provenance[domain])[:20]),
                "created_at": now,
                "updated_at": now,
            }
        )
    registry = {
        "version": 1,
        "generated_at": now,
        "source_workbook": str(workbook) if workbook else "staging_excel_cells",
        "source_count": len(sources),
        "sources": sources,
    }
    output = settings.root / "data" / "reference" / "source_registry.yaml"
    output.write_text(
        yaml.safe_dump(registry, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    flat = [
        {**source, "seed_urls": source["seed_urls"], "list_page_urls": source["list_page_urls"]}
        for source in sources
    ]
    pl.DataFrame(flat, infer_schema_length=None).write_parquet(
        settings.curated / "source_registry.parquet", compression="zstd"
    )
    source_by_domain = {source["domain"]: source for source in sources}
    records = pl.read_parquet(settings.curated / "records.parquet")
    policy_sources = []
    for record in records.iter_rows(named=True):
        url = normalize_url(record.get("primary_source_url"))
        domain = _domain(url or "")
        source = source_by_domain.get(domain)
        if not source or not url:
            continue
        policy_sources.append(
            {
                "policy_source_id": stable_id(record["record_id"], url, prefix="POLSRC"),
                "record_id": record["record_id"],
                "source_id": source["source_id"],
                "source_url": record.get("primary_source_url"),
                "normalized_url": url,
                "source_role": "canonical"
                if source["official_status"] == "official"
                else "supporting",
                "is_canonical": source["official_status"] == "official",
                "official_status": source["official_status"],
                "needs_review": source["official_status"] != "official",
                "created_at": now,
                "updated_at": now,
            }
        )
    policy_source_schema = {
        "policy_source_id": pl.String,
        "record_id": pl.String,
        "source_id": pl.String,
        "source_url": pl.String,
        "normalized_url": pl.String,
        "source_role": pl.String,
        "is_canonical": pl.Boolean,
        "official_status": pl.String,
        "needs_review": pl.Boolean,
        "created_at": pl.String,
        "updated_at": pl.String,
    }
    pl.DataFrame(policy_sources, schema=policy_source_schema).write_parquet(
        settings.curated / "policy_sources.parquet", compression="zstd"
    )
    return {
        "source_count": len(sources),
        "domain_count": len(urls_by_domain),
        "url_count": sum(len(value) for value in urls_by_domain.values()),
        "official_domain_count": sum(source["priority"] == 0 for source in sources),
        "secondary_only_domain_count": sum(
            source["official_status"] == "secondary_only" for source in sources
        ),
        "registry_path": str(output),
        "policy_source_relation_count": len(policy_sources),
    }
