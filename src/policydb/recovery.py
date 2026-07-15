from __future__ import annotations

import json
import re
from collections import Counter
from datetime import UTC, date, datetime
from typing import Literal
from urllib.parse import quote_plus, urljoin, urlsplit

import duckdb
import httpx
import polars as pl
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from rapidfuzz.fuzz import ratio

from policydb.crawl.checkpoint import append_unique
from policydb.crawl.dedup import canonicalize_url, content_sha256
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.parser import parse_document
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


class RecoveryRecord(BaseModel):
    record_id: str
    title: str = ""
    document_number: str | None = None
    issuing_agency: str | None = None
    record_date: date | None = None
    region: str | None = None
    full_text: str | None = None


class SourceCandidate(BaseModel):
    url: str
    title: str = ""
    document_number: str | None = None
    issuing_agency: str | None = None
    publication_date: date | None = None
    region: str | None = None
    text: str | None = None
    official_status: Literal[
        "official", "official_reprint", "authoritative_media", "general_media", "unknown"
    ] = "unknown"
    source_id: str | None = None


class CandidateScore(BaseModel):
    url: str
    score: float = Field(ge=0, le=1)
    components: dict[str, float]
    has_critical_conflict: bool
    evidence: list[str]


def _similarity(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    return ratio(left, right) / 100


def score_source_candidate(record: RecoveryRecord, candidate: SourceCandidate) -> CandidateScore:
    components = {
        "title": _similarity(record.title, candidate.title),
        "document_number": 1.0
        if record.document_number
        and candidate.document_number
        and record.document_number == candidate.document_number
        else 0.0,
        "issuing_agency": _similarity(record.issuing_agency, candidate.issuing_agency),
        "date": 1.0
        if record.record_date and candidate.publication_date == record.record_date
        else 0.5
        if record.record_date
        and candidate.publication_date
        and abs((candidate.publication_date - record.record_date).days) <= 7
        else 0.0,
        "region": _similarity(record.region, candidate.region),
        "text": _similarity((record.full_text or "")[:3000], (candidate.text or "")[:3000]),
        "official": {
            "official": 1.0,
            "official_reprint": 0.85,
            "authoritative_media": 0.6,
            "general_media": 0.35,
            "unknown": 0.0,
        }[candidate.official_status],
    }
    weights = {
        "title": 0.25,
        "document_number": 0.2,
        "issuing_agency": 0.15,
        "date": 0.1,
        "region": 0.1,
        "text": 0.1,
        "official": 0.1,
    }
    conflicts = []
    if record.document_number and candidate.document_number:
        if record.document_number != candidate.document_number:
            conflicts.append("document_number_conflict")
    if record.record_date and candidate.publication_date:
        if abs((candidate.publication_date - record.record_date).days) > 31:
            conflicts.append("publication_date_conflict")
    available = {
        "title": bool(record.title and candidate.title),
        "document_number": bool(record.document_number and candidate.document_number),
        "issuing_agency": bool(record.issuing_agency and candidate.issuing_agency),
        "date": bool(record.record_date and candidate.publication_date),
        "region": bool(record.region and candidate.region),
        "text": bool(record.full_text and candidate.text),
        "official": True,
    }
    denominator = sum(weight for name, weight in weights.items() if available[name]) or 1.0
    score = (
        sum(
            components[name] * weight
            for name, weight in weights.items()
            if available[name]
        )
        / denominator
    )
    if conflicts:
        score = min(score, 0.69)
    evidence = [f"{name}={value:.3f}" for name, value in components.items() if value]
    evidence.extend(conflicts)
    return CandidateScore(
        url=canonicalize_url(candidate.url),
        score=round(score, 4),
        components=components,
        has_critical_conflict=bool(conflicts),
        evidence=evidence,
    )


class SourceRecoveryEngine:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        fetcher: RespectfulFetcher | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.fetcher = fetcher or RespectfulFetcher()

    def rank(
        self, record: RecoveryRecord, candidates: list[SourceCandidate]
    ) -> list[tuple[SourceCandidate, CandidateScore]]:
        ranked = [(candidate, score_source_candidate(record, candidate)) for candidate in candidates]
        return sorted(ranked, key=lambda item: item[1].score, reverse=True)

    def discover(self, record: RecoveryRecord, *, limit: int = 10) -> list[SourceCandidate]:
        registry_path = self.settings.curated / "source_registry.parquet"
        if not registry_path.exists():
            return []
        registry = (
            pl.read_parquet(registry_path)
            .filter(
                pl.col("crawl_enabled")
                & pl.col("search_url_template").is_not_null()
            )
            .sort("priority")
        )
        values = {
            "title": quote_plus(record.title),
            "keyword": quote_plus(record.document_number or record.title),
            "document_number": quote_plus(record.document_number or ""),
            "region": quote_plus(record.region or ""),
            "year": record.record_date.year if record.record_date else "",
        }
        found: list[SourceCandidate] = []
        seen: set[str] = set()
        for source in registry.iter_rows(named=True):
            try:
                search_url = str(source["search_url_template"]).format(**values)
                page = self.fetcher.fetch(search_url)
                soup = BeautifulSoup(page.body, "html.parser")
                base_domain = str(source.get("domain") or "").lower()
                for link in soup.select("a[href]"):
                    candidate_url = canonicalize_url(urljoin(page.final_url, link.get("href")))
                    if not candidate_url or candidate_url in seen:
                        continue
                    candidate_domain = urlsplit(candidate_url).hostname
                    if base_domain and candidate_domain and not candidate_domain.endswith(base_domain):
                        continue
                    seen.add(candidate_url)
                    fetched = self.fetcher.fetch(candidate_url)
                    parsed = parse_document(fetched.body, fetched.content_type, fetched.final_url)
                    text = parsed.get("full_text") or ""
                    if len(text) < 80:
                        continue
                    found.append(
                        SourceCandidate(
                            url=fetched.final_url,
                            title=parsed.get("title") or link.get_text(" ", strip=True),
                            document_number=_document_number(text),
                            publication_date=_date_from_text(text),
                            region=source.get("province") or record.region,
                            text=text,
                            official_status=(
                                "official"
                                if source.get("official_status") == "official"
                                else "official_reprint"
                                if source.get("official_status") in {"official_reprint", "secondary_only"}
                                else "unknown"
                            ),
                            source_id=source["source_id"],
                        )
                    )
                    if len(found) >= limit:
                        return found
            except (KeyError, ValueError, PermissionError, httpx.HTTPError):
                continue
        return found

    def recover(
        self,
        record: RecoveryRecord,
        candidates: list[SourceCandidate],
        *,
        threshold: float = 0.9,
    ) -> dict:
        ranked = self.rank(record, candidates)
        if not ranked:
            return {"status": "no_candidate", "score": 0.0}
        candidate, score = ranked[0]
        runner_up = ranked[1][1].score if len(ranked) > 1 else 0.0
        if score.has_critical_conflict or score.score < threshold or score.score - runner_up < 0.05:
            return {
                "status": "candidate_conflict" if score.has_critical_conflict or score.score - runner_up < 0.05 else "low_confidence",
                "score": score.score,
                "candidate_url": candidate.url,
                "evidence": score.evidence,
            }
        fetched = self.fetcher.fetch(candidate.url)
        parsed = parse_document(fetched.body, fetched.content_type, fetched.final_url)
        now = datetime.now(UTC)
        extension = ".pdf" if parsed["document_type"] == "pdf" else ".html"
        raw_dir = self.settings.root / "data" / "raw" / "webpages" / now.strftime("%Y/%m")
        raw_dir.mkdir(parents=True, exist_ok=True)
        digest = content_sha256(fetched.body)
        raw_path = raw_dir / f"{digest}{extension}"
        if not raw_path.exists():
            raw_path.write_bytes(fetched.body)
        metadata = raw_path.with_suffix(raw_path.suffix + ".metadata.json")
        if not metadata.exists():
            metadata.write_text(
                json.dumps(
                    {
                        "record_id": record.record_id,
                        "requested_url": candidate.url,
                        "final_url": fetched.final_url,
                        "retrieved_at": fetched.retrieved_at.isoformat(),
                        "sha256": digest,
                        "candidate_score": score.model_dump(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        source_id = candidate.source_id or stable_id(
            urlsplit(candidate.url).hostname or candidate.url, prefix="RECOVSRC"
        )
        if not candidate.source_id:
            domain = (urlsplit(candidate.url).hostname or "").lower()
            append_unique(
                self.settings.curated / "source_registry.parquet",
                [
                    {
                        "source_id": source_id,
                        "source_name": domain or candidate.url,
                        "source_type": "government"
                        if candidate.official_status == "official"
                        else "recovered_source",
                        "source_role": "recovered_canonical_candidate",
                        "official_status": candidate.official_status,
                        "agency_type": "unknown",
                        "priority": 0 if candidate.official_status == "official" else 2,
                        "domain": domain,
                        "jurisdiction_level": "unknown",
                        "province": None,
                        "city_id": None,
                        "agency_name": candidate.issuing_agency or domain,
                        "seed_urls": [candidate.url],
                        "list_page_urls": [],
                        "search_url_template": None,
                        "parser_adapter": "recovered_document",
                        "crawl_enabled": False,
                        "rate_limit": 0.5,
                        "last_success_at": now.isoformat(),
                        "notes": "由来源恢复模块建立；默认不启用周期抓取",
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }
                ],
                "source_id",
            )
        version_id = stable_id(record.record_id, digest, prefix="DOCVER")
        append_unique(
            self.settings.curated / "policy_document_versions.parquet",
            [
                {
                    "document_version_id": version_id,
                    "record_id": record.record_id,
                    "crawl_item_id": stable_id(record.record_id, candidate.url, prefix="RECOV"),
                    "source_id": source_id,
                    "canonical_url": canonicalize_url(candidate.url),
                    "final_url": fetched.final_url,
                    "content_sha256": digest,
                    "local_path": str(raw_path.relative_to(self.settings.root)),
                    "content_type": fetched.content_type,
                    "http_status": fetched.status_code,
                    "title": parsed.get("title"),
                    "extracted_text": parsed.get("full_text") or "",
                    "parse_status": parsed.get("parse_status"),
                    "is_material_change": True,
                    "first_seen_at": fetched.retrieved_at.isoformat(),
                    "last_seen_at": fetched.retrieved_at.isoformat(),
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            ],
            "document_version_id",
        )
        append_unique(
            self.settings.curated / "policy_sources.parquet",
            [
                {
                    "policy_source_id": stable_id(record.record_id, candidate.url, prefix="POLSRC"),
                    "record_id": record.record_id,
                    "source_id": source_id,
                    "source_url": candidate.url,
                    "normalized_url": canonicalize_url(candidate.url),
                    "source_role": "recovered_canonical_candidate",
                    "is_canonical": candidate.official_status == "official",
                    "official_status": candidate.official_status,
                    "needs_review": False,
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            ],
            "policy_source_id",
        )
        return {
            "status": "auto_recovered_official"
            if candidate.official_status == "official"
            else "auto_recovered_secondary",
            "score": score.score,
            "candidate_url": candidate.url,
            "local_path": str(raw_path.relative_to(self.settings.root)),
            "content_sha256": digest,
            "parsed": parsed,
            "evidence": score.evidence,
        }


def _date_from_text(text: str) -> date | None:
    match = re.search(r"(20\d{2})[年\-/\.](\d{1,2})[月\-/\.](\d{1,2})日?", text[:5000])
    if not match:
        return None
    try:
        return date(*(int(value) for value in match.groups()))
    except ValueError:
        return None


def _document_number(text: str) -> str | None:
    match = re.search(r"[\u4e00-\u9fff]{1,8}[〔\[（(]?20\d{2}[〕\]）)]?\d{1,5}号", text[:5000])
    return match.group(0) if match else None


def recover_review_sources(
    settings: Settings | None = None,
    *,
    limit: int = 20,
    engine: SourceRecoveryEngine | None = None,
) -> dict:
    """Recover only existing URL-backed review tasks; never searches the open web blindly."""
    settings = settings or Settings.discover()
    engine = engine or SourceRecoveryEngine(settings)
    with duckdb.connect(str(settings.database), read_only=True) as con:
        rows = con.execute(
            """SELECT t.task_id,r.record_id,coalesce(r.title,''),p.document_number,
                      r.record_date,coalesce(g.geography_original,''),coalesce(r.summary,''),
                      r.primary_source_url
                 FROM manual_review_tasks t JOIN records r USING(record_id)
                 LEFT JOIN policies p USING(record_id)
                 LEFT JOIN record_jurisdictions g USING(record_id)
                WHERE t.status='pending' AND t.review_type IN ('missing_source','invalid_url')
                  AND coalesce(t.automation_status,'pending_diagnosis') IN
                    ('pending_diagnosis','manual_review_required')
                QUALIFY row_number() OVER(PARTITION BY t.task_id ORDER BY g.geography_original)=1
                LIMIT ?""",
            [limit],
        ).fetchall()
    updates: dict[str, str] = {}
    outcomes = Counter()
    details = []
    for task_id, record_id, title, document_number, record_date, region, summary, url in rows:
        try:
            recovery_record = RecoveryRecord(
                record_id=record_id,
                title=title,
                document_number=document_number,
                record_date=record_date,
                region=region or None,
                full_text=summary or None,
            )
            candidates: list[SourceCandidate] = []
            if url and str(url).startswith(("http://", "https://")):
                probe = engine.fetcher.fetch(url)
                parsed = parse_document(probe.body, probe.content_type, probe.final_url)
                candidate_text = parsed.get("full_text") or ""
                domain = (urlsplit(probe.final_url).hostname or "").lower()
                official_status = "official" if domain.endswith(".gov.cn") or domain in {
                    "gov.cn",
                    "www.gov.cn",
                    "www.mohurd.gov.cn",
                    "www.pbc.gov.cn",
                    "www.nfra.gov.cn",
                } else "official_reprint" if domain.endswith("people.com.cn") else "unknown"
                candidates.append(
                    SourceCandidate(
                        url=probe.final_url,
                        title=parsed.get("title") or "",
                        document_number=_document_number(candidate_text),
                        publication_date=_date_from_text(candidate_text),
                        region=region or None,
                        text=candidate_text,
                        official_status=official_status,
                    )
                )
            candidates.extend(engine.discover(recovery_record, limit=10 - len(candidates)))
            result = engine.recover(recovery_record, candidates)
            status = result["status"]
            outcomes[status] += 1
            if status in {"auto_recovered_official", "auto_recovered_secondary"}:
                recovered_text = result["parsed"].get("full_text") or ""
                if recovered_text:
                    updates[record_id] = recovered_text
                with duckdb.connect(str(settings.database)) as con:
                    con.execute(
                        """UPDATE manual_review_tasks SET automation_status=?,diagnosis='source_content_missing',
                                  diagnostic_confidence=?,completeness_score=?,recovered_source_url=?,
                                  diagnosis_evidence=?,updated_at=current_timestamp WHERE task_id=?""",
                        [
                            status,
                            result["score"],
                            result["parsed"].get("completeness_score"),
                            result["candidate_url"],
                            json.dumps(result["evidence"], ensure_ascii=False),
                            task_id,
                        ],
                    )
            details.append({"task_id": task_id, "record_id": record_id, **result})
        except (OSError, PermissionError, ValueError, httpx.HTTPError) as exc:
            outcomes["fetch_failed"] += 1
            details.append(
                {"task_id": task_id, "record_id": record_id, "status": "fetch_failed", "error": type(exc).__name__}
            )
    if updates:
        records_path = settings.curated / "records.parquet"
        records = pl.read_parquet(records_path)
        records = records.with_columns(
            pl.col("record_id")
            .replace_strict(updates, default=pl.col("full_text"))
            .alias("full_text")
        )
        records.write_parquet(records_path, compression="zstd")
        from policydb.query.database import build_database

        build_database(settings)
    return {
        "attempted": len(rows),
        "recovered": len(updates),
        "outcomes": dict(outcomes),
        "details": details,
    }
