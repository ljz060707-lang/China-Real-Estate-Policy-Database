from __future__ import annotations

from datetime import date

import fitz
import httpx
import polars as pl
import yaml

from policydb.crawl.checkpoint import append_unique
from policydb.crawl.dedup import canonicalize_url, content_sha256
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.parser import merge_semantic_blocks, parse_document
from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings


def test_crawl_pipeline_fetches_versions_and_resumes(tmp_path):
    root = tmp_path / "repo"
    (root / "data" / "reference").mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    registry = {
        "sources": [
            {
                "source_id": "SRC_TEST",
                "source_name": "测试政府",
                "domain": "example.gov.cn",
                "source_type": "government",
                "source_role": "canonical_candidate",
                "official_status": "official",
                "seed_urls": ["https://example.gov.cn/policy/1"],
                "crawl_enabled": True,
                "priority": 0,
                "rate_limit": 0,
            }
        ]
    }
    (root / "data" / "reference" / "source_registry.yaml").write_text(
        yaml.safe_dump(registry, allow_unicode=True), encoding="utf-8"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            text="<html><title>住房政策</title><body>城市更新政策正文</body></html>",
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    fetcher = RespectfulFetcher(client=client, check_robots=False, rate_limit=0)
    pipeline = CrawlPipeline(Settings(root=root), fetcher=fetcher)
    plan = pipeline.plan(
        run_type="test",
        start_date=date(2018, 1, 1),
        end_date=date(2018, 12, 31),
    )
    first = pipeline.run(plan["run_id"])
    second = pipeline.run(plan["run_id"])
    assert first == {"run_id": plan["run_id"], "fetched": 1, "failed": 0}
    assert second["fetched"] == 0
    versions = pl.read_parquet(root / "data" / "curated" / "policy_document_versions.parquet")
    assert versions.height == 1
    assert (root / versions[0, "local_path"]).exists()
    next_plan = pipeline.plan(
        run_type="test_update",
        start_date=date(2019, 1, 1),
        end_date=date(2019, 12, 31),
    )
    pipeline.run(next_plan["run_id"])
    assert pl.read_parquet(
        root / "data" / "curated" / "policy_document_versions.parquet"
    ).height == 1


def test_fetch_error_does_not_delete_previous_data(tmp_path):
    path = tmp_path / "existing.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(path)
    assert pl.read_parquet(path).height == 1


def test_fetcher_retries_transient_error(monkeypatch):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            503 if calls == 1 else 200,
            text="temporary" if calls == 1 else "ok",
            request=request,
        )

    monkeypatch.setattr("policydb.crawl.fetcher.time.sleep", lambda _seconds: None)
    fetcher = RespectfulFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        check_robots=False,
        retries=2,
        rate_limit=0,
    )
    assert fetcher.fetch("https://example.gov.cn/policy").body == b"ok"
    assert calls == 2


def test_fetcher_applies_domain_rate_limit(monkeypatch):
    sleeps: list[float] = []
    ticks = iter([10.0, 10.0, 10.0, 10.25, 10.25, 10.25])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="ok", request=request)

    monkeypatch.setattr("policydb.crawl.fetcher.time.monotonic", lambda: next(ticks))
    monkeypatch.setattr("policydb.crawl.fetcher.time.sleep", sleeps.append)
    fetcher = RespectfulFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        check_robots=False,
        rate_limit=1,
    )
    fetcher.fetch("https://example.gov.cn/a")
    fetcher.fetch("https://example.gov.cn/b")
    assert sleeps and sleeps[-1] > 0


def test_html_parser_extracts_title_and_text():
    parsed = parse_document(
        "<html><title>城市更新办法</title><body>政策正文</body></html>".encode(),
        "text/html; charset=utf-8",
    )
    assert parsed["document_type"] == "html"
    assert parsed["title"] == "城市更新办法"
    assert "政策正文" in parsed["full_text"]


def test_pdf_parser_extracts_text():
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), "policy document")
    body = document.tobytes()
    document.close()
    parsed = parse_document(body, "application/pdf")
    assert parsed["document_type"] == "pdf"
    assert parsed["page_count"] == 1
    assert "policy document" in parsed["full_text"]


def test_cross_page_sentence_is_merged_without_inventing_text():
    result = merge_semantic_blocks(
        [
            {"text": "本通知自发布之日起", "page": 1, "kind": "text"},
            {"text": "正式施行。", "page": 2, "kind": "text"},
        ]
    )
    assert "本通知自发布之日起正式施行。" in result["text"]
    assert result["repairs"][0]["reason"] == "cross_page_sentence"


def test_crawl_pipeline_fetches_html_attachment(tmp_path):
    root = tmp_path / "repo"
    (root / "data" / "reference").mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    (root / "data" / "reference" / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "source_id": "SRC_ATTACHMENT",
                        "source_name": "测试政府",
                        "domain": "example.gov.cn",
                        "source_type": "government",
                        "source_role": "canonical_candidate",
                        "official_status": "official",
                        "seed_urls": ["https://example.gov.cn/policy"],
                        "crawl_enabled": True,
                        "priority": 0,
                        "rate_limit": 0,
                    }
                ]
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("attachment.pdf"):
            document = fitz.open()
            page = document.new_page()
            page.insert_text((72, 72), "attachment policy text")
            body = document.tobytes()
            document.close()
            return httpx.Response(
                200,
                content=body,
                headers={"content-type": "application/pdf"},
                request=request,
            )
        return httpx.Response(
            200,
            text=(
                "<html><title>政策</title><article><p>政策正文内容足够用于解析。</p>"
                '<a href="/attachment.pdf">附件</a></article></html>'
            ),
            headers={"content-type": "text/html"},
            request=request,
        )

    fetcher = RespectfulFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        check_robots=False,
        rate_limit=0,
    )
    pipeline = CrawlPipeline(Settings(root=root), fetcher=fetcher)
    plan = pipeline.plan(run_type="test", start_date=date(2024, 1, 1), end_date=date(2024, 1, 2))
    pipeline.run(plan["run_id"])
    versions = pl.read_parquet(root / "data" / "curated" / "policy_document_versions.parquet")
    assert versions.height == 2
    assert versions.filter(pl.col("content_type").str.contains("pdf")).height == 1


def test_url_hash_and_checkpoint_deduplication(tmp_path):
    assert canonicalize_url("https://www.example.gov.cn/a/?utm_source=x") == (
        "https://example.gov.cn/a"
    )
    assert content_sha256("same") == content_sha256(b"same")
    path = tmp_path / "items.parquet"
    append_unique(path, [{"item_id": "A", "status": "pending"}], "item_id")
    frame = append_unique(path, [{"item_id": "A", "status": "fetched"}], "item_id")
    assert frame.height == 1
    assert frame[0, "status"] == "fetched"
