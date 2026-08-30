from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.fetcher import FetchTask, HostAwareFetchPool, RespectfulFetcher


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def _urls_from_registry(registry_path: Path, limit: int) -> list[str]:
    registry = pl.read_parquet(registry_path)
    if "crawl_enabled" in registry.columns:
        registry = registry.filter(pl.col("crawl_enabled") == True)  # noqa: E712
    candidates: list[str] = []
    for row in registry.iter_rows(named=True):
        urls = row.get("list_page_urls") or row.get("seed_urls") or []
        if isinstance(urls, str):
            urls = [urls]
        url = next((str(value) for value in urls if str(value).startswith("http")), None)
        if url is None:
            homepage = str(row.get("homepage_url") or "")
            url = homepage if homepage.startswith("http") else None
        if url:
            candidates.append(url)

    # Preserve registry order while taking one URL per host first.  This keeps
    # the real sample deterministic and representative of cross-host crawling.
    selected: list[str] = []
    seen_urls: set[str] = set()
    seen_hosts: set[str] = set()
    for url in candidates:
        host = urlsplit(url).netloc.lower()
        if url in seen_urls or not host or host in seen_hosts:
            continue
        selected.append(url)
        seen_urls.add(url)
        seen_hosts.add(host)
        if len(selected) >= limit:
            return selected
    for url in candidates:
        if url in seen_urls:
            continue
        selected.append(url)
        seen_urls.add(url)
        if len(selected) >= limit:
            break
    return selected


def _load_or_create_manifest(
    manifest_path: Path,
    registry_path: Path,
    limit: int,
) -> dict[str, object]:
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if len(manifest.get("urls", [])) != limit:
            raise ValueError(
                f"fixed manifest contains {len(manifest.get('urls', []))} URLs; expected {limit}"
            )
        return manifest
    urls = _urls_from_registry(registry_path, limit)
    if len(urls) != limit:
        raise ValueError(f"only {len(urls)} eligible URLs found; expected {limit}")
    payload: dict[str, object] = {
        "schema_version": "crpd-crawl-throughput-benchmark-v1",
        "created_at": datetime.now(UTC).isoformat(),
        "registry_path": str(registry_path),
        "sample_policy": "enabled_registry_unique_hosts_first",
        "url_count": len(urls),
        "urls": urls,
        "urls_sha256": hashlib.sha256("\n".join(urls).encode()).hexdigest(),
    }
    _atomic_json(manifest_path, payload)
    return payload


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(round((len(ordered) - 1) * fraction), len(ordered) - 1)
    return round(ordered[index], 3)


def run_serial(
    urls: list[str],
    *,
    timeout: float,
    connect_timeout: float,
) -> tuple[list[dict[str, object]], float]:
    fetcher = RespectfulFetcher(
        timeout=timeout,
        connect_timeout=connect_timeout,
        retries=1,
        rate_limit=0.2,
        check_robots=True,
    )
    results: list[dict[str, object]] = []
    started = time.perf_counter()
    try:
        for ordinal, url in enumerate(urls, start=1):
            request_started = time.perf_counter()
            row: dict[str, object] = {
                "ordinal": ordinal,
                "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                "host": urlsplit(url).netloc.lower(),
            }
            try:
                result = fetcher.fetch(url)
                row.update(
                    {
                        "status": "success",
                        "http_status": result.status_code,
                        "response_bytes": result.response_bytes,
                        "content_type": result.content_type,
                    }
                )
            except Exception as exc:  # benchmark records production error classes
                row.update(
                    {
                        "status": "failed",
                        "error_type": type(exc).__name__,
                    }
                )
            row["latency_seconds"] = round(time.perf_counter() - request_started, 3)
            results.append(row)
            if ordinal % 5 == 0 or ordinal == len(urls):
                print(f"serial {ordinal}/{len(urls)}", flush=True)
    finally:
        fetcher.client.close()
    return results, time.perf_counter() - started


def run_host_aware(
    urls: list[str],
    *,
    timeout: float,
    connect_timeout: float,
    concurrency: int,
    per_host_concurrency: int,
) -> tuple[list[dict[str, object]], float, dict[str, int | float | None]]:
    fetcher = RespectfulFetcher(
        timeout=timeout,
        connect_timeout=connect_timeout,
        retries=1,
        rate_limit=0.2,
        check_robots=True,
    )
    tasks = [
        FetchTask(key=str(ordinal), url=url, rate_limit=0.2)
        for ordinal, url in enumerate(urls, start=1)
    ]
    results: list[dict[str, object]] = []
    started = time.perf_counter()
    pool_metrics: dict[str, int | float | None]
    try:
        with HostAwareFetchPool(
            fetcher,
            tasks,
            max_workers=concurrency,
            per_host_concurrency=per_host_concurrency,
        ) as pool:
            for ordinal, (url, task) in enumerate(zip(urls, tasks, strict=True), start=1):
                row: dict[str, object] = {
                    "ordinal": ordinal,
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "host": urlsplit(url).netloc.lower(),
                }
                try:
                    result = pool.get(task.key)
                    row.update(
                        {
                            "status": "success",
                            "http_status": result.status_code,
                            "response_bytes": result.response_bytes,
                            "content_type": result.content_type,
                        }
                    )
                except Exception as exc:  # benchmark records production error classes
                    row.update(
                        {
                            "status": "failed",
                            "error_type": type(exc).__name__,
                        }
                    )
                results.append(row)
                if ordinal % 5 == 0 or ordinal == len(urls):
                    print(f"host-aware {ordinal}/{len(urls)}", flush=True)
            pool_metrics = pool.metrics()
    finally:
        fetcher.client.close()
    return results, time.perf_counter() - started, pool_metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark the CRPD real-URL fetch lane.")
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--connect-timeout", type=float, default=4.0)
    parser.add_argument("--mode", choices=("serial", "host-aware"), default="serial")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--per-host-concurrency", type=int, default=1)
    args = parser.parse_args()

    manifest = _load_or_create_manifest(args.manifest, args.registry, args.limit)
    urls = [str(value) for value in manifest["urls"]]
    pool_metrics: dict[str, int | float | None] | None = None
    if args.mode == "serial":
        results, wall_seconds = run_serial(
            urls,
            timeout=args.timeout,
            connect_timeout=args.connect_timeout,
        )
    else:
        results, wall_seconds, pool_metrics = run_host_aware(
            urls,
            timeout=args.timeout,
            connect_timeout=args.connect_timeout,
            concurrency=args.concurrency,
            per_host_concurrency=args.per_host_concurrency,
        )
    latencies = [
        float(row["latency_seconds"])
        for row in results
        if row.get("latency_seconds") is not None
    ]
    successes = [row for row in results if row["status"] == "success"]
    response_bytes = sum(int(row.get("response_bytes") or 0) for row in successes)
    summary = {
        "schema_version": "crpd-crawl-throughput-benchmark-result-v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "serial_baseline" if args.mode == "serial" else "host_aware_optimized",
        "manifest_path": str(args.manifest),
        "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "url_count": len(urls),
        "successes": len(successes),
        "failures": len(results) - len(successes),
        "wall_seconds": round(wall_seconds, 3),
        "requests_per_minute": round(len(results) * 60 / wall_seconds, 3),
        "successful_documents_per_minute": round(len(successes) * 60 / wall_seconds, 3),
        "bytes_per_minute": round(response_bytes * 60 / wall_seconds, 3),
        "latency_mean_seconds": round(statistics.mean(latencies), 3) if latencies else None,
        "latency_p50_seconds": (
            pool_metrics.get("latency_p50_seconds")
            if pool_metrics is not None
            else _percentile(latencies, 0.50)
        ),
        "latency_p95_seconds": (
            pool_metrics.get("latency_p95_seconds")
            if pool_metrics is not None
            else _percentile(latencies, 0.95)
        ),
        "global_concurrency": args.concurrency if args.mode == "host-aware" else 1,
        "per_host_concurrency": args.per_host_concurrency,
        "timeout_seconds": args.timeout,
        "connect_timeout_seconds": args.connect_timeout,
        "results": results,
    }
    _atomic_json(args.output, summary)
    print(json.dumps({key: value for key, value in summary.items() if key != "results"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
