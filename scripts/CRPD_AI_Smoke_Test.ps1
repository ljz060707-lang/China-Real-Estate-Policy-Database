param(
    [string]$RunId = "CRAWLRUN_739B7E76B4BC264A2D62",
    [int]$SampleSize = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Windows PowerShell 5.1 pipes text to native programs using a legacy encoding by default.
# Force UTF-8 and avoid piping Python source directly to the interpreter.
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

# uv will use the project's .venv. Remove a stale active-environment marker to avoid warnings.
if ($env:VIRTUAL_ENV) {
    $activeName = Split-Path -Leaf $env:VIRTUAL_ENV
    if ($activeName -ne ".venv") {
        Remove-Item Env:VIRTUAL_ENV -ErrorAction SilentlyContinue
    }
}

$Root = (Get-Location).Path
if (-not (Test-Path (Join-Path $Root "pyproject.toml"))) {
    throw "请先 cd 到 CRPD 项目根目录后再运行本脚本。"
}

if ($SampleSize -lt 1 -or $SampleSize -gt 50) {
    throw "SampleSize 必须在 1 到 50 之间。"
}

Write-Host "1/4 检查 SiliconFlow 连接与模型..." -ForegroundColor Cyan
& uv run policydb ai test
if ($LASTEXITCODE -ne 0) {
    throw "SiliconFlow 连接测试失败。"
}

Write-Host "2/4 从指定抓取批次选取 $SampleSize 条相关正文进行小样本抽取和复核..." -ForegroundColor Cyan
$env:CRPD_AI_TEST_RUN_ID = $RunId
$env:CRPD_AI_TEST_SAMPLE_SIZE = [string]$SampleSize

$TempPython = Join-Path $env:TEMP "crpd_ai_smoke_$PID.py"
$PythonCode = @'
import json
import os

import polars as pl

from policydb.enrich.glm import GLMEnricher
from policydb.settings import Settings

settings = Settings.discover()
run_id = os.environ["CRPD_AI_TEST_RUN_ID"]
sample_size = int(os.environ.get("CRPD_AI_TEST_SAMPLE_SIZE", "5"))

versions_path = settings.curated / "policy_document_versions.parquet"
items_path = settings.curated / "crawl_items.parquet"

if not versions_path.exists() or not items_path.exists():
    raise SystemExit("Missing policy_document_versions.parquet or crawl_items.parquet")

items = (
    pl.read_parquet(items_path)
    .filter(pl.col("run_id") == run_id)
    .select(pl.col("item_id"))
)

if items.is_empty():
    raise SystemExit(f"No crawl items found for run_id={run_id}")

# Keep the Python source ASCII-only so Windows code pages cannot corrupt Chinese terms.
keywords = [
    "\u623f\u5730\u4ea7",  # real estate
    "\u4f4f\u623f",        # housing
    "\u697c\u5e02",        # housing market
    "\u8d2d\u623f",        # home purchase
    "\u571f\u5730",        # land
    "\u516c\u79ef\u91d1",  # provident fund
    "\u57ce\u5e02\u66f4\u65b0",  # urban renewal
    "\u4fdd\u4ea4",        # housing delivery assurance
    "\u623f\u4f01",        # real-estate enterprise
]

versions = (
    pl.read_parquet(versions_path)
    .join(items, left_on="crawl_item_id", right_on="item_id", how="inner")
    .with_columns(
        pl.col("title").fill_null("").cast(pl.String).alias("title"),
        pl.col("extracted_text").fill_null("").cast(pl.String).alias("extracted_text"),
    )
    .with_columns(
        pl.concat_str([pl.col("title"), pl.col("extracted_text")], separator="\n").alias("search_text")
    )
    .filter(pl.col("extracted_text").str.len_chars() >= 200)
    .filter(
        pl.any_horizontal(
            [
                pl.col("search_text").str.contains(keyword, literal=True)
                for keyword in keywords
            ]
        )
    )
    .unique("document_version_id", keep="last")
    .sort("created_at", descending=True)
    .head(sample_size)
)

if versions.is_empty():
    raise SystemExit(
        f"No relevant full-text documents found for run_id={run_id}; "
        "try a larger or different run_id"
    )

document_ids = versions["document_version_id"].to_list()
preview_columns = [
    name
    for name in ("document_version_id", "title", "content_sha256", "canonical_url")
    if name in versions.columns
]
preview = versions.select(preview_columns).to_dicts()

enricher = GLMEnricher(settings)
extract_result = enricher.enrich_pending(document_version_ids=document_ids)
verify_result = enricher.verify_pending(document_version_ids=document_ids)

print(
    json.dumps(
        {
            "run_id": run_id,
            "requested_sample_size": sample_size,
            "actual_sample_size": len(document_ids),
            "document_version_ids": document_ids,
            "documents": preview,
            "extract": extract_result,
            "verify": verify_result,
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    )
)
'@

[System.IO.File]::WriteAllText($TempPython, $PythonCode, $Utf8NoBom)
try {
    & uv run python $TempPython
    $PythonExitCode = $LASTEXITCODE
}
finally {
    Remove-Item $TempPython -Force -ErrorAction SilentlyContinue
    Remove-Item Env:CRPD_AI_TEST_RUN_ID -ErrorAction SilentlyContinue
    Remove-Item Env:CRPD_AI_TEST_SAMPLE_SIZE -ErrorAction SilentlyContinue
}

if ($PythonExitCode -ne 0) {
    throw "AI小样本抽取或复核失败。"
}

Write-Host "3/4 将小样本结果映射到现有分类体系并更新池状态..." -ForegroundColor Cyan
& uv run policydb taxonomy build
if ($LASTEXITCODE -ne 0) { throw "taxonomy build 失败。" }

& uv run policydb ai deduplicate
if ($LASTEXITCODE -ne 0) { throw "deduplicate 失败。" }

& uv run policydb ai route-pools
if ($LASTEXITCODE -ne 0) { throw "route-pools 失败。" }

Write-Host "4/4 最终校验..." -ForegroundColor Cyan
& uv run policydb build-database
if ($LASTEXITCODE -ne 0) { throw "build-database 失败。" }

& uv run policydb validate
if ($LASTEXITCODE -ne 0) { throw "validate 失败。" }

Write-Host "AI小检验完成。请检查 actual_sample_size、extract.completed、verify.completed 和 failed。" -ForegroundColor Green
