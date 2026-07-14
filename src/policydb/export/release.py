from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from policydb.settings import Settings
from policydb.validate.quality import validate


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _csv_safe(frame: pl.DataFrame) -> pl.DataFrame:
    nested = [name for name, dtype in frame.schema.items() if dtype.is_nested()]
    if not nested:
        return frame
    return frame.with_columns(
        pl.col(name)
        .map_elements(
            lambda value: json.dumps(
                value.to_list() if hasattr(value, "to_list") else value,
                ensure_ascii=False,
                default=str,
            )
            if value is not None
            else None,
            return_dtype=pl.String,
        )
        .alias(name)
        for name in nested
    )


def create_release(version: str, settings: Settings | None = None) -> Path:
    settings = settings or Settings.discover()
    final_out = settings.root / "data" / "releases" / version
    if final_out.exists():
        raise FileExistsError(f"Release is immutable and already exists: {final_out}")
    out = final_out.with_name(f".{version}.building")
    if out.exists():
        shutil.rmtree(out)
    (out / "parquet").mkdir(parents=True)
    (out / "csv").mkdir()
    (out / "excel").mkdir()
    for src in settings.curated.glob("*.parquet"):
        shutil.copy2(src, out / "parquet" / src.name)
        _csv_safe(pl.read_parquet(src)).write_csv(out / "csv" / f"{src.stem}.csv")
    with duckdb.connect(str(settings.database), read_only=True) as con:
        for view in (
            "v_city_month_policy_panel",
            "v_city_year_policy_panel",
            "v_policy_event_study",
            "v_policy_105_cities",
            "v_city_month_policy_panel_105",
            "v_city_year_policy_panel_105",
        ):
            if not con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [view]
            ).fetchone()[0]:
                continue
            frame = con.execute(f"SELECT * FROM {view}").pl()
            frame.write_parquet(out / "parquet" / f"{view}.parquet")
            frame.write_csv(out / "csv" / f"{view}.csv")
    excel_records = pl.read_parquet(settings.curated / "records.parquet")
    datetime_columns = [
        name for name, dtype in excel_records.schema.items() if isinstance(dtype, pl.Datetime)
    ]
    if datetime_columns:
        excel_records = excel_records.with_columns(pl.col(datetime_columns).cast(pl.String))
    excel_records.write_excel(out / "excel" / "policy_records.xlsx", autofit=True)
    for name in ("data_dictionary.md", "methodology.md"):
        shutil.copy2(settings.root / "docs" / name, out / name)
    reference_dir = out / "reference"
    reference_dir.mkdir()
    for name in ("cities_105.csv", "source_registry.yaml", "crawl_keywords.yaml"):
        source = settings.root / "data" / "reference" / name
        if source.exists():
            shutil.copy2(source, reference_dir / name)
    report = validate(settings)
    (out / "validation_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    try:
        commit = (
            subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=settings.root,
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            or "not-a-git-repository"
        )
    except OSError:
        commit = "git-unavailable"
    files = [
        {
            "path": str(p.relative_to(out)).replace("\\", "/"),
            "sha256": _sha(p),
            "size": p.stat().st_size,
        }
        for p in out.rglob("*")
        if p.is_file()
    ]
    manifest = {
        "database_name": "中国房地产与城市政策研究数据库",
        "version": version,
        "created_at": datetime.now(UTC).isoformat(),
        "data_cutoff": report["main_date_max"],
        "git_commit": commit,
        "files": files,
        "recommended_citation": f"中国房地产与城市政策研究数据库，版本 {version}，发布日期 {datetime.now():%Y-%m-%d}，数据截止 {report['main_date_max']}，Git {commit}。",
    }
    (out / "release_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    out.rename(final_out)
    return final_out
