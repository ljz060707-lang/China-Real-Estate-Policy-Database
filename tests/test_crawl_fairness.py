from collections import Counter
from datetime import date

import polars as pl
import yaml

from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings


def test_global_limit_is_round_robin_across_sources(tmp_path):
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    sources = []
    for source_index in range(3):
        sources.append(
            {
                "source_id": f"S{source_index}",
                "source_name": f"来源{source_index}",
                "domain": f"s{source_index}.gov.cn",
                "source_type": "government",
                "source_role": "canonical_candidate",
                "official_status": "official",
                "seed_urls": [
                    f"https://s{source_index}.gov.cn/policy/{item}"
                    for item in range(10)
                ],
                "crawl_enabled": True,
                "priority": source_index,
            }
        )
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, allow_unicode=True),
        encoding="utf-8",
    )

    pipeline = CrawlPipeline(Settings(root=root))
    plan = pipeline.plan(
        run_type="test",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        max_candidates_total=6,
        max_candidates_per_source=10,
        batch_size=50,
    )
    rows = pl.read_parquet(
        root / "data" / "curated" / "crawl_items.parquet"
    ).filter(pl.col("run_id") == plan["run_id"])
    counts = Counter(rows["source_id"].to_list())
    assert rows.height == 6
    assert counts == {"S0": 2, "S1": 2, "S2": 2}
