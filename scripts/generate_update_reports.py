from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings
from policydb.validate.quality import validate


def main() -> None:
    settings = Settings.discover(Path(__file__).resolve().parents[1])
    output = settings.root / "outputs"
    output.mkdir(parents=True, exist_ok=True)
    validation = validate(settings)
    (output / "validation_report.json").write_text(
        json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    crawl = CrawlPipeline(settings).audit()
    pl.DataFrame([crawl]).write_csv(output / "crawl_coverage_report.csv")
    with duckdb.connect(str(settings.database), read_only=True) as con:
        source_health = con.execute(
            """SELECT source_id,source_name,domain,official_status,crawl_enabled,priority,
                      last_success_at,CASE WHEN last_success_at IS NULL THEN 'not_checked'
                      ELSE 'observed' END health_status
               FROM source_registry ORDER BY priority,domain"""
        ).pl()
        city_coverage = con.execute(
            """SELECT c.city_id,c.city_name,c.province_name,c.city_tier_existing,
                      count(DISTINCT p.record_id) policy_count,
                      count(DISTINCT CASE WHEN NOT p.needs_review THEN p.record_id END)
                        confirmed_policy_count,
                      count(DISTINCT CASE WHEN p.needs_review THEN p.record_id END)
                        provisional_policy_count,
                      count(DISTINCT CASE WHEN p.official_status IN
                        ('official','official_reprint') THEN p.record_id END) official_policy_count,
                      max(p.record_date) latest_policy_date
               FROM cities_105 c LEFT JOIN v_policy_105_cities p USING(city_id)
               GROUP BY ALL ORDER BY c.province_name,c.city_name"""
        ).pl()
    source_health.write_csv(output / "source_health_report.csv")
    city_coverage.write_csv(output / "city_coverage_report.csv")
    summary = f"""# 更新摘要

- 生成时间：{datetime.now(UTC).isoformat()}
- 105城市：{city_coverage.height}
- 已确定政策记录城市：{city_coverage.filter(pl.col('confirmed_policy_count') > 0).height}
- 含待审核省级适用关系城市：{city_coverage.filter(pl.col('provisional_policy_count') > 0).height}
- 来源域名：{source_health.height}
- 本地政策记录：{validation['record_count']}
- 验证通过：{validation['passed']}
- 抓取运行：{crawl['crawl_runs']}
- 文档版本：{crawl['document_versions']}
- 抓取错误：{crawl['fetch_errors']}
"""
    (output / "update_summary.md").write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
