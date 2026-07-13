from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path

import duckdb
import polars as pl

from policydb.settings import Settings


class ResearchAPI:
    def __init__(self, db: PolicyDB):
        self.db = db

    def city_month_panel(self, start_date=None, end_date=None) -> pl.DataFrame:
        return self.db._dated_view("v_city_month_policy_panel", start_date, end_date)

    def city_year_panel(self, start_date=None, end_date=None) -> pl.DataFrame:
        return self.db._dated_view("v_city_year_policy_panel", start_date, end_date)

    def event_window(self, event_type: str, window=(-12, 24), unit="month") -> pl.DataFrame:
        frame = self.db._query("SELECT * FROM v_city_month_policy_panel")
        return frame.with_columns(
            pl.lit(event_type).alias("event_type"),
            pl.lit(window[0]).alias("window_start"),
            pl.lit(window[1]).alias("window_end"),
            pl.lit(unit).alias("window_unit"),
        )


class PolicyDB:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.research = ResearchAPI(self)

    @classmethod
    def open(cls, root: str | Path | None = None) -> PolicyDB:
        settings = Settings.discover(root)
        if not settings.database.exists():
            from policydb.query.database import build_database

            build_database(settings)
        return cls(settings)

    def _query(self, sql: str, params: list | None = None) -> pl.DataFrame:
        with duckdb.connect(str(self.settings.database), read_only=True) as con:
            return con.execute(sql, params or []).pl()

    def search(
        self,
        keyword=None,
        region=None,
        start_date=None,
        end_date=None,
        topics=None,
        direction=None,
        official_only=False,
        limit=1000,
    ) -> pl.DataFrame:
        clauses, params = ["1=1"], []
        if keyword:
            clauses.append("(title ILIKE ? OR summary ILIKE ? OR full_text ILIKE ?)")
            params += [f"%{keyword}%"] * 3
        if region:
            clauses.append("(city_name ILIKE ? OR geography_original ILIKE ?)")
            params += [f"%{region}%"] * 2
        if start_date:
            clauses.append("record_date>=?")
            params.append(start_date)
        if end_date:
            clauses.append("record_date<=?")
            params.append(end_date)
        if topics:
            clauses.append("(" + " OR ".join("topics ILIKE ?" for _ in topics) + ")")
            params.extend(f"%{topic}%" for topic in topics)
        if direction:
            clauses.append("direction IN (SELECT unnest(?))")
            params.append(list(direction))
        if official_only:
            clauses.append("official_status IN ('official','official_reprint')")
        return self._query(
            f"SELECT * FROM v_policy_master WHERE {' AND '.join(clauses)} ORDER BY record_date DESC NULLS LAST LIMIT {int(limit)}",
            params,
        )

    def get(self, record_id: str) -> dict | None:
        rows = self._query(
            "SELECT * FROM v_policy_master WHERE record_id=?", [record_id]
        ).to_dicts()
        return rows[0] if rows else None

    def timeline(self, region=None, topic=None) -> pl.DataFrame:
        clauses, params = ["1=1"], []
        if region:
            clauses.append("city_name ILIKE ?")
            params.append(f"%{region}%")
        if topic:
            clauses.append("topic=?")
            params.append(topic)
        return self._query(
            f"SELECT * FROM v_city_policy_timeline WHERE {' AND '.join(clauses)} ORDER BY record_date",
            params,
        )

    def stats(self, group_by: Iterable[str]) -> pl.DataFrame:
        allowed = {
            "year": "year(record_date)",
            "province": "city_name",
            "topic": "topics",
            "direction": "direction",
            "source_quality": "source_quality",
        }
        groups = [allowed[x] + f" AS {x}" for x in group_by if x in allowed]
        if not groups:
            raise ValueError("No supported group_by fields")
        names = [x for x in group_by if x in allowed]
        return self._query(
            f"SELECT {','.join(groups)},count(*) policy_count FROM v_policy_master GROUP BY {','.join(str(i + 1) for i in range(len(groups)))} ORDER BY {','.join(names)}"
        )

    def _dated_view(self, view: str, start_date=None, end_date=None) -> pl.DataFrame:
        clauses, params = ["1=1"], []
        if start_date:
            y, m = map(int, str(start_date)[:7].split("-"))
            clauses.append("(year>? OR (year=? AND month>=?))")
            params += [y, y, m]
        if end_date:
            y, m = map(int, str(end_date)[:7].split("-"))
            clauses.append("(year<? OR (year=? AND month<=?))")
            params += [y, y, m]
        return self._query(f"SELECT * FROM {view} WHERE {' AND '.join(clauses)}", params)

    def export(self, data: pl.DataFrame | str, path: str | Path) -> Path:
        frame = self._query(f"SELECT * FROM {data}") if isinstance(data, str) else data
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frame.write_csv(path)
        elif suffix == ".parquet":
            frame.write_parquet(path)
        elif suffix in (".jsonl", ".ndjson"):
            frame.write_ndjson(path)
        elif suffix == ".xlsx":
            frame.write_excel(path, autofit=True)
        elif suffix == ".dta":
            mapping = {c: re.sub(r"[^a-zA-Z0-9_]", "_", c)[:32] for c in frame.columns}
            frame.rename(mapping).to_pandas().to_stata(path, write_index=False)
            path.with_suffix(".fields.json").write_text(
                json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        else:
            raise ValueError(f"Unsupported export: {suffix}")
        return path
