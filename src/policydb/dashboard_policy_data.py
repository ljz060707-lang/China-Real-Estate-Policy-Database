"""Bounded read-only policy queries for the Dashboard.

The formal DuckDB remains the preferred source.  When its external Parquet
views are temporarily unavailable, this service reads the same authoritative
E-drive Curated tables directly and reports the degraded mode explicitly.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from policydb.api import PolicyDB
from policydb.dashboard_live_state import database_health
from policydb.dashboard_queries import (
    filter_options as duckdb_filter_options,
)
from policydb.dashboard_queries import (
    policy_detail as duckdb_policy_detail,
)
from policydb.dashboard_queries import (
    policy_list as duckdb_policy_list,
)
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings


class DashboardPolicyData:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.health = database_health(settings)
        if self.health.get("status") == "HEALTHY" and self.health.get("queryable"):
            self.mode = "duckdb"
        elif self.health.get("fallback_available"):
            self.mode = "curated_fallback"
        else:
            self.mode = "unavailable"
        self.db = PolicyDB(settings) if self.mode == "duckdb" else None

    def _read(self, name: str, columns: list[str] | None = None) -> pl.DataFrame:
        path = self.settings.curated / f"{name}.parquet"
        if not path.exists():
            return pl.DataFrame()
        try:
            return read_parquet_snapshot(path, columns=columns)
        except (OSError, pl.exceptions.PolarsError, ValueError):
            return pl.DataFrame()

    def _curated_index(self) -> pl.DataFrame:
        records = self._read(
            "records",
            [
                "record_id",
                "record_date",
                "title",
                "summary",
                "official_status",
                "manual_review_status",
                "primary_source_url",
            ],
        )
        if records.is_empty():
            return records
        geography = self._read(
            "record_geographies_normalized",
            ["record_id", "province_name", "city_name", "county_name"],
        )
        if not geography.is_empty():
            geography = geography.group_by("record_id").agg(
                pl.col("province_name").drop_nulls().first().alias("province"),
                pl.col("city_name").drop_nulls().first().alias("city"),
                pl.col("county_name").drop_nulls().first().alias("district"),
            )
            records = records.join(geography, on="record_id", how="left")
        else:
            records = records.with_columns(
                pl.lit(None, dtype=pl.String).alias("province"),
                pl.lit(None, dtype=pl.String).alias("city"),
                pl.lit(None, dtype=pl.String).alias("district"),
            )
        classifications = self._read(
            "policy_classifications",
            [
                "record_id",
                "primary_category",
                "secondary_category",
                "instrument_type",
                "direction",
                "confidence",
                "review_status",
            ],
        )
        if not classifications.is_empty():
            classifications = classifications.group_by("record_id").agg(
                pl.col("primary_category").drop_nulls().first().alias("primary_category_code"),
                pl.col("secondary_category").drop_nulls().first().alias("secondary_category_code"),
                pl.col("instrument_type").drop_nulls().first(),
                pl.col("direction").drop_nulls().first(),
                pl.col("confidence").drop_nulls().max().alias("classification_confidence"),
                pl.col("review_status").drop_nulls().first().alias("classification_review_status"),
            )
            records = records.join(classifications, on="record_id", how="left")
        else:
            records = records.with_columns(
                pl.lit(None, dtype=pl.String).alias("primary_category_code"),
                pl.lit(None, dtype=pl.String).alias("secondary_category_code"),
                pl.lit(None, dtype=pl.String).alias("instrument_type"),
                pl.lit(None, dtype=pl.String).alias("direction"),
                pl.lit(None, dtype=pl.Float64).alias("classification_confidence"),
                pl.lit(None, dtype=pl.String).alias("classification_review_status"),
            )
        files = self._read("policy_files", ["record_id", "content_type", "archive_status"])
        if (
            not files.is_empty()
            and "record_id" in files.columns
            and files.schema["record_id"] != pl.Null
            and files.get_column("record_id").drop_nulls().len() > 0
        ):
            files = (
                files.filter(pl.col("record_id").is_not_null())
                .group_by("record_id")
                .agg(
                    (
                        pl.col("content_type").fill_null("").str.to_lowercase().str.contains("pdf")
                        & (pl.col("archive_status") == "archived")
                    )
                    .any()
                    .alias("has_pdf")
                )
            )
            records = records.join(files, on="record_id", how="left").with_columns(
                pl.col("has_pdf").fill_null(False)
            )
        else:
            records = records.with_columns(pl.lit(False).alias("has_pdf"))
        return records

    def filter_options(self) -> dict[str, list[Any]]:
        if self.mode == "duckdb" and self.db is not None:
            return duckdb_filter_options(self.db)
        if self.mode == "unavailable":
            return {
                "provinces": [],
                "cities": [],
                "directions": [],
                "instruments": [],
                "statuses": [],
                "reviews": [],
                "primary": [],
                "secondary": [],
            }
        frame = self._curated_index()

        def unique(column: str) -> list[Any]:
            if frame.is_empty() or column not in frame.columns:
                return []
            return sorted(frame.get_column(column).drop_nulls().unique().to_list())

        primary = unique("primary_category_code")
        secondary = (
            frame.select("primary_category_code", "secondary_category_code")
            .drop_nulls()
            .unique()
            .sort(["primary_category_code", "secondary_category_code"])
            .to_dicts()
            if not frame.is_empty()
            else []
        )
        return {
            "provinces": unique("province"),
            "cities": unique("city"),
            "directions": unique("direction"),
            "instruments": unique("instrument_type"),
            "statuses": unique("official_status"),
            "reviews": unique("classification_review_status"),
            "primary": primary,
            "secondary": secondary,
        }

    def search(
        self,
        filters: dict[str, Any],
        *,
        page: int,
        page_size: int,
        sort_by: str = "发布日期",
    ) -> tuple[pl.DataFrame, int]:
        page = max(1, int(page))
        page_size = min(100, max(10, int(page_size)))
        if self.mode == "duckdb" and self.db is not None:
            return duckdb_policy_list(
                self.db,
                filters,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
            )
        if self.mode == "unavailable":
            return pl.DataFrame(), 0
        frame = self._curated_index()
        if frame.is_empty():
            return frame, 0
        start = filters.get("start_date") or date(2018, 1, 1)
        end = filters.get("end_date") or date.today()
        frame = frame.filter(
            pl.col("record_date").is_not_null()
            & (pl.col("record_date") >= start)
            & (pl.col("record_date") <= end)
        )
        scalar_filters = {
            "province": "province",
            "city": "city",
            "primary_category_code": "primary_category_code",
            "secondary_category_code": "secondary_category_code",
            "direction": "direction",
            "instrument_type": "instrument_type",
            "official_status": "official_status",
            "review_status": "classification_review_status",
        }
        for key, column in scalar_filters.items():
            value = filters.get(key)
            if value and column in frame.columns:
                frame = frame.filter(pl.col(column) == value)
        cities = filters.get("cities") or []
        if cities:
            frame = frame.filter(pl.col("city").is_in(cities))
        if filters.get("has_pdf") is not None:
            frame = frame.filter(pl.col("has_pdf") == bool(filters["has_pdf"]))
        keyword = str(filters.get("keyword") or "").strip()
        if keyword:
            # Keyword filtering is user-triggered and bounded to the Curated record index.
            text = pl.concat_str(
                [pl.col("title").fill_null(""), pl.col("summary").fill_null("")],
                separator=" ",
            )
            frame = frame.filter(text.str.contains(keyword, literal=True))
        total = frame.height
        frame = frame.sort("record_date", descending=True, nulls_last=True).slice(
            (page - 1) * page_size, page_size
        )
        return frame, total

    def detail(self, record_id: str) -> dict[str, Any]:
        if self.mode == "duckdb" and self.db is not None:
            policy, actions, files = duckdb_policy_detail(self.db, record_id)
            return {
                "policy": policy,
                "actions": actions,
                "files": files,
                "versions": pl.DataFrame(),
            }
        if self.mode == "unavailable":
            return {
                "policy": None,
                "actions": pl.DataFrame(),
                "files": pl.DataFrame(),
                "versions": pl.DataFrame(),
            }
        records = self._read("records")
        policy_rows = (
            records.filter(pl.col("record_id") == record_id)
            if not records.is_empty()
            else pl.DataFrame()
        )
        actions = self._read("policy_actions")
        actions = (
            actions.filter(pl.col("record_id") == record_id) if not actions.is_empty() else actions
        )
        files = self._read("policy_files")
        files = (
            files.filter(pl.col("record_id") == record_id)
            if not files.is_empty()
            and "record_id" in files.columns
            and files.schema["record_id"] != pl.Null
            else pl.DataFrame()
        )
        versions = self._read("policy_document_versions")
        versions = (
            versions.filter(pl.col("record_id") == record_id)
            if not versions.is_empty()
            else versions
        )
        return {
            "policy": policy_rows.row(0, named=True) if policy_rows.height else None,
            "actions": actions,
            "files": files,
            "versions": versions,
        }


__all__ = ["DashboardPolicyData"]
