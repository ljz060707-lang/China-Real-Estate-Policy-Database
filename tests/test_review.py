from __future__ import annotations

import csv
import hashlib
import shutil

import duckdb
import polars as pl
import pytest

from policydb.query.database import build_database
from policydb.review import (
    apply_corrections,
    generate_review_tasks,
    list_review_tasks,
    review_task_count,
    save_review_decision,
)
from policydb.settings import Settings


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def review_env(tmp_path_factory, root):
    target = tmp_path_factory.mktemp("review_center")
    shutil.copytree(root / "data" / "curated", target / "data" / "curated")
    staging = target / "data" / "staging" / "excel"
    staging.mkdir(parents=True)
    source_t2 = next((root / "data" / "staging" / "excel").glob("*T2_*.parquet"))
    shutil.copy2(source_t2, staging / source_t2.name)
    raw = target / "data" / "raw" / "seed" / "immutable_seed.bin"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"raw-data-must-never-change")
    settings = Settings(root=target)
    build_database(settings)
    generate_review_tasks(settings)
    return settings


def test_generate_review_tasks(review_env):
    result = generate_review_tasks(review_env)
    assert result["discovered_total"] > 0
    assert result["discovered"]["missing_title"] > 0
    assert result["discovered"]["low_confidence"] > 0
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM manual_review_tasks").fetchone()[0] > 0


def test_generate_review_tasks_is_idempotent(review_env):
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        before = con.execute("SELECT count(*) FROM manual_review_tasks").fetchone()[0]
    result = generate_review_tasks(review_env)
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        after = con.execute("SELECT count(*) FROM manual_review_tasks").fetchone()[0]
    assert result["created_total"] == 0
    assert after == before


def test_review_tasks_are_paginated(review_env):
    total = review_task_count(review_env, review_type="low_confidence")
    first_page = list_review_tasks(
        review_env, review_type="low_confidence", limit=20, offset=0
    )
    second_page = list_review_tasks(
        review_env, review_type="low_confidence", limit=20, offset=20
    )
    assert total > 40
    assert first_page.height == 20
    assert second_page.height == 20
    assert set(first_page["task_id"]).isdisjoint(second_page["task_id"])


def test_review_status_can_be_updated(review_env):
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        task_id = con.execute(
            "SELECT task_id FROM manual_review_tasks WHERE status='pending' LIMIT 1"
        ).fetchone()[0]
    save_review_decision(task_id, "ignored", reviewer="pytest", settings=review_env)
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        status = con.execute(
            "SELECT status FROM manual_review_tasks WHERE task_id=?", [task_id]
        ).fetchone()[0]
    assert status == "ignored"


def test_manual_corrections_csv_is_generated(review_env):
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        task_id = con.execute(
            "SELECT task_id FROM manual_review_tasks "
            "WHERE review_type='low_confidence' AND status='pending' LIMIT 1"
        ).fetchone()[0]
    save_review_decision(
        task_id,
        "approved",
        reviewer="pytest",
        review_note="classification evidence checked",
        settings=review_env,
    )
    with review_env.manual_corrections.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert rows[-1]["decision"] == "approved"
    assert rows[-1]["reviewer"] == "pytest"


def test_apply_review_is_idempotent_and_does_not_modify_raw(review_env):
    raw = review_env.root / "data" / "raw" / "seed" / "immutable_seed.bin"
    raw_hash = _hash(raw)
    with duckdb.connect(str(review_env.database), read_only=True) as con:
        task_id, record_id = con.execute(
            "SELECT task_id,record_id FROM manual_review_tasks "
            "WHERE review_type='missing_title' AND status='pending' LIMIT 1"
        ).fetchone()
    save_review_decision(
        task_id,
        "corrected",
        new_value="人工核验后的政策标题",
        reviewer="pytest",
        settings=review_env,
    )
    first = apply_corrections(review_env)
    second = apply_corrections(review_env)
    title = (
        pl.read_parquet(review_env.curated / "records.parquet")
        .filter(pl.col("record_id") == record_id)
        .select("title")
        .item()
    )
    assert first["applied_total"] > 0
    assert second["applied_total"] == 0
    assert title == "人工核验后的政策标题"
    assert _hash(raw) == raw_hash
