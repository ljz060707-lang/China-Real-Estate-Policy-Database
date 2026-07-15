import polars as pl


def test_search_returns_polars(db):
    assert isinstance(db.search(keyword="公积金", limit=5), pl.DataFrame)


def test_search_limit(db):
    assert db.search(limit=3).height <= 3


def test_search_summary_does_not_load_full_text(db):
    frame = db.search(keyword="城市", limit=5, include_full_text=False)
    assert "full_text" not in frame.columns
    assert {"record_id", "title", "summary"}.issubset(frame.columns)


def test_get(db):
    rid = db.search(limit=1)["record_id"][0]
    assert db.get(rid)["record_id"] == rid


def test_timeline(db):
    assert isinstance(db.timeline(region="武汉"), pl.DataFrame)


def test_stats(db):
    assert "policy_count" in db.stats(["year"]).columns


def test_official_filter(db):
    f = db.search(official_only=True, limit=50)
    assert all(x in ("official", "official_reprint") for x in f["official_status"].to_list())


def test_export_csv(db, tmp_path):
    assert db.export(db.search(limit=2), tmp_path / "x.csv").exists()


def test_export_parquet(db, tmp_path):
    assert db.export(db.search(limit=2), tmp_path / "x.parquet").exists()
