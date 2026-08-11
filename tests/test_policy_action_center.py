import shutil

import duckdb

from policydb import PolicyDB
from policydb.dashboard_queries import policy_list
from policydb.query.database import build_database
from policydb.settings import Settings


def test_action_center_preserves_legacy_lineage_without_using_it_as_filter(
    tmp_path, root
):
    target = tmp_path / "rebuilt"
    shutil.copytree(root / "data" / "curated", target / "data" / "curated")
    settings = Settings(root=target)
    build_database(settings)
    db = PolicyDB.open(target)
    fields = {
        row["column_name"]
        for row in db._query("DESCRIBE v_policy_action_center").to_dicts()
    }
    assert next(
        row["column_type"]
        for row in db._query("DESCRIBE policy_files").to_dicts()
        if row["column_name"] == "record_id"
    ) == "VARCHAR"
    assert next(
        row["column_type"]
        for row in db._query("DESCRIBE policy_publications").to_dicts()
        if row["column_name"] == "record_id"
    ) == "VARCHAR"
    assert next(
        row["column_type"]
        for row in db._query("DESCRIBE v_policy_action_center").to_dicts()
        if row["column_name"] == "record_id"
    ) == "VARCHAR"
    assert {
        "legacy_collection",
        "source_topic",
        "pdf_available",
        "full_text_available",
    } <= fields
    rows, _ = policy_list(
        db,
        {"primary_categories": ["D"]},
        page=1,
        page_size=5,
        sort_by="发布日期",
    )
    assert rows.height <= 5


def _create_stale_record_id_action_center(database_path):
    """Create the persisted-view shape that exposed the INT32 conversion bug."""

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            "CREATE TABLE records(record_id VARCHAR, title VARCHAR, summary VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE policy_actions("
            "action_id VARCHAR, record_id VARCHAR, clause_text VARCHAR, "
            "text_completeness VARCHAR, formal_eligible BOOLEAN, action_status VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE policy_classifications("
            "action_id VARCHAR, record_id VARCHAR, primary_category VARCHAR, "
            "secondary_category VARCHAR, instrument_type VARCHAR, direction VARCHAR, "
            "evidence_text VARCHAR, confidence DOUBLE, evidence_start BIGINT, "
            "evidence_end BIGINT, review_status VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE policy_entities("
            "record_id VARCHAR, policy_entity_id VARCHAR, entity_status VARCHAR)"
        )
        # This is how a NULL-only Parquet field was persisted by the stale view.
        connection.execute(
            "CREATE TABLE policy_publications(record_id INTEGER, document_version_id VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE policy_duplicate_clusters("
            "cluster_id VARCHAR, member_document_version_ids VARCHAR)"
        )
        connection.execute("CREATE TABLE policies(record_id VARCHAR, target_group VARCHAR)")
        connection.execute(
            "CREATE TABLE policy_files("
            "record_id INTEGER, archive_status VARCHAR, content_type VARCHAR, "
            "archive_relative_path VARCHAR)"
        )
        connection.execute("CREATE TABLE record_collections(record_id VARCHAR, collection_name VARCHAR)")
        connection.execute(
            "INSERT INTO records VALUES ('POL_08DAB107CC0E8BFDE780', 'Policy', 'summary')"
        )
        connection.execute(
            "INSERT INTO policy_actions VALUES "
            "('A1', 'POL_08DAB107CC0E8BFDE780', 'clause', 'full', true, 'active')"
        )
        connection.execute(
            "INSERT INTO policy_classifications VALUES "
            "('A1', 'POL_08DAB107CC0E8BFDE780', 'D', 'D06', 'regulation', "
            "'neutral', 'evidence', 0.9, 1, 2, 'auto_verified')"
        )
        connection.execute(
            "INSERT INTO policy_entities VALUES "
            "('POL_08DAB107CC0E8BFDE780', 'E1', 'ok')"
        )
        connection.execute("INSERT INTO policy_publications VALUES (NULL, 'V1')")
        connection.execute("INSERT INTO policy_files VALUES (NULL, 'archived', 'application/pdf', 'x')")
        connection.execute(
            "CREATE VIEW v_policy_action_center AS "
            "WITH identities AS ("
            "SELECT pe.record_id, min(pe.policy_entity_id) AS policy_entity_id, "
            "min(pe.entity_status) AS version_status "
            "FROM policy_entities pe "
            "LEFT JOIN policy_publications pp USING(record_id) "
            "LEFT JOIN policy_duplicate_clusters c "
            "ON strpos(c.member_document_version_ids, pp.document_version_id) > 0 "
            "GROUP BY pe.record_id), "
            "legacy AS ("
            "SELECT record_id, string_agg(collection_name, ',') AS legacy_collection "
            "FROM record_collections GROUP BY record_id) "
            "SELECT a.action_id, a.record_id, r.title, r.summary, c.instrument_type, "
            "p.target_group, i.version_status, f.has_archived_file "
            "FROM policy_actions a JOIN records r USING(record_id) "
            "JOIN policy_classifications c USING(action_id) "
            "LEFT JOIN policies p USING(record_id) "
            "LEFT JOIN identities i USING(record_id) "
            "LEFT JOIN legacy l USING(record_id) "
            "LEFT JOIN (SELECT record_id, bool_or(archive_status='archived') AS has_archived_file "
            "FROM policy_files GROUP BY record_id) f USING(record_id) "
            "UNION ALL "
            "SELECT 'RECORD:' || r.record_id, r.record_id, r.title, r.summary, "
            "NULL::VARCHAR, p.target_group, i.version_status, f.has_archived_file "
            "FROM records r LEFT JOIN policies p USING(record_id) "
            "LEFT JOIN identities i USING(record_id) "
            "LEFT JOIN (SELECT record_id, bool_or(archive_status='archived') AS has_archived_file "
            "FROM policy_files GROUP BY record_id) f USING(record_id) "
            "WHERE NOT EXISTS (SELECT 1 FROM policy_actions a WHERE a.record_id=r.record_id)"
        )


def test_action_center_normalizes_stale_integer_record_id_before_dashboard_queries(tmp_path):
    database_path = tmp_path / "policydb.duckdb"
    _create_stale_record_id_action_center(database_path)
    settings = Settings(
        root=tmp_path,
        curated_path=tmp_path / "curated",
        database_path=database_path,
    )
    db = PolicyDB(settings)

    options = db._query(
        "SELECT DISTINCT instrument_type FROM v_policy_action_center "
        "WHERE instrument_type IS NOT NULL ORDER BY 1"
    )
    details = db._query(
        "SELECT action_id, record_id FROM v_policy_action_center "
        "WHERE record_id=? ORDER BY action_id",
        ["POL_08DAB107CC0E8BFDE780"],
    )
    record_id_type = next(
        row[1]
        for row in db._query("DESCRIBE v_policy_action_center").iter_rows()
        if row[0] == "record_id"
    )

    assert record_id_type == "VARCHAR"
    assert options["instrument_type"].to_list() == ["regulation"]
    assert details.height == 1
