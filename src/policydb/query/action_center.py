"""Read-only compatibility preparation for the policy action-center view."""

from __future__ import annotations

import re

import duckdb

_ACTION_CENTER_SOURCES = ("policy_publications", "policy_files")
_VIEW_NAME = "v_policy_action_center"


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _view_body(view_sql: str) -> str:
    match = re.match(
        rf"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+{re.escape(_VIEW_NAME)}\s+AS\s+(.*)$",
        view_sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise ValueError(f"Unexpected {_VIEW_NAME} definition")
    return match.group(1)


def _source_alias(con: duckdb.DuckDBPyConnection, source_name: str) -> str | None:
    try:
        columns = con.execute(
            f"DESCRIBE main.{_quoted_identifier(source_name)}"
        ).fetchall()
    except duckdb.Error:
        return None
    if not any(row[0] == "record_id" for row in columns):
        return None
    alias = f"__crpd_{source_name}_varchar"
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {_quoted_identifier(alias)} AS "
        "SELECT * REPLACE(CAST(record_id AS VARCHAR) AS record_id) "
        f"FROM main.{_quoted_identifier(source_name)}"
    )
    return alias


def prepare_action_center_view(con: duckdb.DuckDBPyConnection) -> bool:
    """Install a connection-local, VARCHAR-keyed action-center view.

    The formal DuckDB is read-only here.  This creates only temporary catalog
    objects on the Dashboard connection and never replaces a production file.
    """

    view_row = con.execute(
        "SELECT sql FROM duckdb_views() WHERE schema_name='main' AND view_name=?",
        [_VIEW_NAME],
    ).fetchone()
    if not view_row or not view_row[0]:
        return False

    body = _view_body(str(view_row[0]))
    replacements: dict[str, str] = {}
    for source_name in _ACTION_CENTER_SOURCES:
        if not re.search(rf"\b{re.escape(source_name)}\b", body, re.IGNORECASE):
            continue
        alias = _source_alias(con, source_name)
        if alias is None:
            return False
        replacements[source_name] = alias

    if not replacements:
        return False

    for source_name, alias in replacements.items():
        body = re.sub(
            rf"\bmain\s*\.\s*{re.escape(source_name)}\b",
            alias,
            body,
            flags=re.IGNORECASE,
        )
        body = re.sub(
            rf"\b{re.escape(source_name)}\b",
            alias,
            body,
            flags=re.IGNORECASE,
        )
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {_quoted_identifier(_VIEW_NAME)} AS {body}"
    )
    return True


__all__ = ["prepare_action_center_view"]
