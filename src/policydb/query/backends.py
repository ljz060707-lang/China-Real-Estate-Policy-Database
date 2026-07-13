from __future__ import annotations

from typing import Protocol

import polars as pl


class QueryBackend(Protocol):
    """Backend boundary kept stable for a future PostgreSQL implementation."""

    def query(self, sql: str, params: list | None = None) -> pl.DataFrame: ...


class PostgreSQLBackend:
    """Optional adapter placeholder; first release deliberately has no server dependency."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def query(self, sql: str, params: list | None = None) -> pl.DataFrame:
        raise NotImplementedError(
            "Install a PostgreSQL driver and implement this adapter when server deployment is chosen."
        )
