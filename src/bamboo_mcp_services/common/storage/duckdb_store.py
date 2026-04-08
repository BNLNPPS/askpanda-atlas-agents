"""DuckDB storage backend for agent data persistence."""
from __future__ import annotations
import duckdb
from datetime import datetime, timezone
from typing import Optional, Any
import json


class DuckDBStore:
    """DuckDB-based storage for agent data and snapshots.

    Provides methods for storing data snapshots, recording metadata,
    and managing structured data tables.

    Attributes:
        path: Database file path or ":memory:" for in-memory database.
    """

    def __init__(self, path: str = ":memory:") -> None:
        """Initialize the DuckDB store.

        Args:
            path: Path to the DuckDB database file. Use ":memory:" for
                an in-memory database (default).
        """
        self.path = path
        self._conn = duckdb.connect(database=path, read_only=False)
        self._init_meta()

    def _init_meta(self) -> None:
        """Initialize metadata tables and extensions.

        Creates the snapshots table if it doesn't exist and attempts
        to install the sqlite_scannable extension.
        """
        self._conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id TEXT PRIMARY KEY,
            source TEXT,
            fetched_utc TIMESTAMP,
            content_hash TEXT,
            ok BOOLEAN,
            error TEXT
        );
        """)
        try:
            self._conn.execute("INSTALL sqlite_scannable")
        except Exception:
            pass

    def write_table(self, table_name: str, rows: list[dict[str, Any]], overwrite: bool = False) -> None:
        """Write data rows to a table.

        When *overwrite* is ``True`` the entire DROP + CREATE + INSERT sequence
        is wrapped in an explicit transaction so that concurrent readers always
        observe either the previous complete table or the new one — never a torn
        state where the table is absent or partially filled.

        Args:
            table_name: Name of the target table.
            rows: List of dictionaries to insert. Each row is stored as JSON.
            overwrite: If True, drop and recreate the table before inserting.
                If False, create the table only if it doesn't exist.
        """
        if not rows:
            return
        if overwrite:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (data JSON, updated_utc TIMESTAMP)")
                for r in rows:
                    self._conn.execute("INSERT INTO {tn} VALUES (?, ?)".format(tn=table_name), [json.dumps(r, default=str), datetime.now(timezone.utc)])
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        else:
            self._conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (data JSON, updated_utc TIMESTAMP)")
            for r in rows:
                self._conn.execute("INSERT INTO {tn} VALUES (?, ?)".format(tn=table_name), [json.dumps(r, default=str), datetime.now(timezone.utc)])

    def record_snapshot(self, snapshot_id: str, source: str, ok: bool, content_hash: Optional[str] = None, error: Optional[str] = None) -> None:
        """Record metadata for a data snapshot.

        Args:
            snapshot_id: Unique identifier for this snapshot.
            source: Origin identifier (e.g., file path or URL).
            ok: Whether the snapshot was fetched successfully.
            content_hash: SHA-256 hash of the content, if available.
            error: Error message if the fetch failed.
        """
        self._conn.execute(
            "INSERT OR REPLACE INTO snapshots VALUES (?, ?, ?, ?, ?, ?)",
            [snapshot_id, source, datetime.now(timezone.utc), content_hash, ok, error],
        )
