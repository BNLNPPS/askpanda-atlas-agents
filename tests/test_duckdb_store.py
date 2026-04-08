"""Tests for DuckDBStore.

Covers:
- write_table: append mode (overwrite=False) and overwrite mode (overwrite=True)
- write_table: transactional safety when overwrite=True — a failed write must
  leave the previous committed snapshot intact (ROLLBACK on error)
- record_snapshot: basic round-trip
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from bamboo_mcp_services.common.storage.duckdb_store import DuckDBStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    """DuckDBStore backed by a temporary file (not :memory:) so we can open a
    second connection to verify the committed state independently."""
    db_path = str(tmp_path / "test.duckdb")
    s = DuckDBStore(path=db_path)
    yield s
    s._conn.close()


@pytest.fixture
def mem_store():
    """In-memory DuckDBStore — sufficient for most unit tests."""
    s = DuckDBStore(path=":memory:")
    yield s
    s._conn.close()


# ---------------------------------------------------------------------------
# write_table — append mode
# ---------------------------------------------------------------------------

class TestWriteTableAppend:
    """write_table(overwrite=False) accumulates rows without dropping the table."""

    def test_creates_table_and_inserts_rows(self, mem_store):
        rows = [{"x": 1}, {"x": 2}]
        mem_store.write_table("t", rows, overwrite=False)
        count = mem_store._conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 2

    def test_appends_on_second_call(self, mem_store):
        mem_store.write_table("t", [{"x": 1}], overwrite=False)
        mem_store.write_table("t", [{"x": 2}], overwrite=False)
        count = mem_store._conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 2

    def test_empty_rows_is_a_noop(self, mem_store):
        mem_store.write_table("t", [], overwrite=False)
        tables = [r[0] for r in mem_store._conn.execute("SHOW TABLES").fetchall()]
        assert "t" not in tables


# ---------------------------------------------------------------------------
# write_table — overwrite mode
# ---------------------------------------------------------------------------

class TestWriteTableOverwrite:
    """write_table(overwrite=True) replaces all previous rows atomically."""

    def test_replaces_previous_rows(self, mem_store):
        mem_store.write_table("t", [{"x": 1}, {"x": 2}], overwrite=False)
        mem_store.write_table("t", [{"x": 99}], overwrite=True)
        count = mem_store._conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 1

    def test_creates_table_when_none_exists(self, mem_store):
        mem_store.write_table("fresh", [{"a": "hello"}], overwrite=True)
        count = mem_store._conn.execute("SELECT COUNT(*) FROM fresh").fetchone()[0]
        assert count == 1

    def test_empty_rows_is_a_noop(self, mem_store):
        mem_store.write_table("t", [{"x": 1}], overwrite=False)
        mem_store.write_table("t", [], overwrite=True)
        # Table should still exist with the original row.
        count = mem_store._conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# write_table — transactional safety (overwrite=True)
# ---------------------------------------------------------------------------

class TestWriteTableTransactionSafety:
    """A failed overwrite must ROLLBACK, leaving the previous snapshot intact.

    Without an explicit BEGIN/COMMIT the DROP happens outside a transaction:
    if the subsequent INSERT raises, the table is gone and data is lost.
    These tests confirm the fix: the previous snapshot survives any error
    that occurs after the DROP.
    """

    def test_rollback_on_insert_failure_preserves_previous_data(self, mem_store):
        """Previous rows survive when the INSERT inside write_table raises."""
        # Establish a baseline snapshot.
        baseline = [{"key": "baseline_value"}]
        mem_store.write_table("t", baseline, overwrite=False)

        # Patch json.dumps (used inside the INSERT loop) to raise on the second
        # call — simulating a serialisation failure mid-write.
        call_count = {"n": 0}
        real_dumps = __import__("json").dumps

        def _fail_on_second(obj, **kwargs):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise RuntimeError("simulated serialisation failure")
            return real_dumps(obj, **kwargs)

        with patch("bamboo_mcp_services.common.storage.duckdb_store.json.dumps", side_effect=_fail_on_second):
            with pytest.raises(RuntimeError, match="simulated serialisation failure"):
                mem_store.write_table("t", [{"key": "v1"}, {"key": "v2"}], overwrite=True)

        # The previous baseline row must still be present.
        count = mem_store._conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == len(baseline), (
            f"ROLLBACK should have preserved {len(baseline)} baseline row(s), "
            f"but found {count}"
        )

    def test_table_is_never_absent_after_failed_overwrite(self, mem_store):
        """The table must still exist (not be in a dropped state) after a failed overwrite."""
        mem_store.write_table("t", [{"x": 1}], overwrite=False)

        def _always_fail(obj, **kwargs):
            raise RuntimeError("always fail")

        with patch("bamboo_mcp_services.common.storage.duckdb_store.json.dumps", side_effect=_always_fail):
            with pytest.raises(RuntimeError):
                mem_store.write_table("t", [{"x": 2}], overwrite=True)

        tables = [r[0] for r in mem_store._conn.execute("SHOW TABLES").fetchall()]
        assert "t" in tables, "Table must still exist after a failed overwrite (ROLLBACK)"


# ---------------------------------------------------------------------------
# record_snapshot
# ---------------------------------------------------------------------------

class TestRecordSnapshot:
    """Basic round-trip for record_snapshot."""

    def test_inserts_ok_snapshot(self, mem_store):
        mem_store.record_snapshot("snap-1", "file://foo", ok=True, content_hash="abc123")
        row = mem_store._conn.execute(
            "SELECT source, ok, content_hash FROM snapshots WHERE snapshot_id = 'snap-1'"
        ).fetchone()
        assert row is not None
        source, ok, content_hash = row
        assert source == "file://foo"
        assert ok is True
        assert content_hash == "abc123"

    def test_inserts_error_snapshot(self, mem_store):
        mem_store.record_snapshot("snap-err", "file://bar", ok=False, error="timeout")
        row = mem_store._conn.execute(
            "SELECT ok, error FROM snapshots WHERE snapshot_id = 'snap-err'"
        ).fetchone()
        assert row is not None
        ok, error = row
        assert ok is False
        assert error == "timeout"

    def test_replace_updates_existing_snapshot(self, mem_store):
        mem_store.record_snapshot("snap-1", "file://foo", ok=False, error="first error")
        mem_store.record_snapshot("snap-1", "file://foo", ok=True, content_hash="newhash")
        rows = mem_store._conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE snapshot_id = 'snap-1'"
        ).fetchone()[0]
        # INSERT OR REPLACE — only one row should exist.
        assert rows == 1
        ok = mem_store._conn.execute(
            "SELECT ok FROM snapshots WHERE snapshot_id = 'snap-1'"
        ).fetchone()[0]
        assert ok is True
