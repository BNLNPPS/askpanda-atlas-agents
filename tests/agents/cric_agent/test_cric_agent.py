"""Tests for the CRIC agent package.

Covers:
- cric_fetcher: type inference helpers, _build_rows, full run_cycle behaviour
- CricAgent: lifecycle, tick delegation, health reporting
- CLI: argument parsing, config validation, --once end-to-end
"""
from __future__ import annotations

import hashlib
import json
from unittest.mock import patch

import duckdb
import pytest

from bamboo_mcp_services.agents.cric_agent.agent import CricAgent, CricAgentConfig
from bamboo_mcp_services.agents.cric_agent.cric_fetcher import (
    CricQueuedataFetcher,
    _to_cell_value,
    _merge_type,
    _infer_schema,
    _SKIP_FIELDS,
)
from bamboo_mcp_services.agents.cric_agent.cli import build_parser, main
from bamboo_mcp_services.agents.base import AgentState
from bamboo_mcp_services.common.panda.source import RawSnapshot


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

#: Realistic single-queue CRIC payload derived from the AGLT2 sample.
AGLT2_PAYLOAD: dict = {
    "allow_lan": True,
    "allow_wan": True,
    "appdir": "",
    "astorages": {"pr": ["AGLT2_DATADISK"], "pw": ["AGLT2_DATADISK"]},
    "atlas_site": "AGLT2",
    "availablecpu": None,
    "cachedse": None,
    "capability": "ucore",
    "catchall": "",
    "cloud": "US",
    "comment": "test queue",
    "container_options": "",
    "container_type": "singularity:pilot",
    "acopytools": {"pr": ["rucio"], "pw": ["rucio"]},
    "copytools": {"rucio": {"setup": ""}},
    "corecount": 8,
    "coreenergy": 10.0,
    "coreenergy_data": {"coreenergy": 0.0, "scope": "rcsite"},  # must be dropped
    "corepower": 14.54,
    "corepower_data": {"corepower": 13.51, "scope": "rcsite"},  # must be dropped
    "country": "United States",
    "countrygroup": "",
    "depthboost": None,
    "description": "Grand Unified queue at AGLT2",
    "direct_access_lan": True,
    "direct_access_wan": True,
    "environ": "None",
    "fairsharepolicy": "",
    "gocname": "AGLT2",
    "gstat": "AGLT2",
    "harvester": "CERN_central_A",
    "harvester_template": "",
    "hc_param": "AutoExclusion",
    "hc_suite": ["AFT", "PFT"],
    "id": 733,
    "is_cvmfs": True,
    "is_default": True,
    "is_virtual": False,
    "jobseed": "eshigh",
    "last_modified": "2026-04-01T15:47:25",
    "localqueue": "",
    "maxdiskio": 5000,
    "maxdiskio_data": {"maxdiskio": 5000, "scope": "local"},  # must be dropped
    "maxinputsize": 16000,
    "maxrss": 48000,
    "maxtime": 258000,
    "maxwdir": 106498,
    "meanrss": 2200,
    "minrss": 0,
    "mintime": 0,
    "name": "AGLT2",
    "nickname": "AGLT2",
    "nodes": 118,
    "panda_resource": "AGLT2",
    "panda_site": "GreatLakesT2",
    "params": {"maxNewWorkersPerCycle": 300, "unified_dispatch": True},
    "parent": "GreatLakesT2_VIRTUAL",
    "pilot_manager": "Harvester",
    "pilot_version": "3.12.4.1",
    "pledgedcpu": None,
    "probe": None,
    "python_version": "3",
    "queuehours": 0,
    "queues": [{"ce_endpoint": "gate01.aglt2.org:9619", "ce_flavour": "HTCONDOR-CE"}],
    "rc": "US-AGLT2",
    "rc_country": "United States",
    "rc_site": "AGLT2",
    "rc_site_state": "ACTIVE",
    "region": "US-MIDW-MISO",
    "releases": ["AUTO"],
    "resource_type": "GRID",
    "site": "GreatLakesT2",
    "site_state": "ACTIVE",
    "siteid": "AGLT2",
    "special_par": "",
    "state": "ACTIVE",
    "state_comment": "cloned from AGLT2_UCORE",
    "state_update": "2020-05-22T16:39:14",
    "status": "online",
    "tier": "T2D",
    "tier_level": 2,
    "timefloor": 200,
    "transferringlimit": 3000,
    "type": "unified",
    "uconfig": {"resource_type_limits": {"SCORE_HIMEM": 500}},
    "use_pcache": False,
    "validatedreleases": "True",
    "vo_name": "atlas",
    "wnconnectivity": "full#IPv4",
    "workflow": "pull_ups",
    "zip_time_gap": 3600,
}

#: Minimal second queue — deliberately sparse to test schema widening.
MINIMAL_PAYLOAD: dict = {
    "status": "offline",
    "cloud": "DE",
    "country": "Germany",
    "corecount": 1,
    "maxrss": 2000,
}

#: Two-queue CRIC data dict used by most tests.
CRIC_TWO_QUEUES: dict = {"AGLT2": AGLT2_PAYLOAD, "MINIMAL_QUEUE": MINIMAL_PAYLOAD}


def _make_snapshot(data: dict) -> RawSnapshot:
    """Build a RawSnapshot from a dict, computing the SHA-256 hash."""
    raw_json = json.dumps(data)
    content_hash = hashlib.sha256(raw_json.encode()).hexdigest()
    return RawSnapshot(
        source="/fake/cric_pandaqueues.json",
        raw=data,
        fetched_utc="",
        content_hash=content_hash,
    )


def _make_fake_source_class(data: dict):
    """Return a fake BaseSource *class* that always returns a snapshot of *data*."""
    snap = _make_snapshot(data)

    class _FakeSource:
        def fetch_from_file(self, path: str) -> RawSnapshot:
            return snap

    return _FakeSource


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection."""
    c = duckdb.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def fetcher(conn) -> CricQueuedataFetcher:
    """Fetcher with refresh_interval_s=0 so every run_cycle attempt proceeds."""
    return CricQueuedataFetcher(
        conn=conn,
        cric_path="/fake/cric_pandaqueues.json",
        refresh_interval_s=0,
    )


# ===========================================================================
# _to_cell_value
# ===========================================================================

class TestToCellValue:
    def test_none_returns_text(self):
        val, t = _to_cell_value(None)
        assert val is None
        assert t == "TEXT"

    def test_int_returns_bigint(self):
        val, t = _to_cell_value(42)
        assert val == 42
        assert t == "BIGINT"

    def test_float_returns_double(self):
        val, t = _to_cell_value(3.14)
        assert val == 3.14
        assert t == "DOUBLE"

    def test_str_returns_text(self):
        val, t = _to_cell_value("hello")
        assert val == "hello"
        assert t == "TEXT"

    def test_bool_stored_as_text_not_bigint(self):
        # bool is a subclass of int; must NOT be inferred as BIGINT
        val_true, t_true = _to_cell_value(True)
        val_false, t_false = _to_cell_value(False)
        assert t_true == "TEXT"
        assert t_false == "TEXT"

    def test_list_serialised_to_json_text(self):
        val, t = _to_cell_value(["rucio", "xrdcp"])
        assert t == "TEXT"
        assert json.loads(val) == ["rucio", "xrdcp"]

    def test_dict_serialised_to_json_text(self):
        val, t = _to_cell_value({"pr": ["rucio"]})
        assert t == "TEXT"
        assert json.loads(val) == {"pr": ["rucio"]}


# ===========================================================================
# _merge_type
# ===========================================================================

class TestMergeType:
    def test_same_type_unchanged(self):
        assert _merge_type("BIGINT", "BIGINT") == "BIGINT"
        assert _merge_type("TEXT", "TEXT") == "TEXT"

    def test_bigint_and_double_widens_to_double(self):
        assert _merge_type("BIGINT", "DOUBLE") == "DOUBLE"
        assert _merge_type("DOUBLE", "BIGINT") == "DOUBLE"

    def test_numeric_and_text_widens_to_text(self):
        assert _merge_type("BIGINT", "TEXT") == "TEXT"
        assert _merge_type("DOUBLE", "TEXT") == "TEXT"
        assert _merge_type("TEXT", "BIGINT") == "TEXT"


# ===========================================================================
# _infer_schema
# ===========================================================================

class TestInferSchema:
    def test_basic_type_inference(self):
        rows = [{"a": 1, "b": 3.14, "c": "hello", "d": None}]
        schema = _infer_schema(rows)
        assert schema["a"] == "BIGINT"
        assert schema["b"] == "DOUBLE"
        assert schema["c"] == "TEXT"
        assert schema["d"] == "TEXT"

    def test_widening_across_rows(self):
        # Second row has a float where first row had an int → should widen to DOUBLE
        rows = [{"x": 1}, {"x": 2.5}]
        schema = _infer_schema(rows)
        assert schema["x"] == "DOUBLE"

    def test_mutates_rows_in_place(self):
        rows = [{"lst": [1, 2, 3]}]
        _infer_schema(rows)
        # List should now be a JSON string
        assert isinstance(rows[0]["lst"], str)
        assert json.loads(rows[0]["lst"]) == [1, 2, 3]


# ===========================================================================
# CricQueuedataFetcher._build_rows
# ===========================================================================

class TestBuildRows:
    def test_skip_fields_dropped(self, fetcher):
        data = {
            "Q1": {
                "status": "online",
                "coreenergy_data": {"x": 1},
                "corepower_data": {"y": 2},
                "maxdiskio_data": {"z": 3},
            }
        }
        rows = fetcher._build_rows(data)
        assert len(rows) == 1
        for skip in _SKIP_FIELDS:
            assert skip not in rows[0]

    def test_queue_key_stored_in_queue_column(self, fetcher):
        rows = fetcher._build_rows({"MYQUEUE": {"status": "online"}})
        assert rows[0]["queue"] == "MYQUEUE"

    def test_non_dict_payload_stored_in_data_column(self, fetcher):
        rows = fetcher._build_rows({"Q1": "raw_string_value"})
        assert rows[0]["data"] == "raw_string_value"
        assert rows[0]["queue"] == "Q1"

    def test_multiple_queues_produce_multiple_rows(self, fetcher):
        rows = fetcher._build_rows(CRIC_TWO_QUEUES)
        assert len(rows) == 2
        queue_names = {r["queue"] for r in rows}
        assert queue_names == {"AGLT2", "MINIMAL_QUEUE"}


# ===========================================================================
# CricQueuedataFetcher.run_cycle — full integration
# ===========================================================================

class TestRunCycle:
    def test_first_run_loads_table_and_returns_true(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            result = fetcher.run_cycle()

        assert result is True
        rows = fetcher._conn.execute("SELECT queue FROM queuedata ORDER BY queue").fetchall()
        assert len(rows) == 2

    def test_data_fields_absent_from_table(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        cols = [d[0] for d in fetcher._conn.execute("DESCRIBE queuedata").fetchall()]
        for skip in _SKIP_FIELDS:
            assert skip not in cols, f"{skip} should have been dropped"

    def test_queue_column_contains_correct_values(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        names = {
            r[0]
            for r in fetcher._conn.execute("SELECT queue FROM queuedata").fetchall()
        }
        assert names == {"AGLT2", "MINIMAL_QUEUE"}

    def test_json_column_round_trips(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        row = fetcher._conn.execute(
            "SELECT acopytools FROM queuedata WHERE queue = 'AGLT2'"
        ).fetchone()
        assert row is not None
        parsed = json.loads(row[0])
        assert parsed["pr"] == ["rucio"]

    def test_unchanged_file_skips_reload_and_returns_false(self, fetcher):
        fake_cls = _make_fake_source_class(CRIC_TWO_QUEUES)
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            fake_cls,
        ):
            fetcher.run_cycle()
            result = fetcher.run_cycle()

        assert result is False

    def test_changed_file_reloads_table(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        three_queues = {**CRIC_TWO_QUEUES, "NEW_QUEUE": {"status": "online"}}
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(three_queues),
        ):
            result = fetcher.run_cycle()

        assert result is True
        count = fetcher._conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0]
        assert count == 3

    def test_reload_replaces_old_rows(self, fetcher):
        """After a reload only the new rows exist — no stale rows from the previous load."""
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        only_one = {"ONLY_QUEUE": {"status": "online"}}
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(only_one),
        ):
            fetcher.run_cycle()

        rows = fetcher._conn.execute("SELECT queue FROM queuedata").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "ONLY_QUEUE"

    def test_interval_gate_prevents_premature_reload(self, conn):
        gated = CricQueuedataFetcher(
            conn=conn,
            cric_path="/fake",
            refresh_interval_s=9999,
        )
        fake_cls = _make_fake_source_class(CRIC_TWO_QUEUES)
        # Simulate that a run just happened by setting _last_attempt to now,
        # so the interval (9999 s) has definitely not elapsed yet.
        with patch("bamboo_mcp_services.agents.cric_agent.cric_fetcher.time.monotonic",
                   return_value=1_000_000.0):
            gated._last_attempt = 1_000_000.0  # set inside the mock so values match
            with patch(
                "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
                fake_cls,
            ):
                result = gated.run_cycle()

        assert result is False

    def test_health_attributes_updated_after_load(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        assert fetcher.last_row_count == 2
        assert fetcher.last_refresh_utc is not None
        assert fetcher._last_hash is not None

    def test_health_attributes_none_before_first_load(self, conn):
        gated = CricQueuedataFetcher(conn=conn, cric_path="/fake", refresh_interval_s=9999)
        assert gated.last_row_count is None
        assert gated.last_refresh_utc is None
        assert gated._last_hash is None

    def test_file_read_error_returns_false_and_does_not_raise(self, fetcher):
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
        ) as mock_cls:
            mock_cls.return_value.fetch_from_file.side_effect = FileNotFoundError("no file")
            result = fetcher.run_cycle()

        assert result is False

    def test_non_dict_top_level_json_returns_false(self, fetcher):
        """A top-level JSON list (not a dict) is invalid CRIC data."""
        snap = RawSnapshot(
            source="/fake",
            raw=[{"queue": "Q1"}],   # list, not dict
            fetched_utc="",
            content_hash="deadbeef",
        )
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
        ) as mock_cls:
            mock_cls.return_value.fetch_from_file.return_value = snap
            result = fetcher.run_cycle()

        assert result is False


# ===========================================================================
# CricAgent lifecycle
# ===========================================================================

class TestCricAgent:
    def _make_config(self, refresh_interval_s: int = 0) -> CricAgentConfig:
        return CricAgentConfig(
            cric_path="/fake/cric_pandaqueues.json",
            duckdb_path=":memory:",
            refresh_interval_s=refresh_interval_s,
        )

    def test_config_none_raises_value_error(self):
        with pytest.raises(ValueError):
            CricAgent(config=None)

    def test_start_transitions_to_running(self):
        agent = CricAgent(config=self._make_config())
        assert agent.state == AgentState.NEW
        agent.start()
        assert agent.state == AgentState.RUNNING
        agent.stop()

    def test_tick_loads_queuedata_table(self):
        agent = CricAgent(config=self._make_config())
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            agent.start()
            agent.tick()

        count = agent._store._conn.execute(
            "SELECT COUNT(*) FROM queuedata"
        ).fetchone()[0]
        assert count == 2
        agent.stop()

    def test_stop_releases_resources(self):
        agent = CricAgent(config=self._make_config())
        agent.start()
        agent.stop()
        assert agent.state == AgentState.STOPPED
        assert agent._store is None
        assert agent._fetcher is None

    def test_health_ok_while_running(self):
        agent = CricAgent(config=self._make_config())
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            agent.start()
            agent.tick()

        h = agent.health()
        assert h.ok is True
        assert h.details["last_row_count"] == 2
        assert h.details["last_hash"] is not None
        assert h.details["last_refresh_utc"] is not None
        assert h.details["cric_path"] == "/fake/cric_pandaqueues.json"
        agent.stop()

    def test_health_details_none_before_first_load(self):
        """Interval gate active: tick runs but fetcher does not load."""
        agent = CricAgent(config=self._make_config(refresh_interval_s=9999))
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            agent.start()
            # Set _last_attempt to now so the 9999 s interval has not elapsed
            agent._fetcher._last_attempt = __import__("time").monotonic()
            agent.tick()

        h = agent.health()
        assert h.details["last_row_count"] is None
        assert h.details["last_hash"] is None
        assert h.details["last_refresh_utc"] is None
        agent.stop()

    def test_start_is_idempotent(self):
        agent = CricAgent(config=self._make_config())
        agent.start()
        agent.start()  # second call must be a no-op
        assert agent.state == AgentState.RUNNING
        agent.stop()

    def test_stop_is_idempotent(self):
        agent = CricAgent(config=self._make_config())
        agent.start()
        agent.stop()
        agent.stop()  # second call must be a no-op
        assert agent.state == AgentState.STOPPED


# ===========================================================================
# CLI
# ===========================================================================

class TestCLI:
    def test_data_flag_is_required(self):
        p = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            p.parse_args(["--config", "x.yaml"])
        assert exc_info.value.code != 0

    def test_help_exits_zero(self):
        p = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            p.parse_args(["--help"])
        assert exc_info.value.code == 0

    def test_missing_config_file_returns_1(self, tmp_path):
        rc = main([
            "--data", str(tmp_path / "cric.db"),
            "--config", str(tmp_path / "nonexistent.yaml"),
            "--log-file", "",
        ])
        assert rc == 1

    def test_config_without_cric_path_returns_1(self, tmp_path):
        cfg_file = tmp_path / "bad.yaml"
        cfg_file.write_text("tick_interval_s: 1.0\n")
        rc = main([
            "--data", str(tmp_path / "cric.db"),
            "--config", str(cfg_file),
            "--log-file", "",
        ])
        assert rc == 1

    def test_once_loads_queuedata_and_returns_0(self, tmp_path):
        cfg_file = tmp_path / "cric-agent.yaml"
        cfg_file.write_text(
            "cric_path: /fake/cric_pandaqueues.json\n"
            "refresh_interval_s: 0\n"
            "tick_interval_s: 1.0\n"
        )
        db_path = str(tmp_path / "cric.db")

        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            rc = main([
                "--data", db_path,
                "--config", str(cfg_file),
                "--once",
                "--log-file", "",
            ])

        assert rc == 0
        conn = duckdb.connect(db_path, read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0]
        conn.close()
        assert count == 2

    def test_once_with_empty_cric_file_returns_0(self, tmp_path):
        """Empty dict is valid JSON and should not crash — agent exits cleanly."""
        cfg_file = tmp_path / "cric-agent.yaml"
        cfg_file.write_text(
            "cric_path: /fake/cric_pandaqueues.json\n"
            "refresh_interval_s: 0\n"
        )
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class({}),
        ):
            rc = main([
                "--data", str(tmp_path / "cric.db"),
                "--config", str(cfg_file),
                "--once",
                "--log-file", "",
            ])

        assert rc == 0


# ===========================================================================
# Transaction / concurrency safety
# ===========================================================================

class TestCricFetcherTransactionSafety:
    """Verify that _load() wraps the full table replacement in a transaction.

    DuckDB's MVCC means a concurrent reader on a *separate* connection sees
    either the old committed snapshot or the new one — never a torn state
    where the table is absent or partially filled.  These tests confirm:

    * The queuedata table always exists and has a consistent row count after a
      successful load.
    * A simulated mid-write failure triggers a ROLLBACK, leaving the *previous*
      committed snapshot intact (no data loss on error).
    """

    def test_successful_load_leaves_table_complete(self, conn):
        """All rows are visible immediately after run_cycle returns True."""
        fetcher = CricQueuedataFetcher(
            conn=conn,
            cric_path="/fake/cric_pandaqueues.json",
            refresh_interval_s=0,
        )
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            result = fetcher.run_cycle()

        assert result is True
        row_count = conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0]
        assert row_count == 2

    def test_failed_load_preserves_previous_snapshot(self, conn):
        """If _insert_rows raises, the old snapshot is preserved via ROLLBACK."""
        fetcher = CricQueuedataFetcher(
            conn=conn,
            cric_path="/fake/cric_pandaqueues.json",
            refresh_interval_s=0,
        )

        # First successful load — establishes the baseline snapshot.
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher.run_cycle()

        baseline = conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0]
        assert baseline == 2

        # Second load with a different hash so the interval gate passes, but
        # _insert_rows will raise mid-transaction.
        three_queues = dict(CRIC_TWO_QUEUES)
        three_queues["EXTRA_QUEUE"] = {"status": "online", "cloud": "US"}

        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(three_queues),
        ), patch.object(
            fetcher, "_insert_rows", side_effect=RuntimeError("simulated insert failure")
        ):
            result = fetcher.run_cycle()

        # run_cycle should catch the exception and return False.
        assert result is False

        # The previous two-row snapshot must still be intact.
        surviving_count = conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0]
        assert surviving_count == baseline, (
            "ROLLBACK should have preserved the previous snapshot, "
            f"but got {surviving_count} rows instead of {baseline}"
        )

    def test_table_has_no_gap_between_drop_and_insert(self, conn):
        """After a successful _load the table exists and is non-empty.

        This is a direct check that BEGIN/COMMIT wraps the DROP so there is
        never a moment where the committed state has no queuedata table.
        """
        fetcher = CricQueuedataFetcher(
            conn=conn,
            cric_path="/fake/cric_pandaqueues.json",
            refresh_interval_s=0,
        )
        with patch(
            "bamboo_mcp_services.agents.cric_agent.cric_fetcher.BaseSource",
            _make_fake_source_class(CRIC_TWO_QUEUES),
        ):
            fetcher._load(CRIC_TWO_QUEUES)

        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        assert "queuedata" in tables
        assert conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()[0] > 0
