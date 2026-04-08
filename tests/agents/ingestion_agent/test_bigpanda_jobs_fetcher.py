"""Tests for BigPanda jobs fetcher and schema."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from bamboo_mcp_services.common.storage.schema import apply_schema, table_names
from bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher import (
    BigPandaJobsFetcher,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory DuckDB connection with schema applied."""
    c = duckdb.connect(":memory:")
    apply_schema(c)
    yield c
    c.close()


@pytest.fixture
def sample_payload():
    """Minimal realistic BigPanda API payload."""
    return {
        "jobs": [
            {
                "pandaid": 12345,
                "computingsite": "SWT2_CPB",
                "jobstatus": "finished",
                "taskid": 99,
                "jeditaskid": 100,
                "creationtime": "2026-03-23 10:00:00",
                "modificationtime": "2026-03-23 10:30:00",
                "durationsec": 1800.0,
                "cpuefficiency": 0.95,
            },
            {
                "pandaid": 12346,
                "computingsite": "SWT2_CPB",
                "jobstatus": "failed",
                "taskid": 99,
                "jeditaskid": 100,
                "creationtime": "2026-03-23 10:01:00",
                "modificationtime": "2026-03-23 10:15:00",
                "piloterrorcode": 1099,
                "piloterrordiag": "payload error",
            },
        ],
        "selectionsummary": [
            {
                "field": "jobstatus",
                "list": [
                    {"kname": "finished", "kvalue": 1},
                    {"kname": "failed", "kvalue": 1},
                ],
                "stats": {"sum": 2},
            }
        ],
        "errsByCount": [
            {
                "error": "pilot",
                "codename": "payload_error",
                "codeval": 1099,
                "diag": "payload error",
                "desc": "Payload failed",
                "example_pandaid": 12346,
                "count": 1,
                "pandalist": [12346],
            }
        ],
    }


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_apply_schema_is_idempotent(self):
        c = duckdb.connect(":memory:")
        apply_schema(c)
        apply_schema(c)  # must not raise
        c.close()

    def test_all_tables_created(self, conn):
        for tbl in table_names():
            result = conn.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{tbl}'"
            ).fetchone()
            assert result[0] == 1, f"Table {tbl!r} was not created"

    def test_jobs_has_primary_key(self, conn):
        # Inserting the same pandaid twice should replace (not duplicate) the row.
        conn.execute(
            "INSERT INTO jobs (pandaid, _queue, _fetched_utc) VALUES (1, 'Q', '2026-01-01 00:00:00')"
        )
        conn.execute(
            "INSERT OR REPLACE INTO jobs (pandaid, jobstatus, _queue, _fetched_utc) "
            "VALUES (1, 'finished', 'Q', '2026-01-01 01:00:00')"
        )
        count = conn.execute("SELECT count(*) FROM jobs WHERE pandaid = 1").fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# BigPandaJobsFetcher tests
# ---------------------------------------------------------------------------

class TestBigPandaJobsFetcher:
    def _make_fetcher(self, conn, **kwargs):
        return BigPandaJobsFetcher(
            conn=conn,
            queues=["TEST_Q"],
            cycle_interval_s=3600,
            inter_queue_delay_s=0,  # no delay in tests
            **kwargs,
        )

    def test_run_cycle_skipped_before_interval(self, conn):
        fetcher = self._make_fetcher(conn)
        # Manually set last cycle time to now so interval hasn't elapsed.
        fetcher._last_cycle_time = time.monotonic()
        ran = fetcher.run_cycle()
        assert ran is False

    def test_run_cycle_force(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)
        fetcher._last_cycle_time = time.monotonic()  # would normally skip

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            ran = fetcher.run_cycle(force=True)

        assert ran is True

    def test_jobs_upserted(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)

        count = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
        assert count == 2

        row = conn.execute(
            "SELECT jobstatus, _queue FROM jobs WHERE pandaid = 12345"
        ).fetchone()
        assert row is not None
        assert row[0] == "finished"
        assert row[1] == "TEST_Q"

    def test_upsert_replaces_on_second_run(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)
            fetcher.run_cycle(force=True)

        # Should still be 2 rows, not 4.
        count = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
        assert count == 2

    def test_selection_summary_persisted(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)

        row = conn.execute(
            "SELECT field, list_json FROM selectionsummary WHERE _queue = 'TEST_Q'"
        ).fetchone()
        assert row is not None
        assert row[0] == "jobstatus"
        parsed = json.loads(row[1])
        assert len(parsed) == 2

    def test_selection_summary_replaced_on_second_run(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)
            fetcher.run_cycle(force=True)

        count = conn.execute(
            "SELECT count(*) FROM selectionsummary WHERE _queue = 'TEST_Q'"
        ).fetchone()[0]
        assert count == 1  # one summary item, replaced not duplicated

    def test_errors_persisted(self, conn, sample_payload):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)

        row = conn.execute(
            "SELECT codeval, count FROM errors_by_count WHERE _queue = 'TEST_Q'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1099
        assert row[1] == 1

    def test_empty_payload_does_not_raise(self, conn):
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = {"jobs": [], "selectionsummary": [], "errsByCount": []}
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle(force=True)  # must not raise

    def test_inter_queue_delay_called(self, conn, sample_payload):
        """Verify time.sleep is called between queues."""
        fetcher = BigPandaJobsFetcher(
            conn=conn,
            queues=["Q1", "Q2"],
            cycle_interval_s=0,
            inter_queue_delay_s=42,
        )

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ) as mock_get, patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.BigPandaJobsFetcher._interruptible_sleep"
        ) as mock_sleep:
            fetcher.run_cycle(force=True)

        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(42)

    def test_no_sleep_after_last_queue(self, conn, sample_payload):
        """No sleep should happen after the final queue."""
        fetcher = BigPandaJobsFetcher(
            conn=conn,
            queues=["ONLY_Q"],
            cycle_interval_s=0,
            inter_queue_delay_s=99,
        )

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ), patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.BigPandaJobsFetcher._interruptible_sleep"
        ) as mock_sleep:
            fetcher.run_cycle(force=True)

        mock_sleep.assert_not_called()

    def test_failed_queue_does_not_stop_cycle(self, conn, sample_payload):
        """If one queue fails, the remaining queues should still be fetched."""
        fetcher = BigPandaJobsFetcher(
            conn=conn,
            queues=["BAD_Q", "GOOD_Q"],
            cycle_interval_s=0,
            inter_queue_delay_s=0,
        )

        good_response = MagicMock()
        good_response.json.return_value = sample_payload
        good_response.raise_for_status.return_value = None

        def side_effect(url, **kwargs):
            if "BAD_Q" in url:
                raise ConnectionError("timeout")
            return good_response

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            side_effect=side_effect,
        ):
            fetcher.run_cycle(force=True)  # must not raise

        count = conn.execute("SELECT count(*) FROM jobs WHERE _queue = 'GOOD_Q'").fetchone()[0]
        assert count == 2


class TestOneShot:
    def _make_fetcher(self, conn, **kwargs):
        return BigPandaJobsFetcher(
            conn=conn,
            queues=["Q1", "Q2"],
            cycle_interval_s=0,
            inter_queue_delay_s=99,  # would block for 99s without one_shot
            **kwargs,
        )

    def test_one_shot_skips_inter_queue_delay(self, conn, sample_payload):
        """one_shot=True must not call time.sleep between queues."""
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ), patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.BigPandaJobsFetcher._interruptible_sleep"
        ) as mock_sleep:
            fetcher.run_cycle(force=True, one_shot=True)

        mock_sleep.assert_not_called()

    def test_normal_mode_still_sleeps_between_queues(self, conn, sample_payload):
        """Without one_shot, the inter-queue sleep must still fire."""
        fetcher = self._make_fetcher(conn)

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ), patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.BigPandaJobsFetcher._interruptible_sleep"
        ) as mock_sleep:
            fetcher.run_cycle(force=True, one_shot=False)

        mock_sleep.assert_called()

    def test_keyboard_interrupt_from_duckdb_propagates(self, conn):
        """RuntimeError('Query interrupted') caused by KeyboardInterrupt must re-raise as KeyboardInterrupt."""
        fetcher = BigPandaJobsFetcher(conn=conn, queues=["Q1"], cycle_interval_s=0)

        ki = KeyboardInterrupt()
        rt = RuntimeError("Query interrupted")
        rt.__context__ = ki

        with patch.object(fetcher, "_fetch_and_persist", side_effect=rt):
            with pytest.raises(KeyboardInterrupt):
                fetcher.run_cycle(force=True)

    def test_tick_once_on_agent(self, conn, sample_payload):
        """IngestionAgent.tick_once() must complete without inter-queue delay."""
        from bamboo_mcp_services.agents.ingestion_agent.agent import (
            IngestionAgent, IngestionAgentConfig, BigPandaJobsConfig,
        )

        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        cfg = IngestionAgentConfig(
            sources=[],
            duckdb_path=":memory:",
            bigpanda_jobs=BigPandaJobsConfig(
                queues=["Q1", "Q2"],
                cycle_interval_s=0,
                inter_queue_delay_s=99,
            ),
        )
        agent = IngestionAgent(config=cfg)
        agent.start()

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ), patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.BigPandaJobsFetcher._interruptible_sleep"
        ) as mock_sleep:
            agent.tick_once()

        mock_sleep.assert_not_called()
        agent.stop()


# ===========================================================================
# Transaction / concurrency safety
# ===========================================================================

class TestBigPandaFetcherTransactionSafety:
    """Verify that _fetch_and_persist() wraps all three table writes in one transaction.

    A concurrent reader must never see a state where some tables have been
    updated for a queue cycle and others have not.  These tests confirm:

    * All three tables are updated atomically on success.
    * A simulated mid-write failure triggers a ROLLBACK, leaving the previous
      committed data intact (no partial updates).
    """

    def test_all_three_tables_updated_on_success(self, conn, sample_payload):
        """jobs, selectionsummary, and errors_by_count are all populated after a cycle."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        fetcher = BigPandaJobsFetcher(conn=conn, queues=["SWT2_CPB"], cycle_interval_s=0)
        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle()

        job_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        summary_count = conn.execute("SELECT COUNT(*) FROM selectionsummary").fetchone()[0]
        error_count = conn.execute("SELECT COUNT(*) FROM errors_by_count").fetchone()[0]

        assert job_count > 0
        assert summary_count > 0
        assert error_count > 0

    def test_failed_mid_write_rolls_back_all_tables(self, conn, sample_payload):
        """If _insert_errors raises, neither jobs nor selectionsummary are updated."""
        # Populate baseline data for SWT2_CPB.
        mock_response = MagicMock()
        mock_response.json.return_value = sample_payload
        mock_response.raise_for_status.return_value = None

        fetcher = BigPandaJobsFetcher(conn=conn, queues=["SWT2_CPB"], cycle_interval_s=0)
        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ):
            fetcher.run_cycle()

        baseline_jobs = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE _queue = 'SWT2_CPB'"
        ).fetchone()[0]
        assert baseline_jobs > 0

        # Second cycle: _insert_errors will raise after jobs and summary are written.
        new_payload = dict(sample_payload)
        new_payload["jobs"] = [
            {
                "pandaid": 99999,
                "computingsite": "SWT2_CPB",
                "jobstatus": "running",
                "taskid": 1,
                "jeditaskid": 2,
            }
        ]
        mock_response.json.return_value = new_payload
        fetcher._last_cycle_time = 0.0  # reset interval so the cycle runs again

        with patch(
            "bamboo_mcp_services.agents.ingestion_agent.bigpanda_jobs_fetcher.requests.get",
            return_value=mock_response,
        ), patch.object(
            fetcher, "_insert_errors", side_effect=RuntimeError("simulated error-table failure")
        ):
            fetcher.run_cycle()

        # The baseline job (pandaid=12345) should still be present; pandaid=99999 must not.
        surviving_ids = [
            r[0] for r in conn.execute("SELECT pandaid FROM jobs WHERE _queue = 'SWT2_CPB'").fetchall()
        ]
        assert 12345 in surviving_ids, "Baseline row must survive ROLLBACK"
        assert 99999 not in surviving_ids, "New row must not be committed after ROLLBACK"
