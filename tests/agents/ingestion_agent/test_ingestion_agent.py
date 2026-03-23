"""Tests for the ingestion agent."""
from askpanda_atlas_agents.agents.ingestion_agent.agent import (
    IngestionAgent,
    IngestionAgentConfig,
    SourceConfig,
    BigPandaJobsConfig,
)


def test_ingestion_agent_file_source(tmp_path):
    """Test that the ingestion agent can fetch and store data from a file source."""
    p = tmp_path / 'q.json'
    p.write_text('{"queues":[{"name":"q1","site":"S1"}]}')
    s = SourceConfig(name='t1', type='cric', mode='file', path=str(p), interval_s=0)
    cfg = IngestionAgentConfig(
        sources=[s],
        duckdb_path=':memory:',
        tick_interval_s=0.01,
        bigpanda_jobs=BigPandaJobsConfig(enabled=False),
    )
    agent = IngestionAgent(config=cfg)
    agent.start()
    agent.tick()
    store = agent.store
    res = store._conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()
    assert res[0] >= 1
    agent.stop()
