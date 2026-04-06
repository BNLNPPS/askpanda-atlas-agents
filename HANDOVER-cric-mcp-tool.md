# Handover notes: CRIC queuedata for AskPanDA / Bamboo MCP

These notes are for the developer building the Bamboo MCP tool that answers
natural-language questions about ATLAS computing queues using the CRIC
queuedata database produced by the `cric-agent`.

---

## 1. What the database is and where it lives

The `cric` service (`bamboo-cric`) periodically reads the CRIC
`cric_pandaqueues.json` file from CVMFS and stores a full snapshot in a local
[DuckDB](https://duckdb.org) file.  The path is set via `--data PATH` when
the agent is started (there is no default).

DuckDB is an embedded, file-based analytical database — there is no server
process.  Any application that can `import duckdb` and read the file can query
it directly, and multiple readers can hold the file open simultaneously.

The database always holds **exactly one snapshot** — the most recently changed
version of the CRIC file.  There is no history table.  The table is replaced in
its entirety on each changed load.

---

## 2. Data model

The database contains two tables.

### `queuedata`

One row per ATLAS computing queue.  ~700 queues, ~90 columns.  The top-level
key from `cric_pandaqueues.json` is stored in the `queue` column; all other
fields become columns.  Structured values (CE lists, storage dicts, Harvester
params) are serialised to JSON TEXT and accessible via DuckDB's `json_extract`.

The most useful columns for the MCP tool:

| Column | Type | Meaning |
|---|---|---|
| `queue` | VARCHAR | PanDA queue identifier — the primary key |
| `status` | VARCHAR | Brokerage status: `online`, `offline`, `test`, `brokeroff` |
| `state` | VARCHAR | CRIC record state: `ACTIVE`, `INACTIVE` |
| `cloud` | VARCHAR | Logical PanDA cloud code: `US`, `DE`, `CERN`, `UK`, … |
| `country` | VARCHAR | Full country name: `United States`, `Germany`, … |
| `tier` | VARCHAR | WLCG tier label: `T0`, `T1`, `T2`, `T2D`, `T3` |
| `tier_level` | BIGINT | Numeric tier: 1, 2, or 3 |
| `site` | VARCHAR | PanDA site grouping (one site covers multiple queues) |
| `atlas_site` | VARCHAR | ATLAS site name |
| `rc_site` | VARCHAR | WLCG resource-centre name |
| `corecount` | BIGINT | CPU cores per job |
| `corepower` | DOUBLE | Benchmark power per core in HS06 |
| `maxrss` | BIGINT | Max RSS memory per job in MB |
| `maxtime` | BIGINT | Max wall-clock time per job in seconds |
| `nodes` | BIGINT | Worker nodes at the site |
| `harvester` | VARCHAR | Harvester service instance (null if none) |
| `pilot_manager` | VARCHAR | Pilot submission framework |
| `type` | VARCHAR | Queue dispatch type: `unified`, `production`, `analysis` |
| `workflow` | VARCHAR | Submission workflow, e.g. `pull_ups` |
| `resource_type` | VARCHAR | Compute resource type: `GRID`, `CLOUD`, `HPC` |
| `is_cvmfs` | VARCHAR | Whether CVMFS is available (`True`/`False`) |
| `direct_access_lan` | VARCHAR | LAN direct data access allowed (`True`/`False`) |
| `hc_param` | VARCHAR | HammerCloud mode: `False`, `OnlyTest`, `AutoExclusion` |
| `hc_suite` | TEXT (JSON) | HC test suites: `["AFT", "PFT"]` etc. |
| `queues` | TEXT (JSON) | List of CE endpoint records |
| `acopytools` | TEXT (JSON) | Copy-tool config by activity type |
| `astorages` | TEXT (JSON) | RSE assignments by activity type |
| `params` | TEXT (JSON) | Harvester / dispatch config overrides |
| `last_modified` | VARCHAR | UTC timestamp of last CRIC modification |

> **Note on boolean columns**: `is_cvmfs`, `direct_access_lan`,
> `direct_access_wan`, `is_default`, `is_virtual`, `use_pcache` are stored as
> VARCHAR (`'True'` / `'False'`), not as DuckDB BOOLEAN.  Filter with
> `= 'True'` not `= TRUE`.

### `snapshots`

Internal bookkeeping.  One row per fetch attempt by the agent (success or
failure, content hash, timestamp).  Useful for freshness checks but not for
queue queries.

```sql
-- When was the database last updated?
SELECT fetched_utc, ok, content_hash
FROM snapshots
ORDER BY fetched_utc DESC
LIMIT 1;
```

---

## 3. Annotated schema — use this for LLM prompt construction

Every column in `queuedata` has a plain-English description in:

```
src/bamboo_mcp_services/common/storage/schema_annotations.py
```

The key export for the MCP tool is `get_queuedata_schema_context()`:

```python
from bamboo_mcp_services.common.storage.schema_annotations import (
    get_queuedata_schema_context,
    QUEUEDATA_FIELD_DESCRIPTIONS,   # dict: column name → description
)

# Returns a compact "Table: queuedata / col  TYPE  description" block
context = get_queuedata_schema_context()
```

Use it to build the LLM system prompt:

```python
system_prompt = f"""You are a helpful assistant for ATLAS computing operations.
You answer questions about ATLAS computing queues by writing and executing SQL
queries against a DuckDB database containing CRIC queue metadata.

{get_queuedata_schema_context()}

Rules:
- The table name is always 'queuedata'. There is one row per queue.
- 'queue' is the primary identifier. 'site' groups multiple queues.
- Boolean-looking columns (is_cvmfs, direct_access_lan, etc.) are VARCHAR:
  use = 'True' / = 'False', not = TRUE / = FALSE.
- JSON columns (queues, acopytools, astorages, params, hc_suite) require
  json_extract() or json()::STRING LIKE for filtering.
- 'status' is the brokerage state (online/offline/test/brokeroff).
  'state' is the CRIC record state (ACTIVE/INACTIVE). These are different.
- Always quote column names that are SQL reserved words: "type", "state",
  "comment", "region".
"""
```

---

## 4. Opening the database from Python

```python
import duckdb

# Read-only — safe to open while the cric-agent is running.
conn = duckdb.connect("/path/to/cric.db", read_only=True)

# Basic sanity check
row_count, = conn.execute("SELECT COUNT(*) FROM queuedata").fetchone()
print(f"{row_count} queues in database")
```

No schema initialisation is needed before querying — the `queuedata` table
is always present as long as the agent has completed at least one successful
load.  To handle the edge case where the agent has never run, wrap queries in
a try/except for `duckdb.CatalogException`.

---

## 5. Example SQL queries

**All online queues in the US cloud:**

```sql
SELECT queue, tier, corecount, corepower, nodes
FROM queuedata
WHERE status = 'online' AND cloud = 'US'
ORDER BY tier, queue;
```

**All Tier-1 sites and how many queues each has:**

```sql
SELECT site, COUNT(*) AS queue_count
FROM queuedata
WHERE tier_level = 1
GROUP BY site
ORDER BY site;
```

**Queues at a given site:**

```sql
SELECT queue, status, "type", corecount, maxrss
FROM queuedata
WHERE site = 'GreatLakesT2'
ORDER BY queue;
```

**Which queues are currently offline or in test mode?**

```sql
SELECT queue, status, cloud, "state", state_comment
FROM queuedata
WHERE status IN ('offline', 'test', 'brokeroff')
ORDER BY status, cloud, queue;
```

**Queues managed by a specific Harvester instance:**

```sql
SELECT queue, harvester_template, "type"
FROM queuedata
WHERE harvester = 'CERN_central_A'
ORDER BY queue;
```

**Queues that support HammerCloud AFT tests:**

```sql
SELECT queue, hc_param, hc_suite
FROM queuedata
WHERE json(hc_suite)::STRING LIKE '%"AFT"%'
ORDER BY queue;
```

**How many cores are available per cloud (online queues only):**

```sql
SELECT cloud,
       COUNT(*) AS queue_count,
       SUM(corecount * nodes) AS total_cores
FROM queuedata
WHERE status = 'online'
  AND corecount IS NOT NULL
  AND nodes IS NOT NULL
GROUP BY cloud
ORDER BY total_cores DESC;
```

**Extract the first CE endpoint for a queue (JSON column):**

```sql
SELECT queue,
       json_extract(queues, '$[0].ce_endpoint') AS first_ce,
       json_extract(queues, '$[0].ce_flavour')  AS ce_flavour
FROM queuedata
WHERE queue = 'AGLT2';
```

---

## 6. Tool design recommendations

### Architecture

The simplest pattern for the first version:

1. User asks a natural-language question in Bamboo.
2. The MCP tool calls the LLM with a system prompt containing
   `get_queuedata_schema_context()` and a few-shot (question → SQL → answer)
   example.
3. The LLM returns a SQL query.
4. The tool executes the query against the read-only DuckDB connection.
5. The result is formatted and returned to the user.

### Handling site name ambiguity

Users will say "CERN" or "BNL" but these can refer to different things:

- `cloud = 'CERN'` — the CERN logical cloud (many queues)
- `site = 'BNL_PROD'` — the BNL PanDA site (groups multiple queues)
- `queue = 'BNL'` — a specific queue called BNL
- `atlas_site = 'BNL'` — the ATLAS site name

Inject a clarification note into the system prompt, or use `LIKE` predicates
as a fallback: `WHERE queue LIKE '%BNL%' OR site LIKE '%BNL%'`.

### Reserved word quoting

Several CRIC column names are SQL reserved words and must be double-quoted in
queries: `"type"`, `"state"`, `"comment"`, `"region"`.  The LLM should be
told this explicitly in the system prompt — it is a common source of query
errors.

### Returning results

For counting questions return a single number with a sentence.  For listing
questions return a formatted table, limited to a reasonable number of rows
(20–50), with a note if more exist.  For site/queue lookup questions, include
`status` and `cloud` in the result even if the user did not ask for them —
it is almost always contextually useful.

### Safety

Always open the connection read-only (`read_only=True`).  If the LLM generates
a mutating statement, DuckDB will raise an exception — catch it and return an
error rather than crashing.

### Data freshness

The `queuedata` table reflects the CRIC file at the time of the last successful
agent load.  CVMFS propagates CRIC updates roughly every 30 minutes; the agent
polls every 10 minutes.  In practice the data is at most ~40 minutes old under
normal conditions.  Tell the user the last refresh time when relevant:

```python
row = conn.execute(
    "SELECT fetched_utc FROM snapshots WHERE ok = true ORDER BY fetched_utc DESC LIMIT 1"
).fetchone()
last_updated = row[0] if row else "unknown"
```

---

## 7. Relationship to the jobs database

The CRIC agent produces `cric.db` (`queuedata` table).
The ingestion agent produces `jobs.duckdb` (`jobs`, `selectionsummary`,
`errors_by_count` tables).

These are separate files.  A Bamboo tool can open both read-only simultaneously
— DuckDB supports multiple concurrent readers.  Cross-database queries require
the DuckDB `ATTACH` statement:

```sql
ATTACH '/path/to/jobs.duckdb' AS jobs_db (READ_ONLY);

-- Jobs at online US queues right now
SELECT j.pandaid, j.jobstatus, j.computingsite
FROM jobs_db.jobs j
JOIN queuedata q ON j.computingsite = q.queue
WHERE q.status = 'online'
  AND q.cloud = 'US'
LIMIT 20;
```

---

## 8. Key files to read before starting

| File | Purpose |
|---|---|
| `src/bamboo_mcp_services/common/storage/schema_annotations.py` | `QUEUEDATA_FIELD_DESCRIPTIONS` and `get_queuedata_schema_context()` for LLM prompts |
| `src/bamboo_mcp_services/agents/cric_agent/cric_fetcher.py` | How the table is built; type inference; which fields are dropped |
| `src/bamboo_mcp_services/resources/config/cric-agent.yaml` | Default agent configuration |
| `README-cric_agent.md` | Full agent documentation including more query examples |
| `HANDOVER-bamboo-sql-tool.md` | Equivalent notes for the jobs database MCP tool |
