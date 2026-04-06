# Handover notes: PanDA jobs database for AskPanDA / Bamboo

These notes are intended for the developer building the new AskPanDA/Bamboo
tool that answers natural-language questions about PanDA job data by querying
the ingestion-agent DuckDB database.

---

## 1. What the database is and where it lives

The ingestion-agent (`bamboo-ingestion`) periodically downloads job
data from the BigPanda monitoring service and persists it in a local
[DuckDB](https://duckdb.org) file, by default `jobs.duckdb` in the agent's
working directory.  The path is configurable via `duckdb_path` in
`ingestion-agent.yaml`.

DuckDB is an embedded, file-based analytical database — there is no server
process.  Any application that can `import duckdb` and read the file can query
it directly.

---

## 2. Data model and tables

The database contains three data tables and one bookkeeping table.

### `jobs`

The main table.  One row per PanDA job, keyed on `pandaid`.  Rows are upserted
on every ingestion cycle, so each row always reflects the latest known state of
that job.

The BigPanda endpoint queried is:

```
https://bigpanda.cern.ch/jobs/?computingsite=<QUEUE>&json&hours=1
```

**This is the critical data freshness constraint**: the source API only returns
jobs active or completed within the last hour.  Jobs that left that window will
remain in the local database with their last-known state but will no longer
receive updates.  See section 7 for implications and a path forward.

Important columns for the first tool:

| Column | Type | Meaning |
|---|---|---|
| `pandaid` | BIGINT | Unique job identifier (primary key) |
| `jobstatus` | VARCHAR | `defined`, `waiting`, `sent`, `starting`, `running`, `holding`, `merging`, `finished`, `failed`, `cancelled`, `closed` |
| `computingsite` | VARCHAR | Queue / site name — same as `_queue` |
| `_queue` | VARCHAR | Ingestion bookkeeping: which queue was polled |
| `_fetched_utc` | TIMESTAMP | When the ingestion agent last fetched this row |
| `creationtime` | TIMESTAMP | When the job was created in PanDA |
| `modificationtime` | TIMESTAMP | Last PanDA update time |
| `statechangetime` | TIMESTAMP | Last status transition time |
| `produserid` | VARCHAR | User DN or production role that submitted the job |
| `taskid` / `jeditaskid` | BIGINT | Task the job belongs to |
| `piloterrorcode` / `piloterrordiag` | INT / VARCHAR | Pilot-level error code and description |
| `exeerrorcode` / `exeerrordiag` | INT / VARCHAR | Payload-level error code and description |
| `cpuefficiency` | DOUBLE | CPU efficiency ratio (0.0–1.0+) |
| `durationsec` | DOUBLE | Wall-clock run time in seconds |

### `selectionsummary`

Pre-aggregated facet counts from BigPanda, replaced on every cycle.  One row
per facet field per queue.  Useful for fast "how many jobs per status" queries
without scanning the full `jobs` table.

| Column | Meaning |
|---|---|
| `field` | Facet name, e.g. `jobstatus`, `cloud`, `gshare` |
| `list_json` | JSON array of `{kname, kvalue}` — value and count pairs |
| `stats_json` | JSON aggregate stats, e.g. `{"sum": 9928}` |
| `_queue` | Source queue |

Example: to get the job-status breakdown for BNL without touching the `jobs`
table:

```sql
SELECT field, list_json
FROM selectionsummary
WHERE _queue = 'BNL'
  AND field = 'jobstatus';
```

### `errors_by_count`

Ranked error frequency table from BigPanda, replaced on every cycle.  One row
per error code per queue.

| Column | Meaning |
|---|---|
| `error` | Error category: `pilot`, `exe`, `ddm`, `brokerage`, etc. |
| `codename` | Symbolic error name |
| `codeval` | Numeric error code |
| `count` | Number of jobs currently affected |
| `example_pandaid` | A representative job with this error |

### `snapshots`

Internal ingestion bookkeeping.  Records each fetch attempt (success/failure,
content hash, timestamp).  Not directly useful for job queries.

---

## 3. Annotated schema — use this for LLM prompt construction

Every column in all three tables has a plain-English description in:

```
src/bamboo_mcp_services/common/storage/schema_annotations.py
```

The module exports:

```python
from bamboo_mcp_services.common.storage.schema_annotations import (
    JOBS_FIELD_DESCRIPTIONS,        # dict: column name → description
    ALL_FIELD_DESCRIPTIONS,         # merged dict across all tables
    TABLE_FIELD_DESCRIPTIONS,       # dict: table name → {column → description}
    get_schema_context,             # returns a formatted string for LLM prompts
)
```

**`get_schema_context()`** is the key function for the new tool.  It returns a
compact multi-line summary of every table, column, type, and description,
ready to paste into a system prompt:

```python
from bamboo_mcp_services.common.storage.schema_annotations import get_schema_context

system_prompt = f"""You are a helpful assistant for ATLAS computing operations.
You answer questions about PanDA jobs by writing and executing SQL queries
against a DuckDB database.

{get_schema_context()}

Rules:
- Always filter by _queue to scope queries to the right site.
- The database only contains jobs from the last ~1 hour per queue.
- Use _fetched_utc to find the most recent snapshot.
- jobstatus values are: defined, waiting, sent, starting, running,
  holding, merging, finished, failed, cancelled, closed.
"""
```

You can also pass a subset of tables to keep the context shorter:

```python
get_schema_context(["jobs"])               # jobs table only
get_schema_context(["jobs", "errors_by_count"])
```

---

## 4. Opening the database from Python

The database file is opened in **read-only** mode by any consumer other than
the ingestion agent itself:

```python
import duckdb
from bamboo_mcp_services.common.storage.schema_annotations import apply_schema

# Read-only — safe to open while the ingestion agent is running.
conn = duckdb.connect("jobs.duckdb", read_only=True)
```

`apply_schema()` from `schema.py` can be called to guarantee the expected
tables exist before querying (it is idempotent):

```python
from bamboo_mcp_services.common.storage.schema import apply_schema
apply_schema(conn)
```

---

## 5. Example SQL queries for the first tool

These directly answer the example questions from the task brief.

**How many jobs failed at BNL during the last hour?**

```sql
SELECT COUNT(*) AS failed_jobs
FROM jobs
WHERE _queue = 'BNL'
  AND jobstatus = 'failed';
```

> Note: because the source API only returns jobs from the last hour, every row
> in the `jobs` table for a given queue already represents the last-hour window.
> No additional time filter is needed until historical data is available
> (see section 7).

Alternatively, use the pre-aggregated `selectionsummary` for speed:

```sql
SELECT json_extract(value, '$.kvalue')::INTEGER AS count
FROM selectionsummary,
     json_each(list_json) AS t(value)
WHERE _queue = 'BNL'
  AND field = 'jobstatus'
  AND json_extract(value, '$.kname') = 'failed';
```

**Which are the finished jobs at BNL?**

```sql
SELECT pandaid, jobname, produserid, durationsec, cpuefficiency
FROM jobs
WHERE _queue = 'BNL'
  AND jobstatus = 'finished'
ORDER BY _fetched_utc DESC
LIMIT 50;
```

**What are the top errors at SWT2_CPB right now?**

```sql
SELECT error, codename, codeval, count, error_desc_text
FROM errors_by_count
WHERE _queue = 'SWT2_CPB'
ORDER BY count DESC
LIMIT 10;
```

**How many jobs are in each status at BNL?**

```sql
SELECT jobstatus, COUNT(*) AS n
FROM jobs
WHERE _queue = 'BNL'
GROUP BY jobstatus
ORDER BY n DESC;
```

**Data freshness check — when was the database last updated?**

```sql
SELECT _queue, MAX(_fetched_utc) AS last_fetched, COUNT(*) AS job_count
FROM jobs
GROUP BY _queue
ORDER BY last_fetched DESC;
```

---

## 6. Tool design recommendations

### Architecture

The simplest architecture for the first version:

1. User asks a natural-language question in Bamboo.
2. The tool calls the LLM with a system prompt containing `get_schema_context()`
   and a few-shot example of (question → SQL → answer).
3. The LLM returns a SQL query.
4. The tool executes the SQL against the read-only DuckDB connection.
5. The result is formatted and returned to the user.

This is a standard text-to-SQL pattern.  DuckDB supports the full SQL dialect
so the LLM can write complex queries, aggregations, and JSON extraction without
any special handling.

### Handling ambiguous site names

Users will say "CERN" but the queue name in the database may be `CERN_PROD`,
`CERN_IT`, or similar.  Two options:

- Maintain a `site_aliases` dict mapping common names to `_queue` values and
  inject it into the prompt.
- Use a `LIKE` predicate: `WHERE _queue LIKE '%CERN%'`.

### Returning results

For counting questions ("how many...") return a single number with a sentence.
For listing questions ("which jobs...") return a formatted table, limited to a
reasonable number of rows (e.g. 20), with a note if more exist.

### Safety

Always open the connection read-only (`read_only=True`).  If the LLM generates
a mutating statement (`INSERT`, `UPDATE`, `DELETE`, `DROP`), DuckDB will raise
an exception — catch it and return an error message rather than crashing.

---

## 7. Current limitations and the path to historical queries

### The 1-hour window constraint

The BigPanda API endpoint used is:

```
/jobs/?computingsite=<QUEUE>&json&hours=1
```

This means each ingestion cycle captures a snapshot of jobs active in the last
hour.  Jobs that finished more than an hour ago and have not been re-polled
will remain in the database with their final state, but:

- They will not be updated if their state changes (unlikely for finished/failed
  jobs, but possible).
- There is no guarantee about how far back the database actually extends — it
  depends on how long the ingestion agent has been running.

For the first tool, answer the question honestly: *"The database reflects jobs
active at the configured queues within approximately the last hour."*

### Answering "yesterday" questions

To answer "how many jobs failed at CERN yesterday?" the database needs:

1. **Retention** — rows must be kept after they leave the 1-hour window.  The
   current upsert strategy already does this (rows are never deleted from
   `jobs`), so the table does accumulate history as long as the agent runs.

2. **A reliable timestamp to filter on** — the right column is `statechangetime`
   (last status transition) or `modificationtime`, not `_fetched_utc` (which
   is when the agent polled, not when the job changed state).

3. **A wider API window** — optionally, the `hours` parameter in the BigPanda
   URL could be increased (e.g. `hours=24`) to capture more history per cycle,
   at the cost of larger payloads.  This is a one-line config change in
   `bigpanda_jobs_fetcher.py`.

4. **Table partitioning or archival** — for long-running deployments the `jobs`
   table will grow unboundedly.  A future task is to add a periodic archival
   step that moves old rows to a separate `jobs_history` table or a Parquet
   file, and queries both as a union.

The SQL pattern for historical queries once retention is established:

```sql
-- Jobs that failed at BNL on a specific day
SELECT COUNT(*) AS failed_jobs
FROM jobs
WHERE _queue = 'BNL'
  AND jobstatus = 'failed'
  AND DATE(statechangetime) = '2026-03-22';

-- Jobs that failed at any CERN queue yesterday
SELECT COUNT(*) AS failed_jobs
FROM jobs
WHERE _queue LIKE '%CERN%'
  AND jobstatus = 'failed'
  AND statechangetime >= CURRENT_DATE - INTERVAL '1 day'
  AND statechangetime < CURRENT_DATE;
```

---

## 8. Key files to read before starting

| File | Purpose |
|---|---|
| `src/bamboo_mcp_services/common/storage/schema_annotations.py` | All column descriptions; `get_schema_context()` for LLM prompts |
| `src/bamboo_mcp_services/common/storage/schema.py` | DDL for all tables; `apply_schema()` to ensure schema on open |
| `src/bamboo_mcp_services/resources/config/ingestion-agent.yaml` | Queue list, poll interval, database path |
| `README-ingestion_agent.md` | Full ingestion-agent documentation |
| `scripts/dump_ingestion_db.py` | Command-line database inspector; useful during development |
