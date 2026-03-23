"""Human-readable annotations for all fields in the ingestion-agent DuckDB schema.

This module provides plain-English descriptions of every column in the
``jobs``, ``selectionsummary``, and ``errors_by_count`` tables.  The
annotations are intended to be injected into LLM prompts so the model
understands what each field means when a user asks a question that requires
a database lookup.

The descriptions are intentionally written from the perspective of someone
unfamiliar with PanDA internals — they explain *what the field represents*
rather than how it is stored.

Usage example::

    from askpanda_atlas_agents.common.storage.schema_annotations import (
        JOBS_FIELD_DESCRIPTIONS,
        ALL_FIELD_DESCRIPTIONS,
        get_schema_context,
    )

    # Inject a compact schema summary into an LLM system prompt:
    system_prompt = f"You have access to a jobs database.\\n{get_schema_context()}"
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# jobs table
# ---------------------------------------------------------------------------

#: Descriptions for every non-bookkeeping column in the ``jobs`` table.
#: Keys match column names exactly.  The two ingestion-agent bookkeeping
#: columns (_queue, _fetched_utc) are described separately in
#: :data:`BOOKKEEPING_FIELD_DESCRIPTIONS`.
JOBS_FIELD_DESCRIPTIONS: dict[str, str] = {
    # --- Identity / scheduling ---
    "pandaid": "Unique integer identifier for this PanDA job (primary key).",
    "jobdefinitionid": "ID of the job definition template this job was created from.",
    "schedulerid": "Name of the scheduler or pilot factory that submitted this job.",
    "pilotid": "URL or identifier of the pilot wrapper that ran the job on the worker node.",
    "taskid": "ID of the PanDA task this job belongs to.",
    "jeditaskid": "ID of the JEDI task that generated this job (JEDI is the PanDA task management layer).",
    "reqid": "ID of the production or analysis request that originated this task.",
    "jobsetid": "ID of the job set — a logical grouping of related jobs within a task.",
    "workqueue_id": "Numeric ID of the work queue used to schedule this job.",

    # --- Timestamps ---
    "creationtime": "UTC timestamp when the job record was created in the PanDA database.",
    "modificationtime": "UTC timestamp of the most recent update to the job record.",
    "statechangetime": "UTC timestamp of the last job status transition.",
    "proddbupdatetime": "UTC timestamp of the last update propagated to the production database.",
    "starttime": "UTC timestamp when the job started executing on the worker node.",
    "endtime": "UTC timestamp when the job finished (or failed) on the worker node.",

    # --- Hosts and sites ---
    "creationhost": "Hostname of the PanDA server that created this job.",
    "modificationhost": "Hostname of the PanDA server that last modified this job.",
    "computingsite": "Name of the ATLAS computing site (queue) where the job is assigned to run.",
    "computingelement": "Name of the specific computing element (CE) at the site.",
    "nucleus": "Name of the nucleus site in the ATLAS network topology (for satellite/nucleus model).",
    "wn": "Hostname or identifier of the worker node that executed the job.",

    # --- Software and release ---
    "atlasrelease": "ATLAS software release version used by this job (e.g. 'Atlas-25.2.75').",
    "transformation": "Name of the transformation script or executable run by this job.",
    "homepackage": "Software package and version that provides the transformation.",
    "cmtconfig": "CMT/build configuration string describing the platform (e.g. 'x86_64-el9-gcc14-opt').",
    "container_name": "Name of the container image used to run the job, if any.",
    "cpu_architecture_level": "CPU instruction-set level required by this job (e.g. 'x86_64-v2').",

    # --- Classification and labels ---
    "prodserieslabel": "Production series label (e.g. 'pandatest', 'main').",
    "prodsourcelabel": "Source label indicating the job origin type (e.g. 'user', 'managed', 'ptest').",
    "produserid": "Distinguished Name (DN) or username of the user or production role that submitted the job.",
    "gshare": "Global share name indicating the scheduling priority group (e.g. 'User Analysis', 'Express').",
    "grid": "Grid or infrastructure name (e.g. 'OSG', 'WLCG').",
    "cloud": "ATLAS cloud (regional grouping of sites) where the job runs (e.g. 'US', 'DE').",
    "homecloud": "ATLAS cloud that 'owns' the task — may differ from the cloud where it actually runs.",
    "transfertype": "Data transfer mode used for input/output (e.g. 'direct', 'fax').",
    "resourcetype": "Type of compute resource requested (e.g. 'SCORE', 'MCORE', 'SCORE_HIMEM').",
    "eventservice": "Event service mode: 'ordinary' for standard jobs, or an event-streaming variant.",
    "job_label": "High-level label for the job type (e.g. 'user', 'panda', 'prod').",
    "category": "Job category used for scheduling (e.g. 'run', 'merge').",
    "lockedby": "Identifier of the PanDA component that currently holds a lock on this job.",
    "relocationflag": "Integer flag indicating whether the job can be relocated to a different site.",
    "jobname": "Human-readable name for the job, typically encoding the task and dataset names.",
    "ipconnectivity": "Whether the worker node requires outbound internet connectivity ('yes' / 'no').",
    "processor_type": "Processor type required (e.g. 'CPU', 'GPU').",

    # --- Priority ---
    "assignedpriority": "Priority value assigned when the job was created.",
    "currentpriority": "Current scheduling priority (may be dynamically adjusted).",
    "priorityrange": "String representation of the priority band this job falls into.",
    "jobsetrange": "String representation of the job-set ID range this job falls into.",

    # --- Attempts ---
    "attemptnr": "Attempt number for this job — 1 for the first attempt, higher for retries.",
    "maxattempt": "Maximum number of attempts allowed before the job is declared failed.",
    "failedattempt": "Number of failed attempts so far.",

    # --- Status ---
    "jobstatus": (
        "Current job lifecycle status: 'defined', 'waiting', 'sent',"
        " 'starting', 'running', 'holding', 'merging', 'finished',"
        " 'failed', 'cancelled', or 'closed'."
    ),
    "jobsubstatus": "Optional sub-status providing more detail within the main status.",
    "commandtopilot": "Command or signal sent to the pilot (e.g. 'tobekilled').",
    "transexitcode": "Exit code returned by the job transformation script.",

    # --- Error codes and diagnostics ---
    "piloterrorcode": "Numeric error code from the pilot wrapper (0 = no error).",
    "piloterrordiag": "Human-readable diagnostic message from the pilot.",
    "exeerrorcode": "Numeric error code from the payload executable (0 = no error).",
    "exeerrordiag": "Human-readable diagnostic message from the payload executable.",
    "superrorcode": "Numeric error code from the superstructure / job dispatcher (0 = no error).",
    "superrordiag": "Human-readable diagnostic from the superstructure.",
    "ddmerrorcode": "Numeric error code from the DDM (Distributed Data Management) system (0 = no error).",
    "ddmerrordiag": "Human-readable diagnostic from DDM.",
    "brokerageerrorcode": "Numeric error code from the PanDA brokerage (site selection) step (0 = no error).",
    "brokerageerrordiag": "Human-readable diagnostic from the brokerage.",
    "jobdispatchererrorcode": "Numeric error code from the job dispatcher (0 = no error).",
    "jobdispatchererrordiag": "Human-readable diagnostic from the job dispatcher.",
    "taskbuffererrorcode": "Numeric error code from the task buffer layer (0 = no error).",
    "taskbuffererrordiag": "Human-readable diagnostic from the task buffer.",
    "errorinfo": "Consolidated error information string, may contain multiple error sources.",
    "error_desc": "Short human-readable summary of the primary error reason.",
    "transformerrordiag": "Diagnostic message specific to a transformation-level error.",

    # --- Data blocks and storage ---
    "proddblock": "Name of the input dataset or data block consumed by this job.",
    "dispatchdblock": "Name of the dispatch data block used to stage input files to the site.",
    "destinationdblock": "Name of the output dataset or data block where results are written.",
    "destinationse": "Storage element (SE) where output files are written.",
    "sourcesite": "Site from which input data is read.",
    "destinationsite": "Site to which output data is written.",

    # --- Resource requests ---
    "maxcpucount": "Maximum CPU time requested in seconds.",
    "maxcpuunit": "Unit for maxcpucount (typically 'HS06sPerEvent' or seconds).",
    "maxdiskcount": "Maximum scratch disk space requested.",
    "maxdiskunit": "Unit for maxdiskcount (e.g. 'MB', 'GB').",
    "minramcount": "Minimum RAM required.",
    "minramunit": "Unit for minramcount (e.g. 'MB').",
    "corecount": "Number of CPU cores requested.",
    "actualcorecount": "Actual number of CPU cores used during execution.",
    "meancorecount": "Mean core count across retries (stored as string; may be null).",
    "maxwalltime": "Maximum wall-clock time allowed for the job in seconds.",

    # --- CPU and efficiency metrics ---
    "cpuconsumptiontime": "Total CPU time consumed by the job in seconds.",
    "cpuconsumptionunit": "Unit for cpuconsumptiontime (typically 'sec').",
    "cpuconversion": "CPU conversion factor relating consumed CPU to HS06 units (may be null).",
    "cpuefficiency": "CPU efficiency: ratio of used CPU time to wall-clock time × core count (0.0–1.0+).",
    "hs06": "HS06 benchmark score of the worker node — a standard HEP CPU performance unit.",
    "hs06sec": "Total HS06-normalised CPU seconds consumed (may be null).",

    # --- Memory metrics (all in kB unless noted) ---
    "maxrss": "Maximum resident set size (RSS) in kB — peak physical memory used.",
    "maxvmem": "Maximum virtual memory size in kB.",
    "maxswap": "Maximum swap space used in kB.",
    "maxpss": "Maximum proportional set size (PSS) in kB — memory accounting for shared pages.",
    "avgrss": "Average RSS in kB over the job lifetime.",
    "avgvmem": "Average virtual memory in kB over the job lifetime.",
    "avgswap": "Average swap usage in kB over the job lifetime.",
    "avgpss": "Average PSS in kB over the job lifetime.",
    "maxpssgbpercore": "Peak PSS in GB divided by the number of cores — memory per core.",

    # --- I/O metrics ---
    "totrchar": "Total characters read from storage (may be null).",
    "totwchar": "Total characters written to storage (may be null).",
    "totrbytes": "Total bytes read from storage (may be null).",
    "totwbytes": "Total bytes written to storage (may be null).",
    "raterchar": "Read rate in characters per second (may be null).",
    "ratewchar": "Write rate in characters per second (may be null).",
    "raterbytes": "Read throughput in bytes per second (may be null).",
    "ratewbytes": "Write throughput in bytes per second (may be null).",
    "diskio": "Total disk I/O in kB.",
    "memoryleak": "Detected memory leak indicator (may be null).",
    "memoryleakx2": "Secondary memory leak indicator (may be null).",

    # --- Event and file counts ---
    "nevents": "Number of events processed by this job.",
    "ninputdatafiles": "Number of input data files consumed.",
    "inputfiletype": "Type/format of input files (e.g. 'DAOD_PHYS', 'HITS').",
    "inputfileproject": "ATLAS project name of the input dataset (e.g. 'mc23_13p6TeV').",
    "inputfilebytes": "Total size of input files in bytes.",
    "noutputdatafiles": "Number of output data files produced.",
    "outputfilebytes": "Total size of output files in bytes.",
    "outputfiletype": "Type/format of output files.",

    # --- Duration helpers (computed by BigPanda) ---
    "durationsec": "Wall-clock execution time in seconds (endtime − starttime).",
    "durationmin": "Wall-clock execution time in minutes (rounded).",
    "duration": "Wall-clock execution time as a human-readable string (e.g. '1:23:45').",
    "waittimesec": "Time in seconds the job spent waiting in the queue before starting.",
    "waittime": "Queue wait time as a human-readable string.",

    # --- Pilot version ---
    "pilotversion": "Version string of the PanDA pilot software that ran the job.",

    # --- Carbon footprint ---
    "gco2_regional": "Estimated CO₂ equivalent emissions in grams using the regional electricity carbon intensity (may be null).",
    "gco2_global": "Estimated CO₂ equivalent emissions in grams using a global average carbon intensity (may be null).",

    # --- Miscellaneous ---
    "jobmetrics": "Free-form string of additional job metrics reported by the pilot.",
    "jobinfo": "Additional metadata about the job as a free-form string.",
    "consumer": "Identifier of a consuming process or service associated with this job (may be null).",
}

# ---------------------------------------------------------------------------
# selectionsummary table
# ---------------------------------------------------------------------------

#: Descriptions for columns in the ``selectionsummary`` table.
SELECTION_SUMMARY_FIELD_DESCRIPTIONS: dict[str, str] = {
    "field": "Name of the facet field being summarised (e.g. 'jobstatus', 'cloud', 'gshare').",
    "list_json": "JSON array of {kname, kvalue} objects listing each distinct value and its job count for this facet.",
    "stats_json": "JSON object containing aggregate statistics for this facet (e.g. {'sum': 9928}).",
}

# ---------------------------------------------------------------------------
# errors_by_count table
# ---------------------------------------------------------------------------

#: Descriptions for columns in the ``errors_by_count`` table.
ERRORS_BY_COUNT_FIELD_DESCRIPTIONS: dict[str, str] = {
    "error": "Error category name (e.g. 'pilot', 'exe', 'ddm', 'brokerage').",
    "codename": "Symbolic name for the specific error code within the category.",
    "codeval": "Numeric error code value.",
    "diag": "Short diagnostic string associated with this error code.",
    "error_desc_text": "Human-readable description of what this error means.",
    "example_pandaid": "PanDA job ID of a representative job that has this error — useful for looking up details.",
    "count": "Number of jobs in the current snapshot that have this error.",
    "pandalist_json": "JSON array of pandaid values for all jobs with this error in the current snapshot.",
}

# ---------------------------------------------------------------------------
# Ingestion-agent bookkeeping columns (shared across tables)
# ---------------------------------------------------------------------------

#: Descriptions for the two bookkeeping columns added by the ingestion agent
#: to every table (except ``snapshots``).
BOOKKEEPING_FIELD_DESCRIPTIONS: dict[str, str] = {
    "_queue": "Name of the BigPanda computing-site queue this row was fetched from (e.g. 'SWT2_CPB', 'BNL').",
    "_fetched_utc": "UTC timestamp of the ingestion cycle that inserted or last updated this row.",
}

# ---------------------------------------------------------------------------
# Combined view
# ---------------------------------------------------------------------------

#: All field descriptions across all tables, keyed by column name.
#: Where the same column name appears in multiple tables (e.g. ``_queue``),
#: the bookkeeping description takes precedence.
ALL_FIELD_DESCRIPTIONS: dict[str, str] = {
    **JOBS_FIELD_DESCRIPTIONS,
    **SELECTION_SUMMARY_FIELD_DESCRIPTIONS,
    **ERRORS_BY_COUNT_FIELD_DESCRIPTIONS,
    **BOOKKEEPING_FIELD_DESCRIPTIONS,
}

#: Per-table mapping: table name → dict of column descriptions.
TABLE_FIELD_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "jobs": {
        **JOBS_FIELD_DESCRIPTIONS,
        **BOOKKEEPING_FIELD_DESCRIPTIONS,
    },
    "selectionsummary": {
        **SELECTION_SUMMARY_FIELD_DESCRIPTIONS,
        **BOOKKEEPING_FIELD_DESCRIPTIONS,
    },
    "errors_by_count": {
        **ERRORS_BY_COUNT_FIELD_DESCRIPTIONS,
        **BOOKKEEPING_FIELD_DESCRIPTIONS,
    },
}


def get_schema_context(tables: list[str] | None = None) -> str:
    """Return a compact schema summary suitable for inclusion in an LLM prompt.

    The output lists each table with its columns, types, and one-line
    descriptions — giving the model enough context to write correct SQL
    queries against the ingestion database.

    Args:
        tables: List of table names to include.  Defaults to all three data
            tables (``jobs``, ``selectionsummary``, ``errors_by_count``).

    Returns:
        A multi-line string describing the schema.

    Example::

        >>> print(get_schema_context(["jobs"]))
        Table: jobs
          pandaid          BIGINT    Unique integer identifier for this PanDA job (primary key).
          jobstatus        VARCHAR   Current job lifecycle status: ...
          ...
    """
    from askpanda_atlas_agents.common.storage.schema import JOBS_DDL  # noqa: F401

    if tables is None:
        tables = ["jobs", "selectionsummary", "errors_by_count"]

    # Minimal type hints — avoids importing duckdb just for context generation.
    _COLUMN_TYPES: dict[str, str] = {
        "pandaid": "BIGINT", "jobdefinitionid": "BIGINT", "taskid": "BIGINT",
        "jeditaskid": "BIGINT", "reqid": "BIGINT", "jobsetid": "BIGINT",
        "workqueue_id": "INTEGER", "relocationflag": "INTEGER",
        "assignedpriority": "INTEGER", "currentpriority": "INTEGER",
        "attemptnr": "INTEGER", "maxattempt": "INTEGER", "failedattempt": "INTEGER",
        "piloterrorcode": "INTEGER", "exeerrorcode": "INTEGER",
        "superrorcode": "INTEGER", "ddmerrorcode": "INTEGER",
        "brokerageerrorcode": "INTEGER", "jobdispatchererrorcode": "INTEGER",
        "taskbuffererrorcode": "INTEGER", "maxcpucount": "INTEGER",
        "maxdiskcount": "INTEGER", "minramcount": "INTEGER", "corecount": "INTEGER",
        "actualcorecount": "INTEGER", "maxwalltime": "INTEGER", "hs06": "INTEGER",
        "nevents": "INTEGER", "ninputdatafiles": "INTEGER",
        "noutputdatafiles": "INTEGER", "durationmin": "INTEGER",
        "maxrss": "BIGINT", "maxvmem": "BIGINT", "maxswap": "BIGINT",
        "maxpss": "BIGINT", "avgrss": "BIGINT", "avgvmem": "BIGINT",
        "avgswap": "BIGINT", "avgpss": "BIGINT", "inputfilebytes": "BIGINT",
        "outputfilebytes": "BIGINT", "diskio": "BIGINT",
        "cpuconsumptiontime": "DOUBLE", "cpuefficiency": "DOUBLE",
        "maxpssgbpercore": "DOUBLE", "durationsec": "DOUBLE",
        "waittimesec": "DOUBLE",
        "creationtime": "TIMESTAMP", "modificationtime": "TIMESTAMP",
        "statechangetime": "TIMESTAMP", "proddbupdatetime": "TIMESTAMP",
        "starttime": "TIMESTAMP", "endtime": "TIMESTAMP",
        "_fetched_utc": "TIMESTAMP",
        "id": "INTEGER", "codeval": "INTEGER", "count": "INTEGER",
        "example_pandaid": "BIGINT",
        "list_json": "JSON", "stats_json": "JSON", "pandalist_json": "JSON",
    }

    lines: list[str] = []
    for table in tables:
        descriptions = TABLE_FIELD_DESCRIPTIONS.get(table, {})
        lines.append(f"Table: {table}")
        for col, desc in descriptions.items():
            col_type = _COLUMN_TYPES.get(col, "VARCHAR")
            lines.append(f"  {col:<30} {col_type:<10}  {desc}")
        lines.append("")

    return "\n".join(lines)
