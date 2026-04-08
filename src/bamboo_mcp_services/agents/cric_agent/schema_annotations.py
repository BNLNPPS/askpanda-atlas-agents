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

    from bamboo_mcp_services.common.storage.schema_annotations import (
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
    from bamboo_mcp_services.common.storage.schema import JOBS_DDL  # noqa: F401

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


# ===========================================================================
# CRIC queuedata table
# ===========================================================================

#: Human-readable descriptions for every column in the ``queuedata`` table
#: populated by the CRIC agent from ``cric_pandaqueues.json``.
#:
#: Keys match DuckDB column names exactly.  The top-level JSON key (queue name)
#: is stored in the column ``queue``.
#:
#: Fields whose purpose is not yet documented carry the string
#: ``"Purpose not yet documented."`` — do not invent a meaning.
#:
#: The three ``_data``-suffix fields (``coreenergy_data``, ``corepower_data``,
#: ``maxdiskio_data``) are dropped at ingestion time and therefore have no
#: entry here.
QUEUEDATA_FIELD_DESCRIPTIONS: dict[str, str] = {

    # -----------------------------------------------------------------------
    # Identity
    # -----------------------------------------------------------------------
    "queue": (
        "PanDA queue identifier — the top-level key from cric_pandaqueues.json. "
        "Aliases: name, queuename, queue_name."
    ),
    "name": (
        "Queue name string; typically identical to 'queue' and 'siteid'."
    ),
    "nickname": (
        "Short human-readable alias for the queue, often the same as 'name'."
    ),
    "siteid": (
        "ATLAS site identifier for this queue; typically matches 'name' and 'nickname'."
    ),
    "id": (
        "Internal CRIC primary key for this queue record.  Not meaningful for "
        "workload management queries."
    ),

    # -----------------------------------------------------------------------
    # Site / resource hierarchy
    # -----------------------------------------------------------------------
    "site": (
        "PanDA site grouping that this queue belongs to.  A single PanDA site may "
        "cover multiple queues (e.g. 'GreatLakesT2' covers AGLT2, AGLT2_MCORE, …)."
    ),
    "panda_site": (
        "PanDA site name associated with this queue — equivalent to 'site'."
    ),
    "panda_resource": (
        "PanDA resource name; typically matches the queue name or atlas_site."
    ),
    "atlas_site": (
        "The ATLAS site name associated with the queue."
    ),
    "rc_site": (
        "Resource-centre site name as known to WLCG accounting (e.g. 'AGLT2')."
    ),
    "rc": (
        "Full resource-centre identifier used in WLCG accounting (e.g. 'US-AGLT2')."
    ),
    "rc_country": (
        "Country of the resource centre (full country name; mirrors 'country')."
    ),
    "parent": (
        "Name of the parent queue or virtual queue from which this queue inherits "
        "settings.  Empty for top-level queues."
    ),
    "gocname": (
        "Official Grid Operations Centre (GOCDB) site name.  Identifies the site as "
        "registered in the WLCG/EGI Grid Information System."
    ),
    "gstat": (
        "GStat grid-monitoring site name; usually identical to 'gocname'."
    ),

    # -----------------------------------------------------------------------
    # Operational state
    # -----------------------------------------------------------------------
    "status": (
        "Operational status of the queue as seen by PanDA brokerage. "
        "Canonical values: 'online', 'offline', 'test', 'brokeroff'."
    ),
    "state": (
        "Operational state of this queue entry in CRIC (e.g. 'ACTIVE', 'INACTIVE'). "
        "Distinct from 'status': state is the CRIC-level record state; "
        "status is the PanDA brokerage state."
    ),
    "state_comment": (
        "Free-text comment explaining the current state or the most recent state "
        "transition."
    ),
    "state_update": (
        "UTC timestamp of the most recent state change for this queue in CRIC."
    ),
    "last_modified": (
        "UTC timestamp of the most recent modification to this queue record in CRIC."
    ),
    "site_state": (
        "Operational state of the PanDA site grouping this queue belongs to "
        "(e.g. 'ACTIVE')."
    ),
    "rc_site_state": (
        "Operational state of the resource-centre site in WLCG/CRIC "
        "(e.g. 'ACTIVE')."
    ),

    # -----------------------------------------------------------------------
    # Geography / organisation
    # -----------------------------------------------------------------------
    "country": (
        "Full country name where the queue is located "
        "(e.g. 'United States', 'Germany').  Not a short code."
    ),
    "rc_country": (
        "Country of the resource centre (full country name; mirrors 'country')."
    ),
    "cloud": (
        "Logical PanDA cloud/region code for workload management "
        "(e.g. 'US', 'DE', 'CERN').  Not a full country name."
    ),
    "region": (
        "Geographic or network region code (e.g. 'US-MIDW-MISO')."
    ),
    "countrygroup": (
        "Country group selector used by the pilot for getJob requests, corresponding "
        "to the country group that the user who created the job belongs to."
    ),
    "vo_name": (
        "Virtual Organisation name; for ATLAS queues this is always 'atlas'."
    ),
    "tier": (
        "WLCG tier label for this site (e.g. 'T1', 'T2', 'T2D')."
    ),
    "tier_level": (
        "Numeric WLCG tier level (1 = Tier-1, 2 = Tier-2, 3 = Tier-3)."
    ),

    # -----------------------------------------------------------------------
    # Compute resource
    # -----------------------------------------------------------------------
    "resource_type": (
        "Type of compute resource (e.g. 'GRID', 'CLOUD', 'HPC')."
    ),
    "type": (
        "Queue dispatch type.  'unified' means the queue handles multiple job types "
        "via unified dispatch; other values include 'production' and 'analysis'."
    ),
    "corecount": (
        "Number of CPU cores allocated per job."
    ),
    "corepower": (
        "Benchmark result in HS06 indicating how powerful the queue is per core."
    ),
    "coreenergy": (
        "Average power consumption per core in Watts."
    ),
    "nodes": (
        "Number of worker nodes available or registered at this queue/site."
    ),
    "availablecpu": (
        "Total capacity of the site available to ATLAS, measured in HS06 units.  "
        "May reflect the full ATLAS share, or include opportunistic resources."
    ),
    "pledgedcpu": (
        "CPU pledged to ATLAS under the WLCG Memorandum of Understanding, in HS06.  "
        "0 means the value is not used in brokerage decisions.  "
        "-1 means undefined or not reported."
    ),

    # -----------------------------------------------------------------------
    # Job resource limits
    # -----------------------------------------------------------------------
    "maxrss": (
        "Maximum RSS (resident set size) memory a job is allowed to use, in MB."
    ),
    "meanrss": (
        "Mean expected RSS memory consumption per job in MB; used by the brokerage."
    ),
    "minrss": (
        "Minimum RSS memory threshold for job eligibility on this queue, in MB."
    ),
    "maxtime": (
        "Maximum wall-clock time (seconds) allowed for a job on this queue."
    ),
    "mintime": (
        "Minimum wall-clock time (seconds) a job must request to be eligible for "
        "this queue."
    ),
    "maxwdir": (
        "Maximum working directory size (MB) a job may use on local scratch."
    ),
    "maxinputsize": (
        "Maximum total size (MB) of input files a job is permitted to stage in."
    ),
    "maxdiskio": (
        "Maximum average disk I/O limit per core, in kB/s."
    ),
    "timefloor": (
        "Minimum job wall-clock time in minutes required by the brokerage to "
        "consider sending work to this queue."
    ),
    "depthboost": (
        "Multiplier applied to the base queue depth or pilot submission limit to "
        "increase concurrency.  1 = normal; higher values allow proportionally more "
        "concurrent jobs or pilots."
    ),
    "transferringlimit": (
        "Maximum number of concurrent file transfers to/from this site's storage."
    ),
    "queuehours": (
        "Purpose not yet documented."
    ),

    # -----------------------------------------------------------------------
    # Software environment
    # -----------------------------------------------------------------------
    "pilot_manager": (
        "Pilot submission framework managing this queue "
        "(e.g. 'Harvester', 'AutoPilot')."
    ),
    "pilot_version": (
        "Required or default ATLAS pilot version for jobs running on this queue."
    ),
    "python_version": (
        "Python major version required on the worker nodes (e.g. '3')."
    ),
    "releases": (
        "ATLAS software releases supported at this site.  "
        "['AUTO'] means any release is accepted via CVMFS auto-discovery."
    ),
    "validatedreleases": (
        "Whether ATLAS validated releases are explicitly enumerated for this site.  "
        "'True' typically means releases are auto-detected via CVMFS."
    ),
    "is_cvmfs": (
        "Whether CVMFS (CernVM File System) is available on the worker nodes."
    ),
    "environ": (
        "Space-separated list of environment variable assignments to apply before "
        "running jobs (e.g. 'VAR1=value1 VAR2=value2')."
    ),
    "appdir": (
        "Application directory where the PanDA pilot can find source code for "
        "applications."
    ),

    # -----------------------------------------------------------------------
    # Container
    # -----------------------------------------------------------------------
    "container_type": (
        "Specifies which components use containerisation and which container runtime "
        "is used.  Examples: 'singularity:pilot' (pilot runs inside Singularity), "
        "'apptainer:wrapper;container:middleware' (wrapper uses Apptainer, middleware "
        "uses a container).  Two directives may be combined with ';'."
    ),
    "container_options": (
        "Extra command-line options passed to the container runtime (Apptainer/"
        "Singularity) when launching jobs."
    ),

    # -----------------------------------------------------------------------
    # Data management
    # -----------------------------------------------------------------------
    "acopytools": (
        "Advanced copy-tools configuration.  A JSON object mapping transfer activity "
        "types (pr, pw, read_lan, write_lan) to lists of supported copy-tool names "
        "(e.g. 'rucio', 'xrdcp').  "
        "DuckDB access example: json_extract(acopytools, '$.pr[0]')"
    ),
    "astorages": (
        "JSON object mapping transfer activity types to lists of RSE (Rucio Storage "
        "Element) names.  "
        "DuckDB access example: json_extract(astorages, '$.pr[0]')"
    ),
    "copytools": (
        "JSON object mapping copy-tool names to configuration objects with a 'setup' "
        "key (shell script path or empty string).  "
        "DuckDB access example: json_extract(copytools, '$.rucio.setup')"
    ),
    "allow_lan": (
        "Whether LAN (local area network) stage-in transfers are allowed within the "
        "grid site."
    ),
    "allow_wan": (
        "Whether WAN (wide area network) stage-in transfers are allowed between "
        "grid sites."
    ),
    "direct_access_lan": (
        "If true, direct data access over LAN is permitted (no local copy required)."
    ),
    "direct_access_wan": (
        "If true, direct data access over WAN is permitted (no local copy required)."
    ),
    "use_pcache": (
        "Whether the pilot cache (pcache) is enabled for pre-staging input files."
    ),
    "zip_time_gap": (
        "Minimum interval (seconds) between successive zip-file staging requests "
        "from this site."
    ),
    "cachedse": (
        "Deprecated field, formerly used for PD2P data placement.  Ignore."
    ),

    # -----------------------------------------------------------------------
    # Harvester / dispatch
    # -----------------------------------------------------------------------
    "harvester": (
        "Name of the Harvester service instance associated with this queue "
        "(e.g. 'CERN_central_A').  Null if the queue is not managed by Harvester."
    ),
    "harvester_template": (
        "Harvester configuration template name encoding the workflow type and "
        "submission mode (e.g. 'ng.production.push', 'analysis.push').  "
        "Empty or null if no specific template is configured."
    ),
    "workflow": (
        "Pilot submission workflow mode (e.g. 'pull_ups' = Harvester unified "
        "push-pull)."
    ),
    "params": (
        "JSON object of Harvester / unified-dispatch configuration overrides for "
        "this queue (e.g. queue limits, pilot RSS grace, resource type caps).  "
        "DuckDB access example: json_extract(params, '$.unified_dispatch')"
    ),
    "uconfig": (
        "JSON object of per-queue unified configuration overrides; typically "
        "contains resource_type_limits.  "
        "DuckDB access example: json_extract(uconfig, '$.resource_type_limits')"
    ),
    "localqueue": (
        "Local batch system queue name submitted to at the CE.  "
        "Empty if job submission is managed entirely by Harvester."
    ),
    "special_par": (
        "Site-specific special parameters passed to the pilot or job wrapper."
    ),

    # -----------------------------------------------------------------------
    # Computing elements
    # -----------------------------------------------------------------------
    "queues": (
        "JSON array of Computing Element (CE) endpoint records for this queue.  "
        "Each element describes one CE with fields: ce_endpoint, ce_flavour "
        "(e.g. 'HTCONDOR-CE'), ce_jobmanager, ce_queue_name, ce_state, ce_status, "
        "ce_queue_maxwctime, and others.  "
        "DuckDB unnest example: "
        "SELECT q.queue, unnest(from_json(q.queues, '[{...}]')) AS ce FROM queuedata q"
    ),

    # -----------------------------------------------------------------------
    # Testing / monitoring
    # -----------------------------------------------------------------------
    "hc_param": (
        "HammerCloud (HC) testing mode for this queue.  "
        "'False' = HC testing disabled; "
        "'OnlyTest' = used only for HC test jobs; "
        "'AutoExclusion' = HC may automatically exclude the queue if tests fail."
    ),
    "hc_suite": (
        "JSON array of HammerCloud test suite codes to run for this queue "
        "(e.g. ['AFT', 'PFT']).  Empty array means no suites configured.  "
        "DuckDB membership test: json(hc_suite)::STRING LIKE '%\"AFT\"%'"
    ),
    "capability": (
        "A string indicating the capability level of the queue."
    ),
    "probe": (
        "Probe job configuration or probe queue name used for site validation.  "
        "Null if not configured."
    ),

    # -----------------------------------------------------------------------
    # Miscellaneous
    # -----------------------------------------------------------------------
    "is_default": (
        "Whether this queue is the default queue for its PanDA site."
    ),
    "is_virtual": (
        "Whether this is a virtual (aggregation) queue rather than a real "
        "submission target."
    ),
    "wnconnectivity": (
        "Worker-node network connectivity descriptor "
        "(e.g. 'full#IPv4' = full outbound IPv4 connectivity)."
    ),
    "fairsharepolicy": (
        "Comma-separated list of job-type fair-share rules, each of the form "
        "'type=<jobtype>:<percentage>%' (e.g. 'type=evgen:0%,type=any:100%').  "
        "Controls which job types are allowed to run and at what share."
    ),
    "catchall": (
        "General-purpose field for miscellaneous site-specific information."
    ),
    "comment": (
        "Free-text comment about the queue."
    ),
    "description": (
        "Free-text human-readable description of the queue."
    ),
    "jobseed": (
        "Purpose not yet documented."
    ),
    "gshare": (
        "Global share name used by PanDA scheduling (e.g. 'ATLAS', 'Express').  "
        "Controls priority group assignment for brokerage."
    ),
}


def get_queuedata_schema_context() -> str:
    """Return a compact schema summary for the ``queuedata`` table for use in LLM prompts.

    Lists every column with its DuckDB type and a one-line description, giving
    the model enough context to write correct SQL queries against ``cric.db``.

    Returns:
        A multi-line string describing the ``queuedata`` table schema.

    Example::

        >>> print(get_queuedata_schema_context())
        Table: queuedata
          queue                          VARCHAR     PanDA queue identifier — the top-level key ...
          status                         VARCHAR     Operational status of the queue as seen by ...
          ...
    """
    # DuckDB column types for queuedata.
    # Columns not listed here default to VARCHAR (TEXT).
    _QUEUEDATA_TYPES: dict[str, str] = {
        "id": "BIGINT",
        "corecount": "BIGINT",
        "coreenergy": "DOUBLE",
        "corepower": "DOUBLE",
        "availablecpu": "BIGINT",
        "pledgedcpu": "BIGINT",
        "nodes": "BIGINT",
        "maxrss": "BIGINT",
        "meanrss": "BIGINT",
        "minrss": "BIGINT",
        "maxtime": "BIGINT",
        "mintime": "BIGINT",
        "maxwdir": "BIGINT",
        "maxinputsize": "BIGINT",
        "maxdiskio": "BIGINT",
        "tier_level": "BIGINT",
        "depthboost": "BIGINT",
        "transferringlimit": "BIGINT",
        "timefloor": "BIGINT",
        "queuehours": "BIGINT",
        "zip_time_gap": "BIGINT",
        "direct_access_lan": "BOOLEAN",
        "direct_access_wan": "BOOLEAN",
        "is_cvmfs": "BOOLEAN",
        "is_default": "BOOLEAN",
        "is_virtual": "BOOLEAN",
        "use_pcache": "BOOLEAN",
        "acopytools": "JSON",
        "astorages": "JSON",
        "copytools": "JSON",
        "hc_suite": "JSON",
        "params": "JSON",
        "uconfig": "JSON",
        "queues": "JSON",
    }

    lines: list[str] = ["Table: queuedata"]
    for col, desc in QUEUEDATA_FIELD_DESCRIPTIONS.items():
        col_type = _QUEUEDATA_TYPES.get(col, "VARCHAR")
        # Truncate to the first sentence for compactness.
        short_desc = desc.split("\n")[0].split(".")[0] + "."
        lines.append(f"  {col:<30}  {col_type:<10}  {short_desc}")
    lines.append("")
    return "\n".join(lines)
