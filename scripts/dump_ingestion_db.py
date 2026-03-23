#!/usr/bin/env python3
"""Dump the contents of an ingestion-agent DuckDB database to stdout.

Usage examples:
    # Show the first 10 rows of every table in the default database:
    python scripts/dump_ingestion_db.py

    # Use a specific database file and show 25 rows per table:
    python scripts/dump_ingestion_db.py --db path/to/jobs.duckdb --limit 25

    # Show only the jobs table, one field per line (default format):
    python scripts/dump_ingestion_db.py --table jobs --queue SWT2_CPB

    # Machine-readable JSON output, pipe to jq:
    python scripts/dump_ingestion_db.py --table jobs --queue BNL --format json | jq '.pandaid'

    # Show only row counts for every table:
    python scripts/dump_ingestion_db.py --count

    # Show the schema of every table:
    python scripts/dump_ingestion_db.py --schema-only
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb is not installed.  Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)

#: Tables to dump in this order when --table is not specified.
ALL_TABLES = ["jobs", "selectionsummary", "errors_by_count", "snapshots"]

#: Default database path (relative to the repository root).
DEFAULT_DB = "jobs.duckdb"

#: Default row limit per table.
DEFAULT_LIMIT = 10


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    p = argparse.ArgumentParser(
        prog="dump_ingestion_db.py",
        description="Dump ingestion-agent DuckDB contents to stdout.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--db",
        default=DEFAULT_DB,
        metavar="PATH",
        help=f"Path to the DuckDB file (default: {DEFAULT_DB})",
    )
    p.add_argument(
        "--table", "-t",
        default=None,
        metavar="TABLE",
        help=f"Table to dump.  One of: {', '.join(ALL_TABLES)}.  "
             "Omit to dump all tables.",
    )
    p.add_argument(
        "--queue", "-q",
        default=None,
        metavar="QUEUE",
        help="Filter rows to this _queue value (ignored for the snapshots table).",
    )
    p.add_argument(
        "--limit", "-n",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"Maximum rows to show per table (default: {DEFAULT_LIMIT}). "
             "Pass 0 for no limit.",
    )
    p.add_argument(
        "--format", "-f",
        choices=["vertical", "table", "json"],
        default="vertical",
        help=(
            "Output format: 'vertical' (default) prints one field per line, "
            "skipping NULLs — readable for wide tables like jobs; "
            "'table' for a horizontal ASCII grid; "
            "'json' for newline-delimited JSON objects suitable for piping to jq."
        ),
    )
    p.add_argument(
        "--schema-only",
        action="store_true",
        help="Print the column names and types for each table instead of row data.",
    )
    p.add_argument(
        "--count",
        action="store_true",
        help="Print only the total row count for each table (ignores --limit).",
    )
    return p


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_value(v: Any) -> str:
    """Render a single cell value as a compact, display-safe string.

    Args:
        v: The cell value returned by DuckDB.  May be any Python type.

    Returns:
        A string representation of the value, truncated to 120 characters.
        ``None`` is rendered as the literal string ``"NULL"``.
    """
    if v is None:
        return "NULL"
    display = str(v)
    if len(display) > 120:
        return display[:117] + "..."
    return display


def _to_json_safe(val: Any) -> Any:
    """Convert a DuckDB result value into a JSON-serialisable Python object.

    Python's ``json.dumps`` emits bare ``NaN`` / ``Infinity`` for the
    corresponding float values.  These are JavaScript extensions that are
    not valid JSON and are rejected by strict parsers such as ``jq``.
    They must be intercepted before ``json.dumps`` sees them because they
    bypass the ``default`` callback entirely.

    Args:
        val: A cell value returned by DuckDB.

    Returns:
        The original value unchanged, or ``None`` if the value is a
        non-finite float (``nan``, ``inf``, ``-inf``).
    """
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _print_vertical(columns: list[str], rows: list[tuple], title: str) -> None:
    """Print each row as a vertical block of ``column: value`` pairs.

    NULL values are omitted to keep the output compact — for a typical PanDA
    job this reduces ~130 columns down to the ~40 that carry actual data.
    Each row is separated by a horizontal rule.

    Args:
        columns: Ordered list of column names from the query result.
        rows: List of value tuples, one per row.
        title: Human-readable label shown above the block (e.g. table name
            with queue and limit annotations).
    """
    col_width = max(len(c) for c in columns)
    rule_width = col_width + 44
    print(f"\n{'=' * rule_width}")
    print(f"  {title}")
    print(f"{'=' * rule_width}")

    for i, row in enumerate(rows, 1):
        print(f"\n  ── row {i} {'─' * (rule_width - 10)}")
        for col, val in zip(columns, row):
            if val is None:
                continue
            display = str(val)
            if len(display) > 120:
                display = display[:117] + "..."
            print(f"  {col:<{col_width}}  {display}")

    print(f"\n  {len(rows)} row(s) shown")


def _print_table(columns: list[str], rows: list[tuple], title: str) -> None:
    """Print rows as a fixed-width horizontal ASCII grid.

    Column widths are determined by the widest value in each column, capped
    at 60 characters.  This format becomes very wide for tables with many
    columns; prefer ``_print_vertical`` for human-readable output of the
    ``jobs`` table.

    Args:
        columns: Ordered list of column names.
        rows: List of value tuples, one per row.
        title: Label shown above the grid.
    """
    widths = [len(c) for c in columns]
    for row in rows:
        for i, v in enumerate(row):
            widths[i] = min(60, max(widths[i], len(_format_value(v))))

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    header = "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(columns)) + " |"

    print(f"\n{'=' * len(sep)}")
    print(f"  {title}")
    print(f"{'=' * len(sep)}")
    print(sep)
    print(header)
    print(sep)
    for row in rows:
        line = "| " + " | ".join(
            _format_value(v).ljust(widths[i]) for i, v in enumerate(row)
        ) + " |"
        print(line)
    print(sep)
    print(f"  {len(rows)} row(s) shown")


def _print_json(columns: list[str], rows: list[tuple], title: str) -> None:
    """Print rows as newline-delimited JSON (one valid JSON object per line).

    Non-finite float values (``nan``, ``inf``, ``-inf``) are mapped to
    ``null`` so the output is strict RFC 8259 JSON accepted by ``jq``.
    Columns whose names end in ``_json`` are expanded from their stored
    string representation into nested objects.

    The title header is written to *stderr* so it does not pollute the JSON
    stream on *stdout* when piping to ``jq`` or other tools.

    Args:
        columns: Ordered list of column names.
        rows: List of value tuples, one per row.
        title: Label written to stderr to identify the table.
    """
    print(f"# --- {title} ---", file=sys.stderr)
    for row in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(columns, row):
            if col.endswith("_json") and isinstance(val, str):
                try:
                    val = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
            obj[col] = _to_json_safe(val)
        print(json.dumps(obj, default=str, allow_nan=False))


def _print_schema(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    """Print column names and DuckDB types for a single table.

    Args:
        conn: An open, readable DuckDB connection.
        table: Name of the table to describe.
    """
    try:
        rows = conn.execute(f"DESCRIBE {table}").fetchall()
    except Exception as exc:
        print(f"  (could not describe {table!r}: {exc})")
        return
    print(f"\n  {table}")
    print(f"  {'column_name':<40} {'column_type'}")
    print(f"  {'-' * 40} {'-' * 20}")
    for row in rows:
        print(f"  {row[0]:<40} {row[1]}")


def _print_count(
    conn: duckdb.DuckDBPyConnection, table: str, queue: str | None
) -> None:
    """Print the total row count for a single table.

    Args:
        conn: An open, readable DuckDB connection.
        table: Name of the table to count.
        queue: If provided, count only rows where ``_queue`` equals this
            value.  Ignored for the ``snapshots`` table which has no
            ``_queue`` column.
    """
    try:
        if queue and table != "snapshots":
            result = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE _queue = ?", [queue]
            ).fetchone()
            note = f"  (filtered to _queue = {queue!r})"
        else:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            note = ""
        print(f"  {table:<30} {result[0]:>10} rows{note}")
    except Exception as exc:
        print(f"  {table:<30} ERROR: {exc}")


# ---------------------------------------------------------------------------
# Core dump logic
# ---------------------------------------------------------------------------

def _dump_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    queue: str | None,
    limit: int,
    fmt: str,
) -> None:
    """Fetch rows from one table and dispatch to the appropriate print function.

    Args:
        conn: An open, readable DuckDB connection.
        table: Name of the table to query.
        queue: Optional ``_queue`` filter value.  Ignored for ``snapshots``.
        limit: Maximum number of rows to return.  0 means no limit.
        fmt: Output format — one of ``"vertical"``, ``"table"``, ``"json"``.
    """
    params: list[Any] = []
    where = ""
    if queue and table != "snapshots":
        where = "WHERE _queue = ?"
        params.append(queue)

    limit_clause = f"LIMIT {limit}" if limit > 0 else ""

    order = ""
    if table == "jobs":
        order = "ORDER BY _fetched_utc DESC, pandaid DESC"
    elif table in ("selectionsummary", "errors_by_count"):
        order = "ORDER BY _queue, id"
    elif table == "snapshots":
        order = "ORDER BY fetched_utc DESC"

    sql = f"SELECT * FROM {table} {where} {order} {limit_clause}".strip()

    try:
        rel = conn.execute(sql, params)
        columns = [d[0] for d in rel.description]
        rows = rel.fetchall()
    except Exception as exc:
        print(f"\nERROR querying {table!r}: {exc}", file=sys.stderr)
        return

    queue_note = f" [_queue={queue!r}]" if queue and table != "snapshots" else ""
    limit_note = f" (first {limit})" if limit > 0 and len(rows) == limit else ""
    title = f"{table}{queue_note}{limit_note}"

    if fmt == "json":
        _print_json(columns, rows, title)
    elif fmt == "vertical":
        _print_vertical(columns, rows, title)
    else:
        _print_table(columns, rows, title)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parses arguments, opens the DuckDB file in read-only mode, and either
    dumps row data, prints the schema, or prints row counts depending on the
    flags provided.

    Args:
        argv: Argument list to parse.  Uses ``sys.argv[1:]`` when ``None``.

    Returns:
        Exit code — ``0`` on success, ``1`` on error.
    """
    args = build_parser().parse_args(argv)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: database file not found: {db_path}", file=sys.stderr)
        print(
            "Hint: run the ingestion agent first, or pass --db /path/to/jobs.duckdb",
            file=sys.stderr,
        )
        return 1

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        print(f"ERROR: could not open database {db_path}: {exc}", file=sys.stderr)
        return 1

    if args.table:
        if args.table not in ALL_TABLES:
            print(
                f"ERROR: unknown table {args.table!r}.  "
                f"Choose from: {', '.join(ALL_TABLES)}",
                file=sys.stderr,
            )
            return 1
        tables = [args.table]
    else:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        tables = [t for t in ALL_TABLES if t in existing]

    print(f"Database: {db_path.resolve()}", file=sys.stderr)

    if args.schema_only:
        print("\nSchema:")
        for table in tables:
            _print_schema(conn, table)
        return 0

    if args.count:
        print("\nRow counts:")
        for table in tables:
            _print_count(conn, table, args.queue)
        return 0

    for table in tables:
        _dump_table(conn, table, args.queue, args.limit, args.format)

    return 0


if __name__ == "__main__":
    sys.exit(main())
