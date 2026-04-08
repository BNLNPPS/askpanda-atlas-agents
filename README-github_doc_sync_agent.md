# github-doc-sync-agent

A periodic documentation sync agent that downloads changed `.md` and `.rst`
files from one or more GitHub repositories, normalises them for RAG ingestion,
and writes the results to a local directory.

The agent is a **file writer only** — it does not interact with DuckDB or
ChromaDB directly.  Its output directory is intended to be watched by the
[`document-monitor-agent`](./README-document_monitor_agent.md), which handles
chunking, embedding, and ChromaDB insertion.

---

## What it does

- Polls one or more GitHub repositories on a configurable interval (default:
  every hour).
- For each repository, fetches the latest commit SHA via the GitHub REST API
  and compares it against a cached value stored in `.sync_state.json`.  If the
  SHA is unchanged the repository is skipped with no further API calls.
- When a repository has new commits, fetches the full file tree, filters it
  with configurable include/exclude glob patterns, and downloads only the
  matching files.
- Optionally normalises each file for RAG by prepending a YAML frontmatter
  block (containing `source_repo`, `source_path`, `source_type`,
  `source_commit_sha`) and converting RST headings, code blocks, admonitions,
  and links to Markdown equivalents.
- A failure for one repository is logged and recorded in health details but
  does **not** abort the remaining repositories — all repos are always
  attempted in each cycle.
- Supports `within_hours`: if the repository's latest commit is older than the
  configured threshold it is skipped entirely, avoiding noise from dormant
  repos.

---

## Output structure

For each configured repository the agent creates two directories under the
configured `destination`:

```
data/
  repo-name/
    raw/
      docs/guide.md          ← verbatim downloaded files
      docs/install.rst
      ...
    normalized/
      docs/guide.md          ← RAG-ready files with frontmatter
      docs/install.md        ← RST converted to Markdown
      ...
    .sync_state.json         ← cached commit SHA, last sync time, file count
```

The `normalized/` directory is what the `document-monitor-agent` should be
pointed at.

### Normalised file format

Each normalised file begins with a YAML frontmatter block:

```
---
source_repo: atlas-project/panda-docs
source_path: docs/guide.md
source_type: md
source_commit_sha: a1b2c3d4e5f6...
---

# Original content follows...
```

This metadata enables traceability in RAG results and can be used to surface
source links in Bamboo responses.

---

## Integration with `document-monitor-agent`

The two agents form a pipeline:

```
github-doc-sync-agent        document-monitor-agent
─────────────────────        ──────────────────────
polls GitHub                 watches ./data/*/normalized/
downloads changed files  →   recurses into subdirectories
writes to normalized/        chunks and embeds files  →  ChromaDB
owner/repo/...               updates checkpoints
```

Neither agent needs to know about the other.  They can run as separate
long-lived daemons on independent tick intervals, or both be invoked with
`--once` in a cron pipeline.

Example cron pipeline (daily at 02:00):

```cron
0 2 * * *  bamboo-github-sync --config /path/to/repos.yaml --once
5 2 * * *  bamboo-document-monitor --dir /path/to/RAG --chroma-dir /path/to/.chromadb --once
```

---

## Installation

This project uses a conda environment.  If you have not set it up yet:

```bash
conda create -n bamboo-mcp-services python=3.12
conda activate bamboo-mcp-services
pip install -r requirements.txt
pip install -e ".[dev]"
```

On a normal working day, just activate the existing environment:

```bash
conda activate bamboo-mcp-services
pip install -e .   # pick up any dependency changes
```

The `-e` flag is required — the project uses a `src/` layout and the package
will not be importable without it.  See [CONTRIBUTING.md](./CONTRIBUTING.md)
for the full first-time setup guide including pre-commit hooks and the DuckDB
CLI.

Verify the entry point is available:

```bash
bamboo-github-sync --help
```

No additional dependencies are needed beyond those already in
`requirements.txt` — `requests` and `pyyaml` are already declared.

---

## GitHub API rate limits

The GitHub API allows **60 unauthenticated requests per hour**.  Each sync
cycle makes at least one request per repository (the commit SHA check), plus
tree and file download requests when new commits are found.  For more than a
handful of repositories, or for repositories with many changed files, you will
want to authenticate.

Set the `GITHUB_TOKEN` environment variable to a personal access token (classic
or fine-grained, with `Contents: read` scope):

```bash
export GITHUB_TOKEN=ghp_your_token_here
bamboo-github-sync --config repos.yaml --once
```

With a token the limit rises to **5,000 requests per hour**.  Private
repositories also require a token.

The agent logs a confirmation at startup when `GITHUB_TOKEN` is detected.

---

## Configuration

The agent is configured via a YAML file.  The default path is:

```
src/bamboo_mcp_services/resources/config/github-doc-sync-agent.yaml
```

### Full example

```yaml
# Minimum seconds between sync cycles across all repos.
refresh_interval_s: 3600   # 1 hour

# Seconds between tick() calls in the run loop.
tick_interval_s: 60.0

repos:
  - name: PanDAWMS/panda-docs
    destination: ./data/panda-docs/raw
    normalized_destination: ./data/panda-docs/normalized
    within_hours: 168          # skip if latest commit is older than 1 week
    branch: main
    include_patterns:
      - "*.md"
      - "*.rst"
    exclude_patterns:
      - "drafts/*"
      - "archive/*"
    normalize_for_rag: true

  - name: PanDAWMS/harvester
    destination: ./data/harvester/raw
    normalized_destination: ./data/harvester/normalized
    within_hours: 48
    branch: master
    include_patterns:
      - "docs/*.md"
      - "docs/*.rst"
    normalize_for_rag: true
```

### Top-level options

| Key | Default | Description |
|---|---|---|
| `refresh_interval_s` | `3600` | Minimum seconds between sync cycles. The gate is shared across all repos — when the interval elapses, all repos are checked in sequence. |
| `tick_interval_s` | `60.0` | Seconds between `tick()` calls in the run loop. Most ticks are instant no-ops when the interval has not elapsed. |

### Per-repository options

| Key | Required | Description |
|---|---|---|
| `name` | ✅ | Repository identifier in `owner/repo` format — the part of the GitHub URL after `github.com/`.  For example, `https://github.com/PanDAWMS/panda-docs` becomes `name: PanDAWMS/panda-docs`. |
| `destination` | ✅ | Directory where raw downloaded files are written. Created if it does not exist. |
| `normalized_destination` | — | Directory for RAG-normalised files.  If omitted, normalisation is skipped even if `normalize_for_rag: true`. |
| `branch` | — | Branch or ref to sync.  Defaults to the repository's default branch. |
| `within_hours` | — | Skip this repository if its latest commit is older than this many hours.  Useful for dormant repos that need not be checked every cycle. |
| `include_patterns` | — | Glob patterns (e.g. `*.md`, `docs/*.rst`).  Only matching files are downloaded.  If empty, all files are included. |
| `exclude_patterns` | — | Glob patterns.  Matching files are excluded even if they match an include pattern. |
| `normalize_for_rag` | — | Prepend YAML frontmatter and convert RST to Markdown.  Requires `normalized_destination` to be set. |

---

## Running the agent

### One-shot (recommended for first use and cron)

```bash
bamboo-github-sync --config repos.yaml --once
```

Runs a single sync cycle and exits.  All configured repositories are checked.

> **Note:** On a freshly created syncer the refresh interval gate starts at
> zero, so the first tick always fires.  If you use `--once` in a cron job and
> want the interval gate to be respected across runs, use a long-lived daemon
> instead, or rely on the per-repo `.sync_state.json` commit SHA cache (which
> persists across restarts) to avoid redundant downloads.

### Long-running daemon

```bash
bamboo-github-sync --config repos.yaml
```

Loops indefinitely, calling `tick()` every `tick_interval_s` seconds.
Repositories are contacted at most once per `refresh_interval_s`.  Stop with
Ctrl-C or SIGTERM — both trigger a clean shutdown.

### All command-line options

| Option | Default | Description |
|---|---|---|
| `--config PATH`, `-c` | `src/.../github-doc-sync-agent.yaml` | Path to the YAML configuration file. |
| `--once` | off | Run a single tick then exit. |
| `--log-file PATH` | `github-doc-sync-agent.log` | Rotating log file (10 MB × 5 backups).  Pass `""` to disable file logging. |
| `--log-level LEVEL` | `INFO` | Minimum log level for console and file output.  One of `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

### First-run walkthrough

```bash
# 1. Create a minimal config:
cat > repos.yaml << 'EOF'
refresh_interval_s: 0   # fire immediately on first tick
repos:
  - name: PanDAWMS/panda-docs
    destination: ./data/panda-docs/raw
    normalized_destination: ./data/panda-docs/normalized
    include_patterns:
      - "*.md"
      - "*.rst"
    normalize_for_rag: true
EOF

# 2. Run once with debug logging to see what happens:
bamboo-github-sync --config repos.yaml --once --log-level DEBUG

# 3. Inspect the downloaded files:
find ./data/panda-docs/raw -name "*.md" | head -10
cat ./data/panda-docs/normalized/docs/something.md | head -20

# 4. Check the sync state:
cat ./data/panda-docs/raw/.sync_state.json

# 5. If everything looks good, switch to daemon mode:
bamboo-github-sync --config repos.yaml --log-file panda-docs-sync.log
```

---

## Sync state and incremental updates

Each repository stores its state in `{destination}/.sync_state.json`:

```json
{
  "last_commit_sha": "a1b2c3d4e5f6...",
  "last_sync_time": "2026-04-06T14:00:00+00:00",
  "files_downloaded": 12
}
```

On each cycle the agent fetches only the latest commit SHA (one API call per
repo).  If the SHA matches the cached value the repository is skipped entirely
— no tree fetch, no file downloads.  This makes repeated runs cheap for
repositories that change infrequently.

On a first run (no state file), or after the state file is deleted, a full sync
is performed: the entire file tree is fetched and all matching files are
downloaded.

---

## Architecture

```
GithubDocSyncAgent
├── _start_impl()        — instantiates GithubDocSyncer
├── _tick_impl()
│   └── GithubDocSyncer.run_cycle()
│       ├── interval check (skip if < refresh_interval_s since last attempt)
│       └── for each RepoConfig:
│           ├── get_latest_commit()    — GitHub API: fetch commit SHA
│           ├── within_hours check     — skip if commit is too old
│           ├── SHA unchanged?  →  skip
│           └── SHA changed?
│               ├── _get_tree()        — GitHub API: recursive blob list
│               ├── _matches_patterns() — apply include/exclude globs
│               ├── _download_file()   — raw.githubusercontent.com
│               ├── write to destination/
│               └── normalize_text()  →  write to normalized_destination/
│               └── save_state()       — update .sync_state.json
└── _stop_impl()         — releases syncer reference (no connections to close)
```

Key modules:

| Module | Purpose |
|---|---|
| `agents/github_doc_sync_agent/agent.py` | Agent lifecycle, `GithubDocSyncConfig` dataclass |
| `agents/github_doc_sync_agent/github_doc_syncer.py` | Interval gate, multi-repo loop, error isolation |
| `agents/github_doc_sync_agent/github_markdown_sync.py` | GitHub API calls, file download, normalisation, state persistence |
| `agents/github_doc_sync_agent/cli.py` | CLI entry point (`bamboo-github-sync`) |

`github_markdown_sync.py` is vendored from the standalone
[`github-documentation-sync`](https://github.com/nilsnilsson/github-documentation-sync)
project (MIT licence).  It is included directly in the package so that
`bamboo-mcp-services` has no dependency on an unpublished package.

---

## CI and testing

```bash
pytest tests/agents/github_doc_sync_agent/ -v
```

The test suite (62 tests) covers:

- **Interval gate** — first call fires, second immediate call is blocked,
  call after elapsed interval fires, gate uses `time.monotonic` not wall clock.
- **No-repo case** — empty repo list returns cleanly, health attributes
  initialise to sane values.
- **Successful cycle** — `sync_repo` called once per repo, correct config
  passed, health attributes updated, error cleared after clean cycle.
- **Failure isolation** — one failing repo does not abort others, error repo
  and message recorded, `repos_synced` count includes failed repos, no
  exception ever propagates from `run_cycle`.
- **Agent lifecycle** — `config=None` raises, `start`/`stop` idempotency,
  tick delegation, syncer reference cleared on stop.
- **Health reporting** — before first tick, after successful tick, after
  failing tick, after stop.
- **CLI** — argument parsing, missing config file, missing required repo keys,
  end-to-end `--once` run with mocked `sync_repo`, multi-repo invocation,
  `GITHUB_TOKEN` env var, `--log-file /dev/null`.

All network I/O is mocked with `unittest.mock.patch`; no GitHub API calls or
file system writes occur during testing.
