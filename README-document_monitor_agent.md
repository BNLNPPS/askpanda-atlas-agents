# document_monitor_agent

A production-oriented agent that watches a directory for new or changed documents, extracts and chunks text, computes deterministic chunk IDs, embeds chunks, and stores vectors and metadata in a local ChromaDB collection.

---

## What it does

- Monitors a directory (including all subdirectories) for file changes via polling.
- Extracts text from `.pdf`, `.docx`, `.txt`, and `.md` files.
- Splits text into overlapping character chunks.
- Generates deterministic chunk IDs (stable across re-ingestion).
- Embeds chunks using a pluggable embedder (default: `sentence-transformers`).
- Stores vectors and metadata in ChromaDB (persistent local backend).
- Maintains a JSON checkpoint store to prevent re-processing unchanged files.
- Replaces stale vectors when file content changes.

---

## Design guarantees

### Deterministic IDs

Chunk IDs are derived from `absolute_file_path + chunk_index`, hashed with SHA-256, and prefixed with `doc:`. This ensures stable IDs across re-ingestion and replace-in-place behaviour when content changes.

### Replace-on-change strategy

When a file's content hash changes:

1. Previous chunk IDs (stored in checkpoint) are deleted from ChromaDB.
2. New chunks and embeddings are computed.
3. New vectors are inserted under the same stable ID scheme.
4. Checkpoint is updated.

This prevents stale vectors from being retrieved by RAG and reduces hallucination risk.

---

## Installation & setup

Follow these steps in order. The `bamboo-document-monitor` command will not be available until all steps are complete.

### Step 1 — Install Miniforge

Miniforge is the recommended conda distribution. Do **not** use `brew install conda` — it installs a bare-bones version that won't set up your shell correctly.

```bash
brew install --cask miniforge
conda init zsh   # or 'conda init bash' if you use bash
```

Restart your terminal after running `conda init`. Alternatively, download the installer directly from [github.com/conda-forge/miniforge](https://github.com/conda-forge/miniforge).

### Step 2 — Create the conda environment

> Use Python 3.12 or earlier. Python 3.13+ is not yet reliably supported by ML libraries such as PyTorch and sentence-transformers.

**Apple Silicon:**
```bash
conda create -n bamboo-mcp-services python=3.12 -y
conda activate bamboo-mcp-services
conda install -c conda-forge -c pytorch pytorch cpuonly -y
```

**Intel macOS:**
```bash
conda create -n bamboo-mcp-services python=3.12 -y
conda activate bamboo-mcp-services
conda install -c pytorch -c conda-forge pytorch -y
```

PyTorch is installed via conda because it provides pre-compiled binaries tested for your platform, avoiding ABI and architecture issues. The remaining packages are installed with pip because they are not well-maintained on conda channels, but install cleanly once PyTorch is in place.

### Step 3 — Install remaining dependencies

```bash
pip install sentence-transformers langchain langchain-community langchain-huggingface chromadb pdfminer.six python-docx
pip install -r requirements.txt
```

### Step 4 — Install the package

This registers the `bamboo-document-monitor` CLI command:

```bash
pip install -e .
```

---

## Running the agent

### One-shot (recommended for cron and pipeline use)

Process all new or changed files once and exit:

```bash
bamboo-document-monitor --dir ./documents --chroma-dir .chromadb --once
```

This is the recommended mode when running after `bamboo-github-sync` in a
pipeline or cron job — it processes whatever was downloaded and exits cleanly.

### Long-running daemon

Poll continuously, picking up new files as they arrive:

```bash
bamboo-document-monitor --dir ./documents --poll-interval 10 --chroma-dir .chromadb
```

Stop with Ctrl-C or SIGTERM.

### Via module

```bash
python -m bamboo_mcp_services.agents.document_monitor_agent.cli --dir ./documents --once
```

> **First run on a new machine:** the agent loads the embedding model from local cache and avoids network calls on startup. This means the model must be downloaded at least once first. On a fresh machine, trigger the download by running with `HF_HUB_OFFLINE=0`:
> ```bash
> HF_HUB_OFFLINE=0 bamboo-document-monitor --dir ./documents --poll-interval 10 --chroma-dir .chromadb
> ```
> Subsequent runs will use the cached model automatically and do not need the flag.

> **Always use an absolute path for `--chroma-dir`** to avoid the database being written to a different location depending on the working directory:
> ```bash
> bamboo-document-monitor --dir /abs/path/to/docs --chroma-dir /abs/path/to/.chromadb
> ```

---

## Re-ingestion

Re-ingestion is required when:

- You change `--chunk-size` or `--chunk-overlap` (existing chunks remain at the old size until wiped).
- The ChromaDB index becomes corrupted or out of sync with the SQLite metadata.
- You want to start fresh after adding or removing documents.

To re-ingest cleanly:

```bash
# 1. Wipe the vector store and checkpoint
rm -rf .chromadb .document_monitor/checkpoints.json

# 2. Re-run the agent — it will process all files from scratch
bamboo-document-monitor --dir /abs/path/to/docs --chroma-dir /abs/path/to/.chromadb
```

---

## Starting a new session

Once set up, you only need to activate the environment at the start of each session:

```bash
conda activate bamboo-mcp-services
```

To verify everything is in order:

```bash
conda info
python --version
```

If a virtualenv is currently active, deactivate it first — only one environment manager should be active at a time:

```bash
deactivate
conda activate bamboo-mcp-services
```

---

## Configuration options

> **Chunk size guidance:** the default of 3000 characters is a good balance for technical
> documentation with long class or function definitions. If your documents are short
> (e.g. individual wiki pages), a smaller value like 1000–1500 may give better retrieval
> precision. Changing this setting requires a full re-ingestion — see [Re-ingestion](#re-ingestion).

| Option | Default | Description |
|---|---|---|
| `--dir` | *(required)* | Directory to monitor (all subdirectories are included) |
| `--poll-interval` | `10` | Poll interval in seconds (daemon mode only) |
| `--chroma-dir` | `.chromadb` | ChromaDB persistence directory |
| `--checkpoint-file` | `.document_monitor/checkpoints.json` | JSON checkpoint path |
| `--chunk-size` | `3000` | Characters per chunk |
| `--chunk-overlap` | `300` | Overlap between chunks |
| `--once` | off | Run a single poll cycle then exit |

---

## Checkpoint format

```json
{
  "processed": {
    "/abs/path/to/file.pdf": {
      "content_hash": "sha256...",
      "processed_ts": "2026-03-12T12:34:56Z",
      "chunks": 5,
      "chunk_ids": ["doc:...", "doc:..."]
    }
  }
}
```

---

## CI and testing

Use a dummy embedder in tests to avoid model downloads:

```python
class DummyEmbedder:
    def encode(self, texts, show_progress_bar=False):
        return [[0.0] * 8 for _ in texts]
```
