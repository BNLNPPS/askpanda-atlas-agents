"""Document monitor agent implementation.

This agent watches a directory (polling by default), processes new documents,
splits them into chunks, computes deterministic IDs, embeds them using a pluggable
embedder, and stores vectors+metadata into ChromaDB.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

from bamboo_mcp_services.agents.base import Agent
from .utils import (
    extract_text_from_file,
    chunk_text,
    content_hash,
    deterministic_chunk_id,
    CheckpointStore,
)
from .storage import ChromaWrapper

LOG = logging.getLogger(__name__)


class DocumentMonitorAgent(Agent):
    """Agent that monitors a directory and ingests new files into ChromaDB.

    The agent lifecycle integrates with the project's Base Agent: it must
    implement start/tick/stop hooks via the base class (the names used here
    match a thin adapter to your existing base).

    Args:
        name: Agent name.
        directory: Directory to monitor (create if missing).
        poll_interval_sec: Polling interval in seconds.
        chunk_size: Character chunk size (default: 3000).
        chunk_overlap: Chunk overlap in characters (default: 300).
        checkpoint_file: Path to JSON checkpoint file.
        chroma_dir: Directory for ChromaDB persistence.
        embedder: Object with an .encode(list[str], show_progress_bar=False) -> np.ndarray interface.
                  If None, a default local sentence-transformers embedder will be created lazily.
    """

    def __init__(
        self,
        name: str,
        directory: str,
        poll_interval_sec: int = 10,
        chunk_size: int = 3000,
        chunk_overlap: int = 300,
        checkpoint_file: str = ".document_monitor/checkpoints.json",
        chroma_dir: str = ".chromadb",
        embedder: Optional[object] = None,
        embedding_model_name: str = "all-MiniLM-L6-v2",
    ) -> None:
        super().__init__(name=name)
        self.directory = Path(directory)
        self.poll_interval_sec = poll_interval_sec
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.checkpoint = CheckpointStore(checkpoint_file)
        self.chroma = ChromaWrapper(persist_directory=chroma_dir)
        self.collection = self.chroma.get_or_create_collection(name)
        self._last_processed_file: Optional[str] = None
        self._last_error: Optional[str] = None
        self._embedder = embedder
        self._embedding_model_name = embedding_model_name

    # ---------------------- embedder ---------------------------------------
    def _ensure_embedder(self) -> None:
        """Ensure an embedder is available; instantiate default if not provided."""
        if self._embedder is not None:
            return
        try:
            # Lazy import to avoid hard requirement in test/mocked environments
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._embedder = SentenceTransformer(self._embedding_model_name)
        except Exception as exc:
            LOG.exception("Failed to create default embedder: %s", exc)
            raise

    # ---------------------- lifecycle hooks --------------------------------
    def _start_impl(self) -> None:
        """Start hook called by base Agent.start().

        Creates the monitored directory if missing and performs any one-time init.
        """
        try:
            from importlib.metadata import version
            _version = version("bamboo-mcp-services")
        except Exception:
            _version = "unknown"
        LOG.info("document-monitor-agent v%s starting. Monitoring: %s", _version, self.directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _is_file_changed(self, path_str: str, text: str) -> tuple[bool, str, list]:
        """Check whether a file needs ingesting by comparing its content hash to the checkpoint.

        Returns:
            Tuple of (changed, content_hash, prev_chunk_ids).
        """
        h = content_hash(text)
        prev = self.checkpoint._data.get("processed", {}).get(path_str)
        prev_hash = prev.get("content_hash") if prev else None
        prev_chunk_ids = prev.get("chunk_ids", []) if prev else []

        if prev_hash == h:
            return False, h, prev_chunk_ids

        if prev_hash is None:
            LOG.info("New file detected: %s", path_str)
        else:
            LOG.info("File changed, re-ingesting: %s", path_str)

        return True, h, prev_chunk_ids

    def _ingest_file(self, path_str: str, text: str, h: str, prev_chunk_ids: list) -> None:
        """Chunk, embed, and store a single file into ChromaDB, then update the checkpoint."""
        chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
        ts = datetime.now(timezone.utc).isoformat()

        if not chunks:
            LOG.debug("No chunks generated for %s; recording empty checkpoint.", path_str)
            self.checkpoint.mark_processed(path_str, {"content_hash": h, "processed_ts": ts, "chunks": 0, "chunk_ids": []})
            self._last_processed_file = path_str
            self._last_error = None
            return

        ids: List[str] = [deterministic_chunk_id(path_str, "", i) for i in range(len(chunks))]
        metadatas: List[Dict] = [
            {"source_file": path_str, "chunk_index": i, "content_hash": h, "processed_ts": ts}
            for i in range(len(chunks))
        ]

        if prev_chunk_ids:
            try:
                self.chroma.delete_documents_by_ids(self.collection, prev_chunk_ids)
                LOG.debug("Deleted %d previous chunk ids for %s", len(prev_chunk_ids), path_str)
            except Exception:
                LOG.exception("Failed to delete previous chunk ids for %s (best-effort)", path_str)

        self._ensure_embedder()
        raw_embeddings = self._embedder.encode(chunks, show_progress_bar=False)
        try:
            embeddings = raw_embeddings.tolist()  # type: ignore[attr-defined]
        except Exception:
            embeddings = [list(map(float, v)) for v in raw_embeddings]

        self.chroma.add_documents(self.collection, ids=ids, documents=chunks, metadatas=metadatas, embeddings=embeddings)
        self.chroma.persist()
        self.checkpoint.mark_processed(path_str, {"content_hash": h, "processed_ts": ts, "chunks": len(chunks), "chunk_ids": ids})

        self._last_processed_file = path_str
        self._last_error = None
        LOG.info("Processed file %s -> chunks=%d", path_str, len(chunks))

    def _tick_impl(self) -> None:
        """Perform one polling cycle: detect new/changed files, ingest chunks into ChromaDB.

        Lists files in the monitored directory, skips unchanged files, and ingests
        any that are new or modified. Logs a summary at the end of each cycle.
        Errors are caught per-file so one bad file does not abort the whole cycle.
        """
        try:
            files = sorted([p for p in self.directory.iterdir() if p.is_file()])
        except Exception as exc:
            LOG.exception("Failed listing directory %s: %s", self.directory, exc)
            self._last_error = str(exc)
            time.sleep(self.poll_interval_sec)
            return

        processed_count = 0
        skipped_count = 0

        for p in files:
            path_str = str(p.resolve())
            try:
                text = extract_text_from_file(path_str)
                if not text:
                    LOG.debug("No text extracted from %s; skipping.", path_str)
                    continue

                changed, h, prev_chunk_ids = self._is_file_changed(path_str, text)
                if not changed:
                    skipped_count += 1
                    continue

                self._ingest_file(path_str, text, h, prev_chunk_ids)
                processed_count += 1

            except Exception as exc:
                LOG.exception("Error processing file %s: %s", path_str, exc)
                self._last_error = str(exc)

        if processed_count > 0:
            LOG.info("Poll cycle complete: %d file(s) ingested, %d unchanged. Next poll in %ds.",
                     processed_count, skipped_count, self.poll_interval_sec)
        else:
            LOG.debug("Poll cycle complete: no changes detected (%d file(s) unchanged). Next poll in %ds.",
                      skipped_count, self.poll_interval_sec)

        time.sleep(self.poll_interval_sec)

    def _stop_impl(self) -> None:
        """Stop hook called by base Agent.stop().

        Persist chroma and perform cleanup.
        """
        LOG.info("document_monitor_agent stopping. Persisting Chroma.")
        try:
            self.chroma.persist()
        except Exception:
            LOG.exception("Failed persisting chroma on stop")

    def _health_details(self) -> Dict:
        """Return agent-specific health details for monitoring dashboards.

        Returns:
            Dictionary with last processed file, last error, checkpoint location and collection name.
        """
        return {
            "last_processed_file": self._last_processed_file,
            "last_error": self._last_error,
            "checkpoint_file": str(self.checkpoint.path),
            "chroma_collection": getattr(self.collection, "name", "<unknown>"),
        }
