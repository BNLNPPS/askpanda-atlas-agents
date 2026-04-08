"""GitHub documentation syncer for periodic ingestion of repository documentation.

This module periodically polls one or more GitHub repositories, downloads
changed ``.md`` / ``.rst`` documentation files, normalises them for RAG
ingestion, and writes the results to a local directory that the
:class:`~bamboo_mcp_services.agents.document_monitor_agent.agent.DocumentMonitorAgent`
can watch.

On each call to :meth:`GithubDocSyncer.run_cycle` the syncer checks whether
the configured refresh interval has elapsed.  If it has, it iterates over the
list of configured repositories and calls :func:`.github_markdown_sync.sync_repo`
for each one.  A failure in one repository is logged and recorded but does
**not** abort the remaining repositories — all repos are always attempted.

The syncer does **not** interact with DuckDB or ChromaDB.  It is a file writer
only; downstream ingestion is the responsibility of a separate agent.

Typical usage
-------------
The :class:`GithubDocSyncer` is driven by the
:class:`~bamboo_mcp_services.agents.github_doc_sync_agent.agent.GithubDocSyncAgent`'s
``_tick_impl`` method::

    syncer = GithubDocSyncer(
        repos=repo_configs,
        refresh_interval_s=3600,
    )
    # In each tick:
    syncer.run_cycle()
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

from bamboo_mcp_services.agents.github_doc_sync_agent.github_markdown_sync import (
    RepoConfig,
    sync_repo,
)

logger = logging.getLogger(__name__)


class GithubDocSyncer:
    """Drives periodic GitHub documentation sync across a list of repositories.

    Each call to :meth:`run_cycle` is cheap: it is a no-op unless
    ``refresh_interval_s`` seconds have elapsed since the last attempt.  When
    the interval has elapsed, every configured repository is synced in turn.
    Per-repository state (last commit SHA, files downloaded) is persisted on
    disk by :func:`.github_markdown_sync.sync_repo` — no additional state is
    kept here beyond the timing gate and health counters.

    A failure for one repository is caught, logged, and recorded in
    :attr:`last_error_repo` / :attr:`last_error_msg`, but never propagates:
    the remaining repositories are always processed.

    Attributes:
        repos: Ordered list of repository configurations to sync.
        refresh_interval_s: Minimum seconds between sync cycles.
        last_sync_utc: UTC timestamp of the most recent completed cycle, or
            ``None`` if no cycle has run yet in this session.
        last_repos_synced: Number of repositories for which ``sync_repo`` was
            called (successfully or not) during the most recent cycle.
        last_files_downloaded_total: Total files downloaded across all repos
            during the most recent cycle, based on the counts reported by
            ``sync_repo`` via the on-disk state files.
        last_error_repo: Name of the last repository that raised an exception,
            or ``None`` if the most recent cycle completed without errors.
        last_error_msg: Exception message from the last failing repository,
            or ``None``.
    """

    def __init__(
        self,
        repos: List[RepoConfig],
        refresh_interval_s: int = 3600,
    ) -> None:
        """Initialise the syncer.

        Args:
            repos: List of :class:`.RepoConfig` instances describing the
                repositories to sync.  An empty list is valid — ``run_cycle``
                will simply be a no-op after the interval check.
            refresh_interval_s: Minimum seconds between successive sync
                cycles.  Defaults to 3600 (1 hour).
        """
        self.repos = repos
        self.refresh_interval_s = refresh_interval_s

        self._last_attempt: float = 0.0  # monotonic time of last run_cycle attempt

        # Exposed for health reporting
        self.last_sync_utc: Optional[datetime] = None
        self.last_repos_synced: int = 0
        self.last_files_downloaded_total: int = 0
        self.last_error_repo: Optional[str] = None
        self.last_error_msg: Optional[str] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run_cycle(self) -> bool:
        """Run one sync cycle if the refresh interval has elapsed.

        Iterates over all configured repositories and calls
        :func:`~.github_markdown_sync.sync_repo` for each one.  Per-repo
        failures are caught, logged, and recorded but do not abort the
        remaining repositories.

        Returns:
            ``True`` if a sync cycle was executed (interval had elapsed),
            ``False`` if the cycle was skipped because the interval has not
            yet elapsed.
        """
        now = time.monotonic()
        if now - self._last_attempt < self.refresh_interval_s:
            return False

        # Record the attempt time before I/O so a failure does not cause a
        # tight retry loop.
        self._last_attempt = now

        if not self.repos:
            logger.debug("github_doc_syncer: no repositories configured, nothing to sync")
            self.last_sync_utc = datetime.now(timezone.utc)
            self.last_repos_synced = 0
            self.last_files_downloaded_total = 0
            return True

        logger.info(
            "github_doc_syncer: starting sync cycle for %d repo(s)", len(self.repos)
        )

        repos_attempted = 0
        error_repo: Optional[str] = None
        error_msg: Optional[str] = None

        for cfg in self.repos:
            repos_attempted += 1
            try:
                sync_repo(cfg)
            except Exception as exc:
                error_repo = cfg.name
                error_msg = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "github_doc_syncer: sync failed for '%s': %s",
                    cfg.name,
                    error_msg,
                )

        self.last_sync_utc = datetime.now(timezone.utc)
        self.last_repos_synced = repos_attempted
        self.last_error_repo = error_repo
        self.last_error_msg = error_msg

        logger.info(
            "github_doc_syncer: cycle complete — %d repo(s) attempted, last_error_repo=%s",
            repos_attempted,
            error_repo or "none",
        )
        return True
