"""GitHub documentation sync agent for periodic ingestion of repository docs.

This module provides :class:`GithubDocSyncAgent`, a lightweight agent that
periodically polls one or more GitHub repositories, downloads changed
documentation files, and writes normalised Markdown to a local directory
that the
:class:`~bamboo_mcp_services.agents.document_monitor_agent.agent.DocumentMonitorAgent`
can watch for RAG ingestion into ChromaDB.

The agent follows the standard lifecycle defined in
:class:`~bamboo_mcp_services.agents.base.Agent`:

- :meth:`~GithubDocSyncAgent.start` validates the configuration and
  initialises the syncer.
- :meth:`~GithubDocSyncAgent.tick` delegates to the syncer, which is a
  no-op unless ``refresh_interval_s`` seconds have elapsed.
- :meth:`~GithubDocSyncAgent.stop` releases the syncer reference (no
  persistent connections to close).

This agent is intentionally a **file writer only** — it does not interact
with DuckDB or ChromaDB.  Downstream ingestion is the responsibility of
the document-monitor agent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any, List, Mapping, Optional

from bamboo_mcp_services.agents.base import Agent
from bamboo_mcp_services.agents.github_doc_sync_agent.github_doc_syncer import (
    GithubDocSyncer,
)
from bamboo_mcp_services.agents.github_doc_sync_agent.github_markdown_sync import (
    RepoConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class GithubDocSyncConfig:
    """Configuration for :class:`GithubDocSyncAgent`.

    Attributes:
        repos: Ordered list of repository configurations to sync.  Each
            :class:`.RepoConfig` specifies the GitHub repository name,
            destination directories, file patterns, and normalisation
            settings.
        refresh_interval_s: Minimum seconds between successive sync cycles.
            Defaults to 3600 (1 hour).
        tick_interval_s: Seconds to sleep between ``tick()`` calls in the
            run loop.  Defaults to 60.0.
    """

    repos: List[RepoConfig] = field(default_factory=list)
    refresh_interval_s: int = 3600
    tick_interval_s: float = 60.0


class GithubDocSyncAgent(Agent):
    """Agent that periodically syncs documentation from GitHub repositories.

    On each tick the agent delegates to
    :class:`~bamboo_mcp_services.agents.github_doc_sync_agent.github_doc_syncer.GithubDocSyncer`,
    which contacts the GitHub API only when the configured refresh interval
    has elapsed.  Within that check the syncer compares each repository's
    latest commit SHA against the previously cached value and skips
    repositories whose content has not changed.

    Files are written to the ``destination`` and ``normalized_destination``
    directories specified in each :class:`.RepoConfig`.  The
    :class:`~bamboo_mcp_services.agents.document_monitor_agent.agent.DocumentMonitorAgent`
    should be pointed at the normalised output directory to complete the
    RAG ingestion pipeline.
    """

    def __init__(
        self,
        name: str = "github-doc-sync-agent",
        config: Optional[GithubDocSyncConfig] = None,
    ) -> None:
        """Initialise the GitHub documentation sync agent.

        Args:
            name: Agent name (default: ``'github-doc-sync-agent'``).
            config: Agent configuration.  Must be supplied; the parameter is
                typed ``Optional`` only to match the base-class pattern used
                across all agents.

        Raises:
            ValueError: If *config* is ``None``.
        """
        super().__init__(name=name)
        if config is None:
            raise ValueError("GithubDocSyncConfig must be provided")
        self.config = config
        self._syncer: Optional[GithubDocSyncer] = None

    # ------------------------------------------------------------------
    # Agent lifecycle hooks
    # ------------------------------------------------------------------

    def _start_impl(self) -> None:
        """Initialise the syncer."""
        self._syncer = GithubDocSyncer(
            repos=self.config.repos,
            refresh_interval_s=self.config.refresh_interval_s,
        )
        logger.info(
            "GithubDocSyncAgent started: repos=%d  refresh_interval=%ds",
            len(self.config.repos),
            self.config.refresh_interval_s,
        )

    def _tick_impl(self) -> None:
        """Run one syncer cycle (no-op if the interval has not elapsed)."""
        assert self._syncer is not None  # guaranteed by _start_impl
        self._syncer.run_cycle()

    def _stop_impl(self) -> None:
        """Release the syncer reference."""
        self._syncer = None
        logger.info("GithubDocSyncAgent stopped")

    # ------------------------------------------------------------------
    # Health reporting
    # ------------------------------------------------------------------

    def _health_details(self) -> Mapping[str, Any]:
        """Return GitHub-sync-specific health metrics for the health report.

        Returns:
            Dictionary with keys:

            * ``repo_count`` — number of configured repositories.
            * ``repo_names`` — list of repository names (``owner/repo``).
            * ``refresh_interval_s`` — configured interval in seconds.
            * ``last_sync_utc`` — ISO 8601 timestamp of the most recent
              completed sync cycle, or ``None``.
            * ``last_repos_synced`` — number of repos attempted in the
              most recent cycle.
            * ``last_error_repo`` — name of the last repo that raised an
              exception, or ``None``.
            * ``last_error_msg`` — exception message from the last failing
              repo, or ``None``.
        """
        last_sync = None
        last_repos_synced = 0
        last_error_repo = None
        last_error_msg = None

        if self._syncer is not None:
            if self._syncer.last_sync_utc is not None:
                last_sync = (
                    self._syncer.last_sync_utc
                    .astimezone(timezone.utc)
                    .isoformat()
                )
            last_repos_synced = self._syncer.last_repos_synced
            last_error_repo = self._syncer.last_error_repo
            last_error_msg = self._syncer.last_error_msg

        return {
            "repo_count": len(self.config.repos),
            "repo_names": [r.name for r in self.config.repos],
            "refresh_interval_s": self.config.refresh_interval_s,
            "last_sync_utc": last_sync,
            "last_repos_synced": last_repos_synced,
            "last_error_repo": last_error_repo,
            "last_error_msg": last_error_msg,
        }
