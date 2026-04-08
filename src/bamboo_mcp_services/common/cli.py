"""Shared CLI utilities for bamboo-mcp-services agents.

This module provides helpers that are used by every agent CLI entry point
to ensure consistent startup behaviour.
"""
from __future__ import annotations

import importlib.metadata
import logging
import sys

#: The distribution name as declared in ``pyproject.toml``.
_PACKAGE_NAME = "bamboo-mcp-services"


def log_startup_banner(logger: logging.Logger, prog: str) -> None:
    """Emit a startup log line containing the program name, package version, and Python version.

    The version is resolved at runtime from the installed package metadata so
    it always reflects the version declared in ``pyproject.toml`` without
    requiring a hardcoded constant in source.

    Produces a log line of the form::

        bamboo-cric  version=0.1.0  python=3.12.3

    Args:
        logger: Logger to emit the banner on (typically the calling module's
            ``logging.getLogger(__name__)``).
        prog: Short program name, e.g. ``"bamboo-cric"``.  Used as the leading
            field in the banner so each agent is identifiable in aggregated logs.
    """
    try:
        version = importlib.metadata.version(_PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"

    logger.info(
        "%s  version=%s  python=%d.%d.%d",
        prog,
        version,
        sys.version_info.major,
        sys.version_info.minor,
        sys.version_info.micro,
    )
