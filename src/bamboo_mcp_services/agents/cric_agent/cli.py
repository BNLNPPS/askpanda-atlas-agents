"""Command-line interface for the CRIC agent."""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import signal
import sys
import time
import yaml
from typing import Optional, Sequence

from bamboo_mcp_services.agents.cric_agent.agent import CricAgent, CricAgentConfig
from bamboo_mcp_services.common.cli import log_startup_banner

logger = logging.getLogger(__name__)

#: Log format shared by the console handler and the rotating file handler.
_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Default log file path (relative to CWD; override with --log-file).
_DEFAULT_LOG_FILE = "cric-agent.log"

#: Rotating file handler limits — 10 MB per file, keep 5 backups.
_LOG_MAX_BYTES = 10 * 1024 * 1024
_LOG_BACKUP_COUNT = 5

#: Default YAML config path.
_DEFAULT_CONFIG = "src/bamboo_mcp_services/resources/config/cric-agent.yaml"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    p = argparse.ArgumentParser(
        prog="bamboo-cric",
        description="Periodic CRIC queuedata ingestion agent.",
    )
    p.add_argument(
        "--config", "-c",
        default=_DEFAULT_CONFIG,
        metavar="PATH",
        help="Path to YAML configuration file (default: %(default)s)",
    )
    p.add_argument(
        "--data",
        required=True,
        metavar="PATH",
        help="Path to the DuckDB output file (e.g. cric.db). Required.",
    )
    p.add_argument(
        "--log-file",
        default=_DEFAULT_LOG_FILE,
        metavar="PATH",
        help=(
            "Path to the rotating log file (default: %(default)s). "
            "Pass an empty string or /dev/null to disable file logging."
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Minimum log level for both console and file output (default: %(default)s)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Run a single tick then exit (useful for cron / one-shot invocations).",
    )
    return p


def _configure_logging(log_file: str, log_level: str) -> None:
    """Set up the root logger with a console handler and an optional rotating file handler.

    Both handlers share the same format and level.  Third-party libraries that
    produce noisy output at INFO are suppressed to WARNING.

    Args:
        log_file: Path for the rotating log file.  Pass ``""`` or
            ``"/dev/null"`` to skip file logging.
        log_level: String log level, e.g. ``"INFO"``.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(formatter)
    console.setLevel(level)
    root.addHandler(console)

    if log_file and log_file != os.devnull:
        try:
            fh = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=_LOG_MAX_BYTES,
                backupCount=_LOG_BACKUP_COUNT,
                encoding="utf-8",
            )
            fh.setFormatter(formatter)
            fh.setLevel(level)
            root.addHandler(fh)
            logging.getLogger(__name__).info(
                "Logging to file: %s (max %d MB, %d backups)",
                os.path.abspath(log_file),
                _LOG_MAX_BYTES // (1024 * 1024),
                _LOG_BACKUP_COUNT,
            )
        except OSError as exc:
            logging.getLogger(__name__).warning(
                "Could not open log file %r: %s — file logging disabled.", log_file, exc
            )

    for _noisy in ("urllib3", "requests", "httpx", "httpcore"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)


def _make_signal_handler(agent: CricAgent):
    """Return a SIGTERM handler that stops the agent gracefully.

    Args:
        agent: The running agent instance to stop on signal.

    Returns:
        Signal handler callable.
    """
    def _handler(signum, frame):
        logger.info("Signal %d received — stopping agent.", signum)
        try:
            agent.stop()
        except Exception:
            logger.exception("Error while stopping agent on signal.")
        sys.exit(0)
    return _handler


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for the CRIC agent.

    Parses arguments, configures logging, builds and starts the agent, then
    either runs a single tick (``--once``) or loops indefinitely calling
    ``tick()`` at the configured interval until interrupted.

    Args:
        argv: Command-line arguments.  If ``None``, uses ``sys.argv``.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    args = build_parser().parse_args(argv)
    _configure_logging(args.log_file, args.log_level)
    log_startup_banner(logger, "bamboo-cric")
    logger.info("Starting (config=%s  data=%s)", args.config, args.data)

    try:
        with open(args.config, "r") as fh:
            cfg = yaml.safe_load(fh)
    except OSError as exc:
        logger.error("Cannot read config file %r: %s", args.config, exc)
        return 1

    if cfg is None:
        cfg = {}

    cric_path = cfg.get("cric_path")
    if not cric_path:
        logger.error(
            "Config file %r must contain a non-empty 'cric_path' key.", args.config
        )
        return 1

    refresh_interval_s = int(cfg.get("refresh_interval_s", 600))
    tick_interval_s = float(cfg.get("tick_interval_s", 60.0))

    logger.info(
        "Configuration: cric_path=%s  duckdb_path=%s  "
        "refresh_interval=%ds  tick_interval=%.1fs",
        cric_path,
        args.data,
        refresh_interval_s,
        tick_interval_s,
    )

    config = CricAgentConfig(
        cric_path=cric_path,
        duckdb_path=args.data,
        refresh_interval_s=refresh_interval_s,
        tick_interval_s=tick_interval_s,
    )
    agent = CricAgent(config=config)
    signal.signal(signal.SIGTERM, _make_signal_handler(agent))

    try:
        agent.start()
        logger.info("Agent started (state=%s)", agent.state.value)

        if args.once:
            logger.info("--once flag set: running a single tick then exiting.")
            agent.tick()
            h = agent.health()
            logger.info(
                "Tick complete. last_row_count=%s  last_hash=%s",
                h.details.get("last_row_count"),
                h.details.get("last_hash"),
            )
        else:
            logger.info(
                "Entering run loop (tick_interval=%.1fs). "
                "Press Ctrl-C or send SIGTERM to stop.",
                tick_interval_s,
            )
            while True:
                agent.tick()
                time.sleep(tick_interval_s)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down.")
    except Exception:
        logger.exception("Unhandled exception in agent run loop.")
        return 1
    finally:
        try:
            agent.stop()
            logger.info("Agent stopped cleanly (state=%s)", agent.state.value)
        except Exception:
            logger.exception("Error while stopping agent.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
