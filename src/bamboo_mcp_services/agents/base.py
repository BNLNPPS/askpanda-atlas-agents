"""Base agent infrastructure for lifecycle management.

This module provides the core Agent interface and state management that all
AskPanDA-ATLAS agents must implement.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional


class AgentState(str, Enum):
    """Enumeration of agent lifecycle states.

    Attributes:
        NEW: Agent has been created but not started.
        STARTING: Agent is in the process of starting.
        RUNNING: Agent is running and can accept tick() calls.
        STOPPING: Agent is in the process of stopping.
        STOPPED: Agent has stopped cleanly.
        FAILED: Agent has encountered an unrecoverable error.
    """
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class HealthReport:
    """Agent health and status report.

    Attributes:
        name: Agent name.
        state: Current agent state.
        ok: Whether the agent is healthy (running or stopped cleanly).
        last_tick_utc: Timestamp of the last tick() call.
        last_success_utc: Timestamp of the last successful tick() completion.
        last_error_utc: Timestamp of the last error.
        error: String description of the last error, if any.
        details: Agent-specific health details.
    """
    name: str
    state: AgentState
    ok: bool
    last_tick_utc: Optional[datetime] = None
    last_success_utc: Optional[datetime] = None
    last_error_utc: Optional[datetime] = None
    error: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert health report to a JSON-serializable dictionary.

        Returns:
            Dictionary representation with ISO 8601 formatted timestamps.
        """
        def _dt(v: Optional[datetime]) -> Optional[str]:
            return v.astimezone(timezone.utc).isoformat() if v else None
        return {
            "name": self.name,
            "state": self.state.value,
            "ok": self.ok,
            "last_tick_utc": _dt(self.last_tick_utc),
            "last_success_utc": _dt(self.last_success_utc),
            "last_error_utc": _dt(self.last_error_utc),
            "error": self.error,
            "details": dict(self.details),
        }


class Agent(ABC):
    """Abstract base class for all AskPanDA-ATLAS agents.

    Agents follow a minimal lifecycle interface with four core methods:
    - start(): Initialize resources and enter running state.
    - tick(): Execute one scheduled unit of work.
    - health(): Return lightweight health/status information.
    - stop(): Gracefully release resources and shut down.

    Subclasses must implement the protected _start_impl(), _tick_impl(),
    and _stop_impl() methods to define agent-specific behavior.
    """

    def __init__(self, name: str) -> None:
        """Initialize the agent.

        Args:
            name: Unique identifier for this agent instance.
        """
        self._name = name
        self._state: AgentState = AgentState.NEW
        self._last_tick_utc: Optional[datetime] = None
        self._last_success_utc: Optional[datetime] = None
        self._last_error_utc: Optional[datetime] = None
        self._last_error: Optional[str] = None

    @property
    def name(self) -> str:
        """Return the agent's name.

        Returns:
            Agent name string.
        """
        return self._name

    @property
    def state(self) -> AgentState:
        """Return the agent's current state.

        Returns:
            Current AgentState value.
        """
        return self._state

    def start(self) -> None:
        """Initialize resources and transition to RUNNING state.

        This method is idempotent - calling it multiple times on a running
        agent has no effect. If initialization fails, the agent transitions
        to FAILED state.

        Raises:
            Exception: If the _start_impl() implementation raises an exception.
        """
        if self._state in (AgentState.RUNNING, AgentState.STARTING):
            return
        self._state = AgentState.STARTING
        try:
            self._start_impl()
            self._state = AgentState.RUNNING
            self._last_error = None
        except Exception as exc:
            self._mark_failed(exc)
            raise

    def tick(self) -> None:
        """Execute one scheduled unit of work.

        This method can only be called when the agent is in RUNNING state.
        It updates timestamps and error state based on execution results.

        Raises:
            RuntimeError: If the agent is not in RUNNING state.
            Exception: If the _tick_impl() implementation raises an exception.
        """
        if self._state != AgentState.RUNNING:
            raise RuntimeError(f"Agent '{self._name}' is not running (state={self._state.value}); cannot tick().")
        self._last_tick_utc = datetime.now(timezone.utc)
        try:
            self._tick_impl()
            self._last_success_utc = datetime.now(timezone.utc)
            self._last_error = None
        except Exception as exc:
            self._mark_failed(exc)
            raise

    def stop(self) -> None:
        """Release resources and transition to STOPPED state.

        This method is idempotent - calling it multiple times on a stopped
        agent has no effect. Even if _stop_impl() raises an exception,
        the agent will transition to STOPPED state.

        Raises:
            Exception: If the _stop_impl() implementation raises an exception.
        """
        if self._state in (AgentState.STOPPING, AgentState.STOPPED):
            return
        self._state = AgentState.STOPPING
        try:
            self._stop_impl()
            self._state = AgentState.STOPPED
        except Exception as exc:
            self._last_error_utc = datetime.now(timezone.utc)
            self._last_error = f"stop() failed: {exc!r}"
            self._state = AgentState.STOPPED
            raise

    def health(self) -> HealthReport:
        """Return current health and status information.

        Returns:
            HealthReport containing agent state, timestamps, errors, and
            agent-specific details from _health_details().
        """
        ok = self._state in (AgentState.RUNNING, AgentState.STOPPED)
        if self._state == AgentState.FAILED:
            ok = False
        return HealthReport(
            name=self._name,
            state=self._state,
            ok=ok,
            last_tick_utc=self._last_tick_utc,
            last_success_utc=self._last_success_utc,
            last_error_utc=self._last_error_utc,
            error=self._last_error,
            details=self._health_details(),
        )

    @abstractmethod
    def _start_impl(self) -> None:
        """Initialize agent-specific resources.

        Subclasses must implement this method to perform initialization
        tasks such as opening connections, loading configuration, etc.

        Raises:
            Exception: If initialization fails.
        """
        pass

    @abstractmethod
    def _tick_impl(self) -> None:
        """Execute one unit of agent-specific work.

        Subclasses must implement this method to perform periodic tasks
        such as polling, processing data, syncing state, etc.

        Raises:
            Exception: If the work fails.
        """
        pass

    @abstractmethod
    def _stop_impl(self) -> None:
        """Release agent-specific resources.

        Subclasses must implement this method to perform cleanup tasks
        such as closing connections, flushing buffers, etc.

        Raises:
            Exception: If cleanup fails.
        """
        pass

    def _health_details(self) -> Mapping[str, Any]:
        """Return agent-specific health details.

        Subclasses may override this method to include custom metrics,
        counters, or status information in health reports.

        Returns:
            Dictionary of agent-specific health information.
        """
        return {}

    def _mark_failed(self, exc: Exception) -> None:
        """Mark the agent as failed and record error details.

        Args:
            exc: The exception that caused the failure.
        """
        self._state = AgentState.FAILED
        self._last_error_utc = datetime.now(timezone.utc)
        self._last_error = f"{type(exc).__name__}: {exc}"
