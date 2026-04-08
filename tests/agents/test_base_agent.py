# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

import pytest

from bamboo_mcp_services.agents.base import Agent, AgentState


class DummyAgent(Agent):
    """A controllable Agent for unit tests."""

    def __init__(self, name: str = "dummy-agent") -> None:
        super().__init__(name=name)
        self.started = 0
        self.stopped = 0
        self.ticks = 0
        self.fail_start = False
        self.fail_tick = False
        self.fail_stop = False
        self._extra_details: dict[str, Any] = {"component": "dummy"}

    def _start_impl(self) -> None:
        self.started += 1
        if self.fail_start:
            raise RuntimeError("boom-start")

    def _tick_impl(self) -> None:
        self.ticks += 1
        if self.fail_tick:
            raise RuntimeError("boom-tick")

    def _stop_impl(self) -> None:
        self.stopped += 1
        if self.fail_stop:
            raise RuntimeError("boom-stop")

    def _health_details(self) -> Mapping[str, Any]:
        return dict(self._extra_details)


def _is_utc_dt(value: Optional[datetime]) -> bool:
    """Check if a datetime value is UTC-aware.

    Args:
        value: Datetime to check, or None.

    Returns:
        True if the value is a UTC-aware datetime, False otherwise.
    """
    if value is None:
        return False
    return value.tzinfo is not None and value.tzinfo.utcoffset(value) == timezone.utc.utcoffset(value)


def test_start_transitions_to_running_and_is_idempotent() -> None:
    """Test that start() transitions agent to RUNNING and is idempotent."""
    agent = DummyAgent()
    assert agent.state == AgentState.NEW

    agent.start()
    assert agent.state == AgentState.RUNNING
    assert agent.started == 1

    # start() should be idempotent once running
    agent.start()
    assert agent.state == AgentState.RUNNING
    assert agent.started == 1


def test_tick_updates_timestamps_and_success_state() -> None:
    """Test that tick() updates timestamps and success state correctly."""
    agent = DummyAgent()
    agent.start()

    assert agent.health().to_dict()["last_tick_utc"] is None
    assert agent.health().to_dict()["last_success_utc"] is None

    agent.tick()

    report = agent.health()
    assert report.state == AgentState.RUNNING
    assert report.ok is True
    assert agent.ticks == 1

    assert report.last_tick_utc is not None
    assert report.last_success_utc is not None
    assert _is_utc_dt(report.last_tick_utc)
    assert _is_utc_dt(report.last_success_utc)

    # last_success should be >= last_tick in normal cases (very close timestamps)
    assert report.last_success_utc >= report.last_tick_utc


def test_tick_raises_if_not_running() -> None:
    """Test that tick() raises RuntimeError when agent is not running."""
    agent = DummyAgent()
    with pytest.raises(RuntimeError, match="not running"):
        agent.tick()

    # After stop() it's not running either
    agent.start()
    agent.stop()
    with pytest.raises(RuntimeError, match="not running"):
        agent.tick()


def test_tick_failure_marks_failed_and_sets_error_fields() -> None:
    """Test that exceptions in tick() mark the agent as FAILED with error details."""
    agent = DummyAgent()
    agent.start()
    agent.fail_tick = True

    with pytest.raises(RuntimeError, match="boom-tick"):
        agent.tick()

    report = agent.health()
    assert report.state == AgentState.FAILED
    assert report.ok is False
    assert report.last_error_utc is not None
    assert _is_utc_dt(report.last_error_utc)
    assert report.error is not None
    assert "RuntimeError" in report.error
    assert "boom-tick" in report.error


def test_start_failure_marks_failed() -> None:
    """Test that exceptions in start() mark the agent as FAILED."""
    agent = DummyAgent()
    agent.fail_start = True

    with pytest.raises(RuntimeError, match="boom-start"):
        agent.start()

    report = agent.health()
    assert report.state == AgentState.FAILED
    assert report.ok is False
    assert report.last_error_utc is not None
    assert report.error is not None
    assert "boom-start" in report.error


def test_stop_transitions_to_stopped_and_is_idempotent() -> None:
    """Test that stop() transitions agent to STOPPED and is idempotent."""
    agent = DummyAgent()
    agent.start()

    agent.stop()
    assert agent.state == AgentState.STOPPED
    assert agent.stopped == 1

    # stop() should be idempotent
    agent.stop()
    assert agent.state == AgentState.STOPPED
    assert agent.stopped == 1


def test_health_includes_custom_details() -> None:
    """Test that health() includes agent-specific custom details."""
    agent = DummyAgent()
    agent.start()
    report = agent.health()

    assert report.details["component"] == "dummy"
    # Ensure health is JSON-serializable via to_dict()
    as_dict = report.to_dict()
    assert as_dict["details"]["component"] == "dummy"
    assert as_dict["state"] == AgentState.RUNNING.value


def test_stop_failure_sets_error_but_ends_stopped() -> None:
    """Test that exceptions in stop() set error but still transition to STOPPED."""
    agent = DummyAgent()
    agent.start()
    agent.fail_stop = True

    with pytest.raises(RuntimeError, match="boom-stop"):
        agent.stop()

    # Even if stop fails, base class sets STOPPED per implementation
    assert agent.state == AgentState.STOPPED

    report = agent.health()
    assert report.last_error_utc is not None
    assert report.error is not None
    assert "stop() failed" in report.error
