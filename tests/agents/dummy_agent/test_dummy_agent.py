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

"""Tests for the dummy agent."""
from __future__ import annotations

import threading
import time

from bamboo_mcp_services.agents.base import AgentState
from bamboo_mcp_services.agents.dummy_agent.agent import DummyAgent, DummyAgentConfig


def test_dummy_agent_lifecycle_start_tick_stop() -> None:
    """Test that the dummy agent follows the standard lifecycle correctly."""
    agent = DummyAgent(config=DummyAgentConfig(tick_interval_s=0.01))

    assert agent.state == AgentState.NEW

    agent.start()
    assert agent.state == AgentState.RUNNING

    agent.tick()
    agent.tick()
    assert agent.ticks == 2

    report = agent.health()
    assert report.ok is True
    assert report.details["ticks"] == 2

    agent.stop()
    assert agent.state == AgentState.STOPPED


def test_dummy_agent_run_forever_stops_on_request() -> None:
    """Test that the dummy agent's run_forever loop stops when requested."""
    agent = DummyAgent(config=DummyAgentConfig(tick_interval_s=0.01))

    t = threading.Thread(target=agent.run_forever, daemon=True)
    t.start()

    # Allow a few ticks to happen
    time.sleep(0.05)
    assert agent.state == AgentState.RUNNING
    assert agent.ticks > 0

    agent.request_stop()
    t.join(timeout=2.0)

    assert agent.state == AgentState.STOPPED
