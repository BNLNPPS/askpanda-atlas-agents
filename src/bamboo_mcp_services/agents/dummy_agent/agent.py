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

import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from bamboo_mcp_services.agents.base import Agent


@dataclass(frozen=True)
class DummyAgentConfig:
    """Configuration for DummyAgent.

    Attributes:
        tick_interval_s: Time to sleep between ticks in the run loop.
        work_delay_s: Optional delay performed in each tick to simulate work.
    """

    tick_interval_s: float = 1.0
    work_delay_s: float = 0.0


class DummyAgent(Agent):
    """A minimal no-op agent used for testing and as a template.

    The agent:
    - starts successfully,
    - performs no real work in tick(),
    - exposes an interruptible run loop,
    - stops cleanly.
    """

    def __init__(self, name: str = "dummy-agent", config: Optional[DummyAgentConfig] = None) -> None:
        """Initialize the dummy agent.

        Args:
            name: Agent name.
            config: Optional DummyAgentConfig.
        """
        super().__init__(name=name)
        self._config = config or DummyAgentConfig()
        self._stop_event = threading.Event()
        self._ticks = 0

    @property
    def ticks(self) -> int:
        """Return how many ticks have been executed."""
        return self._ticks

    def request_stop(self) -> None:
        """Request the agent to stop (used by CLI/supervisor)."""
        self._stop_event.set()

    def run_forever(self) -> None:
        """Run tick loop until a stop is requested.

        This is a convenience wrapper for CLIs and local development.
        In production, the supervisor may call `tick()` itself.
        """
        self.start()

        # Make Ctrl+C and SIGTERM stop the agent cleanly in the CLI.
        self._install_signal_handlers()

        try:
            while not self._stop_event.is_set():
                self.tick()
                time.sleep(self._config.tick_interval_s)
        finally:
            self.stop()

    # ---- Agent hooks ---------------------------------------------------------

    def _start_impl(self) -> None:
        """Initialize resources (none for DummyAgent)."""
        # Nothing to initialize.
        return

    def _tick_impl(self) -> None:
        """Perform one unit of work (no-op)."""
        self._ticks += 1
        if self._config.work_delay_s > 0:
            time.sleep(self._config.work_delay_s)

    def _stop_impl(self) -> None:
        """Release resources (none for DummyAgent)."""
        self._stop_event.set()

    def _health_details(self) -> Mapping[str, Any]:
        """Return extra health details."""
        return {
            "tick_interval_s": self._config.tick_interval_s,
            "work_delay_s": self._config.work_delay_s,
            "ticks": self._ticks,
            "stop_requested": self._stop_event.is_set(),
        }

    # ---- Internal helpers ----------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Install signal handlers to request stop.

        Note:
            Python only delivers signals to the main thread. This is intended
            primarily for CLI usage.
        """

        def _handler(_signum: int, _frame: object) -> None:
            self.request_stop()

        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except ValueError:
            # Happens if called from a non-main thread; ignore for safety.
            pass
