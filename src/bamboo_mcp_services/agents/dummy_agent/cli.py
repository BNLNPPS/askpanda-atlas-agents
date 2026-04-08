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

import argparse

from bamboo_mcp_services.agents.dummy_agent.agent import DummyAgent, DummyAgentConfig


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="bamboo-dummy",
        description="Run the AskPanDA Dummy Agent (no-op) until stopped.",
    )
    parser.add_argument(
        "--tick-interval",
        type=float,
        default=1.0,
        help="Seconds to sleep between ticks (default: 1.0).",
    )
    parser.add_argument(
        "--work-delay",
        type=float,
        default=0.0,
        help="Optional seconds to sleep inside each tick to simulate work (default: 0.0).",
    )
    return parser


def main() -> int:
    """CLI entry point.

    Returns:
        Exit code (0 for success).
    """
    args = build_parser().parse_args()

    agent = DummyAgent(
        config=DummyAgentConfig(
            tick_interval_s=args.tick_interval,
            work_delay_s=args.work_delay,
        )
    )
    agent.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
