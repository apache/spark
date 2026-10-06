#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Internal numeric metrics and reusable timers for one Python worker task."""

import time
from typing import Any, Optional


class _WorkerTimer:
    """Measure one named duration and add each interval to the collector's task total.

    The collector owns the duration dictionary. This scope keeps a reference to it and a
    start time for its current with block. Reuse the scope across sequential batches.
    Nested scopes measure inclusive time, including their children.

    Pipelined reads can overlap UDF work on another thread. An additive phase breakdown
    for that mode would require tracking those overlapping intervals.
    """

    def __init__(self, shared_duration_totals_ns: dict[str, int], metric_name: str) -> None:
        """Bind one metric name to the collector's shared duration totals."""
        # Keep the collector's dictionary by reference so updates reach its task totals.
        self._shared_duration_totals_ns = shared_duration_totals_ns
        self._metric_name = metric_name
        self._block_start_ns: Optional[int] = None

    def __enter__(self) -> None:
        """Start the clock when Python enters the with block."""
        # Re-entering this instance would overwrite an unfinished measurement's start time.
        if self._block_start_ns is not None:
            raise RuntimeError("Worker timer is already running")
        self._block_start_ns = time.perf_counter_ns()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Accumulate elapsed time when Python leaves the block, including on an exception."""
        assert self._block_start_ns is not None
        elapsed_ns = time.perf_counter_ns() - self._block_start_ns
        self._block_start_ns = None
        self._shared_duration_totals_ns[self._metric_name] += elapsed_ns
        # Python supplies the exception arguments; returning None lets the error propagate.


class WorkerMetrics:
    """Collect values and duration totals for one worker task.

    set/increment store values in their reporting units: counts, byte counts, or epoch milliseconds.
    measure creates timer scopes that accumulate nanoseconds in a separate shared dictionary.
    to_dict combines both dictionaries, converting only durations to milliseconds.
    """

    def __init__(self) -> None:
        """Create fresh task state so a reused worker does not retain the previous totals."""
        # Counters, spill byte counts, and epoch timestamps need no conversion at report time.
        self._values_in_report_units: dict[str, int] = {}
        # These totals belong to the collector; timer scopes share and update them.
        self._duration_totals_ns: dict[str, int] = {}

    def set(self, name: str, value: int) -> None:
        """Store a value in its reporting unit, replacing any earlier value.

        Used for initial counter values, epoch timestamps, and final spill byte counts.
        Elapsed durations are accumulated through measure instead.
        """
        self._values_in_report_units[name] = value

    def increment(self, name: str, value: int = 1) -> None:
        """Add to a counter in its reporting unit, starting from zero.

        The default increment is one, as used for the number of timed batches.
        """
        self._values_in_report_units[name] = self._values_in_report_units.get(name, 0) + value

    def measure(self, name: str) -> _WorkerTimer:
        """Create a named timing scope; entering it starts the clock."""
        # Register zero so an empty supported task can still report this timing.
        self._duration_totals_ns.setdefault(name, 0)
        return _WorkerTimer(self._duration_totals_ns, name)

    def to_dict(self) -> dict[str, int]:
        """Build the numeric report dictionary consumed by report_metrics.

        Counters, bytes, and epoch timestamps already have their reporting units. Only the
        accumulated duration totals require conversion from nanoseconds to milliseconds.
        """
        # Round once per task, preserving sub-millisecond contributions from individual batches.
        return {
            **self._values_in_report_units,
            **{name: duration // 1_000_000 for name, duration in self._duration_totals_ns.items()},
        }
