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

"""Implementation of a progress bar that is displayed while a query is running."""
import abc
from dataclasses import dataclass
import time
import sys
import typing
from types import TracebackType
from typing import Iterable, Any

from pyspark.sql.connect.proto import ExecutePlanResponse

try:
    from IPython.utils.terminal import get_terminal_size
except ImportError:

    def get_terminal_size(defaultx: Any = None, defaulty: Any = None) -> Any:  # type: ignore[misc]
        return (80, 25)


from pyspark.sql.connect.shell import progress_bar_enabled


@dataclass
class StageInfo:
    stage_id: int
    num_tasks: int
    num_completed_tasks: int
    num_bytes_read: int
    done: bool


class ProgressHandler(abc.ABC):
    @abc.abstractmethod
    def __call__(
        self,
        stages: typing.Optional[Iterable[StageInfo]],
        inflight_tasks: int,
        operation_id: typing.Optional[str],
        done: bool,
    ) -> None:
        pass


def from_proto(
    proto: ExecutePlanResponse.ExecutionProgress,
) -> typing.Tuple[Iterable[StageInfo], int]:
    result = []
    for stage in proto.stages:
        result.append(
            StageInfo(
                stage_id=stage.stage_id,
                num_tasks=stage.num_tasks,
                num_completed_tasks=stage.num_completed_tasks,
                num_bytes_read=stage.input_bytes_read,
                done=stage.done,
            )
        )
    return (result, proto.num_inflight_tasks)


class Progress:
    """This is a small helper class to visualize a textual progress bar.
    he interface is very simple and assumes that nothing else prints to the
    standard output."""

    SI_BYTE_SIZES = (1 << 60, 1 << 50, 1 << 40, 1 << 30, 1 << 20, 1 << 10, 1)
    SI_BYTE_SUFFIXES = ("EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B")

    def __init__(
        self,
        char: str = "*",
        min_width: int = 80,
        output: typing.IO = sys.stdout,
        enabled: bool = False,
        handlers: Iterable[ProgressHandler] = [],
        operation_id: typing.Optional[str] = None,
    ) -> None:
        """
        Constructs a new Progress bar. The progress bar is typically used in
        the blocking query execution path to process the execution progress
        methods from the server.

        Parameters
        ----------
        char : str
          The Default character to be used for printing the bar.
        min_width : numeric
          The minimum width of the progress bar
        output : file
          The output device to write the progress bar to.
        enabled : bool
          Whether the progress bar printing should be enabled or not.
        handlers : list of ProgressHandler
          A list of handlers that will be called when the progress bar is updated.
        """
        self._ticks: typing.Optional[int] = None
        self._tick: typing.Optional[int] = None
        x, y = get_terminal_size()
        self._min_width = min_width
        self._char = char
        self._width = max(min(min_width, x), self._min_width)
        self._max_printed = 0
        self._started = time.time()
        self._enabled = enabled or progress_bar_enabled()
        self._bytes_read = 0
        self._out = output
        self._running = 0
        self._handlers = handlers
        self._stages: Iterable[StageInfo] = []
        self._operation_id = operation_id

    def _notify(self, done: bool = False) -> None:
        for handler in self._handlers:
            handler(
                stages=self._stages,
                inflight_tasks=self._running,
                operation_id=self._operation_id,
                done=done,
            )

    def __enter__(self) -> "Progress":
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exception: typing.Optional[BaseException],
        exc_tb: typing.Optional[TracebackType],
    ) -> typing.Any:
        self.finish()
        return False

    def update_ticks(
        self,
        stages: Iterable[StageInfo],
        inflight_tasks: int,
        operation_id: typing.Optional[str] = None,
    ) -> None:
        """This method is called from the execution to update the progress bar with a new total
        tick counter and the current position. This is necessary in case new stages get added with
        new tasks and so the total task number will be updated as well.

        Parameters
        ----------
        stages : list
          A list of StageInfo objects reporting progress in each stage.
        inflight_tasks : int
          The number of tasks that are currently running.
        """
        if self._operation_id is None or len(self._operation_id) == 0:
            self._operation_id = operation_id

        total_tasks = sum(map(lambda x: x.num_tasks, stages))
        completed_tasks = sum(map(lambda x: x.num_completed_tasks, stages))
        if total_tasks > 0:
            self._ticks = total_tasks
            self._tick = completed_tasks
            self._bytes_read = sum(map(lambda x: x.num_bytes_read, stages))
            if self._tick is not None and self._tick >= 0:
                self.output()
            self._running = inflight_tasks
            self._stages = stages
            self._notify(False)

    def finish(self) -> None:
        """Clear the last line. Called when the processing is done."""
        self._notify(True)
        if self._enabled:
            print("\r" + " " * self._max_printed, end="", flush=True, file=self._out)
            print("\r", end="", flush=True, file=self._out)

    def output(self) -> None:
        """Writes the progress bar out."""
        if self._enabled and self._tick is not None and self._ticks is not None:
            val = int((self._tick / float(self._ticks)) * self._width)
            bar = self._char * val + "-" * (self._width - val)
            percent_complete = (self._tick / self._ticks) * 100
            elapsed = int(time.time() - self._started)
            scanned = self._bytes_to_string(self._bytes_read)
            running = self._running
            buffer = (
                f"\r[{bar}] {percent_complete:.2f}% Complete "
                f"({running} Tasks running, {elapsed}s, Scanned {scanned})"
            )
            self._max_printed = max(len(buffer), self._max_printed)
            print(buffer, end="", flush=True, file=self._out)

    @staticmethod
    def _bytes_to_string(size: int) -> str:
        """Helper method to convert a numeric bytes value into a human-readable representation"""
        i = 0
        while i < len(Progress.SI_BYTE_SIZES) - 1 and size < 2 * Progress.SI_BYTE_SIZES[i]:
            i += 1
        result = float(size) / Progress.SI_BYTE_SIZES[i]
        return f"{result:.1f} {Progress.SI_BYTE_SUFFIXES[i]}"
