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
from __future__ import annotations

from typing import Any, Iterator, Optional, Union, cast

import pandas as pd

from pyspark.sql.streaming.stateful_processor import (
    ExpiredTimerInfo,
    ListState,
    MapState,
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
    ValueState,
)
from pyspark.sql.types import Row, StructType
from pyspark.errors import PySparkValueError, PySparkAssertionError

__all__ = ["TwsTester"]


class _InMemoryTimers:
    """In-memory implementation of timers for testing."""

    def __init__(self) -> None:
        # Maps grouping key -> sorted set of timer expiry timestamps.
        self._key_to_timers: dict[Any, set[int]] = {}

    def registerTimer(self, grouping_key: Any, expiryTimestampMs: int) -> None:
        if grouping_key not in self._key_to_timers:
            self._key_to_timers[grouping_key] = set()
        self._key_to_timers[grouping_key].add(expiryTimestampMs)

    def deleteTimer(self, grouping_key: Any, expiryTimestampMs: int) -> None:
        if grouping_key in self._key_to_timers:
            self._key_to_timers[grouping_key].discard(expiryTimestampMs)
            if not self._key_to_timers[grouping_key]:
                del self._key_to_timers[grouping_key]

    def listTimers(self, grouping_key: Any) -> Iterator[int]:
        if grouping_key in self._key_to_timers:
            return iter(sorted(self._key_to_timers[grouping_key]))
        return iter([])

    def getAllKeysWithTimers(self) -> Iterator[Any]:
        return iter(self._key_to_timers.keys())


class _InMemoryValueState(ValueState):
    """In-memory implementation of ValueState for testing."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state: dict[Any, tuple] = dict()

    def exists(self) -> bool:
        return self.handle.grouping_key in self.state

    def get(self) -> Optional[tuple]:
        return self.state.get(self.handle.grouping_key, None)

    def update(self, newValue: tuple) -> None:
        self.state[self.handle.grouping_key] = newValue

    def clear(self) -> None:
        if self.exists():
            del self.state[self.handle.grouping_key]


class _InMemoryListState(ListState):
    """In-memory implementation of ListState for testing."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, list[tuple]]

    def exists(self) -> bool:
        return self.handle.grouping_key in self.state

    def get(self) -> Iterator[tuple]:
        return iter(self.state.get(self.handle.grouping_key, []))

    def put(self, newState: list[tuple]) -> None:
        self.state[self.handle.grouping_key] = newState

    def appendValue(self, newState: tuple) -> None:
        if not self.exists():
            self.state[self.handle.grouping_key] = []
        self.state[self.handle.grouping_key].append(newState)

    def appendList(self, newState: list[tuple]) -> None:
        if not self.exists():
            self.state[self.handle.grouping_key] = []
        self.state[self.handle.grouping_key].extend(newState)

    def clear(self) -> None:
        if self.exists():
            del self.state[self.handle.grouping_key]


class _InMemoryMapState(MapState):
    """In-memory implementation of MapState for testing."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, dict[tuple, tuple]]

    def exists(self) -> bool:
        return self.handle.grouping_key in self.state

    def getValue(self, key: tuple) -> Optional[tuple]:
        if not self.exists():
            return None
        return self.state[self.handle.grouping_key].get(key, None)

    def containsKey(self, key: tuple) -> bool:
        if not self.exists():
            return False
        return key in self.state[self.handle.grouping_key]

    def updateValue(self, key: tuple, value: tuple) -> None:
        if not self.exists():
            self.state[self.handle.grouping_key] = {}
        self.state[self.handle.grouping_key][key] = value

    def iterator(self) -> Iterator[tuple[tuple, tuple]]:
        if not self.exists():
            return iter([])
        return iter(self.state[self.handle.grouping_key].items())

    def keys(self) -> Iterator[tuple]:
        if not self.exists():
            return iter([])
        return iter(self.state[self.handle.grouping_key].keys())

    def values(self) -> Iterator[tuple]:
        if not self.exists():
            return iter([])
        return iter(self.state[self.handle.grouping_key].values())

    def removeKey(self, key: tuple) -> None:
        if self.exists() and key in self.state[self.handle.grouping_key]:
            del self.state[self.handle.grouping_key][key]

    def clear(self) -> None:
        if self.exists():
            del self.state[self.handle.grouping_key]


class _InMemoryStatefulProcessorHandle(StatefulProcessorHandle):
    """In-memory implementation of StatefulProcessorHandle for testing."""

    def __init__(self, timeMode: str = "None") -> None:
        self.grouping_key = None
        self.value_states: dict[Any, _InMemoryValueState] = dict()
        self.list_states: dict[Any, _InMemoryListState] = dict()
        self.map_states: dict[Any, _InMemoryMapState] = dict()
        self._timeMode = timeMode.lower()
        self._timers = _InMemoryTimers()

    def setGroupingKey(self, key: Any) -> None:
        self.grouping_key = key

    def getValueState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ValueState:
        if stateName not in self.value_states:
            self.value_states[stateName] = _InMemoryValueState(self)
        return self.value_states[stateName]

    def getListState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ListState:
        if stateName not in self.list_states:
            self.list_states[stateName] = _InMemoryListState(self)
        return self.list_states[stateName]

    def getMapState(
        self,
        stateName: str,
        userKeySchema: Union[StructType, str],
        valueSchema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> MapState:
        if stateName not in self.map_states:
            self.map_states[stateName] = _InMemoryMapState(self)
        return self.map_states[stateName]

    def registerTimer(self, expiryTimestampMs: int) -> None:
        if self._timeMode == "none":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={"operation": "Timers with TimeMode.None"},
            )
        self._timers.registerTimer(self.grouping_key, expiryTimestampMs)

    def deleteTimer(self, expiryTimestampMs: int) -> None:
        if self._timeMode == "none":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={"operation": "Timers with TimeMode.None"},
            )
        self._timers.deleteTimer(self.grouping_key, expiryTimestampMs)

    def listTimers(self) -> Iterator[int]:
        if self._timeMode == "none":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={"operation": "Timers with TimeMode.None"},
            )
        return self._timers.listTimers(self.grouping_key)

    def deleteIfExists(self, stateName: str) -> None:
        if stateName in self.value_states:
            del self.value_states[stateName]
        if stateName in self.list_states:
            del self.list_states[stateName]
        if stateName in self.map_states:
            del self.map_states[stateName]

    def deleteState(self, stateName: str) -> None:
        """Clears the state for the current grouping key."""
        if stateName in self.value_states:
            self.value_states[stateName].clear()
        elif stateName in self.list_states:
            self.list_states[stateName].clear()
        elif stateName in self.map_states:
            self.map_states[stateName].clear()
        else:
            raise PySparkAssertionError(
                errorClass="STATE_NOT_EXISTS",
                messageParameters={},
            )


def _as_row_list(x: Any) -> list[Row]:
    return cast(list[Row], x)


def _as_df_list(x: Any) -> list[pd.DataFrame]:
    return cast(list[pd.DataFrame], x)


class TwsTester:
    """
    Testing utility for transformWithState stateful processors.

    This class enables unit testing of StatefulProcessor business logic by simulating the
    behavior of transformWithState and transformWithStateInPandas. It processes input rows
    for a single key and returns output rows equivalent to those that would be produced by
    the processor in an actual Spark streaming query.

    **Supported:**
        - Processing input rows for a single key via :meth:`test` (Row mode) or
          :meth:`testInPandas` (Pandas mode).
        - Initial state setup via constructor parameters ``initialStateRow`` or
          ``initialStatePandas``.
        - Direct state manipulation via :meth:`setValueState`, :meth:`setListState`,
          :meth:`setMapState`.
        - Direct state inspection via :meth:`peekValueState`, :meth:`peekListState`,
          :meth:`peekMapState`.
        - Timers in ProcessingTime mode (use ``advanceProcessingTime`` to fire timers).
        - Timers in EventTime mode (use ``eventTimeColumnName`` and ``watermarkDelayMs`` to
          configure; watermark advances automatically based on event times, or use
          ``advanceWatermark`` manually).
        - Late event filtering in EventTime mode (events older than the current watermark are
          dropped).

    **Not Supported:**
        - **TTL**: State TTL configurations are ignored. All state persists indefinitely.

    **Use Cases:**
        - **Primary**: Unit testing business logic in ``handleInputRows`` implementations.
        - **Not recommended**: End-to-end testing or performance testing - use actual Spark
          streaming queries for those scenarios.

    Parameters
    ----------
    processor : :class:`StatefulProcessor`
        The StatefulProcessor to test.
    initialStateRow : list of tuple, optional
        Initial state for each key as a list of (key, Row) tuples (for Row mode processors).
        Cannot be specified together with ``initialStatePandas``.
    initialStatePandas : list of tuple, optional
        Initial state for each key as a list of (key, DataFrame) tuples (for Pandas mode
        processors). Cannot be specified together with ``initialStateRow``.
    timeMode : str, default "None"
        Time mode for the stateful processor. Valid values are "None", "ProcessingTime",
        or "EventTime" (case-insensitive). When set to "ProcessingTime", use
        ``advanceProcessingTime`` to simulate time passage and fire timers. When set to
        "EventTime", the watermark advances automatically based on event times in the input
        data, or use ``advanceWatermark`` to manually advance it.
    realTimeMode : bool, default False
        When True, input rows are processed one-by-one (separate call to handleInputRows
        for each input row), simulating real-time streaming behavior where each row arrives
        as a separate micro-batch. When False (default), all input rows are processed in
        a single call to handleInputRows.
    eventTimeColumnName : str, optional
        Name of the column containing event time in milliseconds. Required when ``timeMode``
        is "EventTime". The column value should be an integer representing milliseconds since
        epoch. Used for watermark calculation and late event filtering.
    watermarkDelayMs : int, default 0
        Watermark delay in milliseconds. The watermark is computed as
        ``max(event_time) - watermarkDelayMs``. Only used when ``timeMode`` is "EventTime".

    Examples
    --------
    **Example 1: Testing a Row mode processor**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(processor)
    >>> result = tester.test("key1", [Row(value="a"), Row(value="b")])
    >>> # Result: [Row(key="key1", count=2)]

    **Example 2: Testing a Pandas mode processor**

    >>> processor = RunningCountStatefulProcessorFactory().pandas()
    >>> tester = TwsTester(processor)
    >>> input_df = pd.DataFrame({"value": ["a", "b"]})
    >>> result = tester.testInPandas("key1", input_df)
    >>> # Result: pd.DataFrame({"key": ["key1"], "count": [2]})

    **Example 3: Testing with initial state**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(
    ...     processor,
    ...     initialStateRow=[("key1", Row(initial_count=10))]
    ... )
    >>> result = tester.test("key1", [Row(value="a")])
    >>> # Result: [Row(key="key1", count=11)]

    **Example 4: Direct state manipulation**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(processor)
    >>> tester.setValueState("count", "key1", (5,))
    >>> result = tester.test("key1", [Row(value="a")])
    >>> tester.peekValueState("count", "key1")
    >>> # Returns: (6,)

    .. versionadded:: 4.0.2
    """

    def __init__(
        self,
        processor: StatefulProcessor,
        initialStateRow: Optional[list[tuple[Any, Row]]] = None,
        initialStatePandas: Optional[list[tuple[Any, pd.DataFrame]]] = None,
        timeMode: str = "None",
        realTimeMode: bool = False,
        eventTimeColumnName: str = "",
        watermarkDelayMs: int = 0,
    ) -> None:
        self.processor = processor
        self._timeMode = timeMode.lower()
        self._eventTimeColumnName = eventTimeColumnName
        self._watermarkDelayMs = watermarkDelayMs
        self.handle = _InMemoryStatefulProcessorHandle(timeMode=timeMode)
        self.realTimeMode = realTimeMode

        # Timer-related state
        self._currentProcessingTimeMs: int = 0
        self._currentWatermarkMs: int = 0

        assert self._timeMode in ["none", "eventtime", "processingtime"]
        if self._timeMode == "eventtime" and eventTimeColumnName == "":
            raise PySparkValueError(
                errorClass="ARGUMENT_REQUIRED",
                messageParameters={
                    "arg_name": "eventTimeColumnName",
                    "condition": "timeMode is EventTime",
                },
            )

        self.processor.init(self.handle)

        if initialStateRow is not None:
            assert initialStatePandas is None, "Cannot specify both Row and Pandas initial states."
            for key, row in initialStateRow:
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState((key,), row, self._getTimerValues())
        elif initialStatePandas is not None:
            for key, df in initialStatePandas:
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState((key,), df, self._getTimerValues())

    def test(self, key: Any, input: list[Row]) -> list[Row]:
        """
        Processes input rows for a single key through the stateful processor.

        This method is used for testing **Row mode** processors (transformWithState).
        It makes exactly one call to ``handleInputRows`` with the provided key and input rows
        (unless realTimeMode=true).

        Parameters
        ----------
        key : Any
            The grouping key for the input rows.
        input : list of :class:`Row`
            Input rows to process for the given key.

        Returns
        -------
        list of :class:`Row`
            All output rows produced by the processor for this key.
        """
        if self.realTimeMode:
            return [row_out for row_in in input for row_out in self._testInternal(key, [row_in])]
        else:
            return self._testInternal(key, input)

    def _testInternal(self, key: Any, input: list[Row]) -> list[Row]:
        self.handle.setGroupingKey(key)
        filtered_input = self._filterLateEventsRow(input)
        result_iter = self.processor.handleInputRows(
            (key,), iter(filtered_input), self._getTimerValues()
        )
        result: list[Row] = _as_row_list(list(result_iter))

        if self._timeMode == "eventtime":
            self._updateWatermarkFromEventTimeRow(input)
            result.extend(_as_row_list(self._handleExpiredTimers()))

        return result

    def testInPandas(self, key: Any, input: pd.DataFrame) -> pd.DataFrame:
        """
        Processes input data for a single key through the stateful processor.

        This method does the same thing as :meth:`test`, but input and output are pandas
        DataFrames. It is used for testing **Pandas mode** processors - those intended to be
        passed to ``transformWithStateInPandas`` in a real streaming query.

        Parameters
        ----------
        key : Any
            The grouping key for the input data.
        input : :class:`pandas.DataFrame`
            Input data to process for the given key.

        Returns
        -------
        :class:`pandas.DataFrame`
            All output data produced by the processor for this key.
        """
        if self.realTimeMode:
            result_dfs: list[pd.DataFrame] = []
            for i in range(len(input)):
                single_row_df = input.iloc[[i]].reset_index(drop=True)
                result_dfs += self._testInPandasInternal(key, single_row_df)
        else:
            result_dfs = self._testInPandasInternal(key, input)

        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()

    def _testInPandasInternal(self, key: Any, input: pd.DataFrame) -> list[pd.DataFrame]:
        """Internal method that processes input DataFrame in a single batch."""
        self.handle.setGroupingKey(key)
        filtered_input = self._filterLateEventsPandas(input)
        result_iter = self.processor.handleInputRows(
            (key,), iter([filtered_input]), self._getTimerValues()
        )
        result: list[pd.DataFrame] = _as_df_list(list(result_iter))

        if self._timeMode == "eventtime":
            self._updateWatermarkFromEventTimePandas(input)
            result.append(cast(pd.DataFrame, self._handleExpiredTimers()))

        return result

    def setValueState(self, stateName: str, key: Any, value: tuple) -> None:
        """Directly set a value state for a given key."""
        assert stateName in self.handle.value_states, f"State {stateName} has not been initialized."
        self.handle.value_states[stateName].state[key] = value

    def peekValueState(self, stateName: str, key: Any) -> Optional[tuple]:
        """
        Peek at a value state for a given key without modifying it.

        Returns None if the state does not exist for the given key.
        """
        assert stateName in self.handle.value_states, f"State {stateName} has not been initialized."
        return self.handle.value_states[stateName].state.get(key, None)

    def setListState(self, stateName: str, key: Any, value: list[tuple]) -> None:
        """Directly set a list state for a given key."""
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        self.handle.list_states[stateName].state[key] = value

    def peekListState(self, stateName: str, key: Any) -> list[tuple]:
        """
        Peek at a list state for a given key without modifying it.

        Returns an empty list if the state does not exist for the given key.
        """
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        return list(self.handle.list_states[stateName].state.get(key, []))

    def setMapState(self, stateName: str, key: Any, value: dict) -> None:
        """Directly set a map state for a given key."""
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        self.handle.map_states[stateName].state[key] = dict(value)

    def peekMapState(self, stateName: str, key: Any) -> dict:
        """
        Peek at a map state for a given key without modifying it.

        Returns an empty dict if the state does not exist for the given key.
        """
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        return dict(self.handle.map_states[stateName].state.get(key, {}))

    def deleteState(self, stateName: str, key: Any) -> None:
        """Deletes state for a given key."""
        self.handle.setGroupingKey(key)
        self.handle.deleteState(stateName)

    # Timer-related helper methods

    def _getTimerValues(self) -> TimerValues:
        """Create TimerValues based on current time state."""
        if self._timeMode == "none":
            return TimerValues(-1, -1)
        elif self._timeMode == "processingtime":
            return TimerValues(self._currentProcessingTimeMs, -1)
        else:  # EventTime
            return TimerValues(self._currentProcessingTimeMs, self._currentWatermarkMs)

    def _handleExpiredTimers(self) -> list[Row] | pd.DataFrame:
        """Handle expired timers and return emitted rows."""
        if self._timeMode == "none":
            return []

        timer_values = self._getTimerValues()
        if self._timeMode == "processingtime":
            expiry_threshold = self._currentProcessingTimeMs
        else:  # eventtime
            expiry_threshold = self._currentWatermarkMs

        result: list = []
        for key in list(self.handle._timers.getAllKeysWithTimers()):
            self.handle.setGroupingKey(key)
            expired_timers = [
                t for t in self.handle._timers.listTimers(key) if t <= expiry_threshold
            ]
            for timer_expiry_ms in expired_timers:
                expired_timer_info = ExpiredTimerInfo(timer_expiry_ms)
                output_iter = self.processor.handleExpiredTimer(
                    (key,), timer_values, expired_timer_info
                )
                result.extend(list(output_iter))
                self.handle._timers.deleteTimer(key, timer_expiry_ms)

        if len(result) > 0 and isinstance(result[0], pd.DataFrame):
            return pd.concat(_as_df_list(result), ignore_index=True)
        else:
            return _as_row_list(result)

    def _getEventTimeMs(self, row: Row) -> int:
        """Extract event time in milliseconds from a Row."""
        return getattr(row, self._eventTimeColumnName)

    def _filterLateEventsRow(self, values: list[Row]) -> list[Row]:
        """Filter out late events based on the current watermark, in EventTime mode."""
        if self._timeMode != "eventtime" or self._eventTimeColumnName is None:
            return values
        return [v for v in values if self._getEventTimeMs(v) >= self._currentWatermarkMs]

    def _filterLateEventsPandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out late events from DataFrame based on current watermark."""
        if self._timeMode != "eventtime" or self._eventTimeColumnName is None:
            return df
        mask = df[self._eventTimeColumnName] >= self._currentWatermarkMs
        return df[mask].reset_index(drop=True)

    def _updateWatermarkFromEventTimeRow(self, values: list[Row]) -> None:
        """Update watermark based on max event time in input rows."""
        if self._timeMode != "eventtime" or self._eventTimeColumnName is None:
            return
        for value in values:
            event_time_ms = self._getEventTimeMs(value)
            self._currentWatermarkMs = max(
                self._currentWatermarkMs, event_time_ms - self._watermarkDelayMs
            )

    def _updateWatermarkFromEventTimePandas(self, df: pd.DataFrame) -> None:
        """Update watermark based on max event time in DataFrame."""
        if self._timeMode != "eventtime" or self._eventTimeColumnName is None:
            return
        if len(df) == 0:
            return
        max_event_time = df[self._eventTimeColumnName].max()
        self._currentWatermarkMs = max(
            self._currentWatermarkMs, max_event_time - self._watermarkDelayMs
        )

    def advanceProcessingTime(self, durationMs: int) -> list[Row] | pd.DataFrame:
        """Advances the simulated processing time and fires all expired timers.

        Call this after `test()` to simulate time passage and trigger any timers registered
        with `registerTimer()`. Timers with expiry time <= current processing time will fire,
        invoking `handleExpiredTimer` for each. This mirrors Spark's behavior where timers
        are processed after input data within a microbatch.
        """
        if self._timeMode != "processingtime":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={
                    "operation": "advanceProcessingTime with TimeMode other than ProcessingTime"
                },
            )
        self._currentProcessingTimeMs += durationMs
        return self._handleExpiredTimers()

    def advanceWatermark(self, durationMs: int) -> list[Row] | pd.DataFrame:
        """Advances the watermark and fires all expired event-time timers."""
        if self._timeMode != "eventtime":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={
                    "operation": "advanceWatermark with TimeMode other than EventTime"
                },
            )
        self._currentWatermarkMs += durationMs
        return self._handleExpiredTimers()
