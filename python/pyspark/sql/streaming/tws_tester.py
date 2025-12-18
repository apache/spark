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

from typing import Any, Callable, Iterator, Optional, Union, cast

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


class _TtlTracker:
    """Helper to track expired keys based on TTL."""

    def __init__(self, get_current_time_ms: Callable[[], int], ttl_duration_ms: int) -> None:
        assert ttl_duration_ms >= 0, "TTL duration must be non-negative."
        self._get_current_time_ms = get_current_time_ms
        self._ttl_duration_ms = ttl_duration_ms
        self._key_to_last_updated_time: dict[Any, int] = {}

    def is_key_expired(self, key: Any) -> bool:
        if self._ttl_duration_ms == 0:
            return False
        if key not in self._key_to_last_updated_time:
            return False
        expiration = self._key_to_last_updated_time[key] + self._ttl_duration_ms
        return expiration <= self._get_current_time_ms()

    def on_key_updated(self, key: Any) -> None:
        self._key_to_last_updated_time[key] = self._get_current_time_ms()


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
        ttl_tracker: Optional[_TtlTracker] = None,
    ) -> None:
        self.handle = handle
        self.state: dict[Any, tuple] = dict()
        self._ttl_tracker = ttl_tracker

    def _get_value(self) -> Optional[tuple]:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None and self._ttl_tracker.is_key_expired(key):
            return None
        return self.state.get(key, None)

    def exists(self) -> bool:
        return self._get_value() is not None

    def get(self) -> Optional[tuple]:
        return self._get_value()

    def update(self, newValue: tuple) -> None:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None:
            self._ttl_tracker.on_key_updated(key)
        self.state[key] = newValue

    def clear(self) -> None:
        if self.handle.grouping_key in self.state:
            del self.state[self.handle.grouping_key]


class _InMemoryListState(ListState):
    """In-memory implementation of ListState for testing."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
        ttl_tracker: Optional[_TtlTracker] = None,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, list[tuple]]
        self._ttl_tracker = ttl_tracker

    def _get_list(self) -> Optional[list[tuple]]:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None and self._ttl_tracker.is_key_expired(key):
            return None
        return self.state.get(key, None)

    def exists(self) -> bool:
        return self._get_list() is not None

    def get(self) -> Iterator[tuple]:
        result = self._get_list()
        return iter(result if result is not None else [])

    def put(self, newState: list[tuple]) -> None:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None:
            self._ttl_tracker.on_key_updated(key)
        self.state[key] = newState

    def appendValue(self, newState: tuple) -> None:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None:
            self._ttl_tracker.on_key_updated(key)
        if not self.exists():
            self.state[key] = []
        self.state[key].append(newState)

    def appendList(self, newState: list[tuple]) -> None:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None:
            self._ttl_tracker.on_key_updated(key)
        if not self.exists():
            self.state[key] = []
        self.state[key].extend(newState)

    def clear(self) -> None:
        if self.handle.grouping_key in self.state:
            del self.state[self.handle.grouping_key]


class _InMemoryMapState(MapState):
    """In-memory implementation of MapState for testing."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
        ttl_tracker: Optional[_TtlTracker] = None,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, dict[tuple, tuple]]
        self._ttl_tracker = ttl_tracker

    def _get_map(self) -> Optional[dict[tuple, tuple]]:
        key = self.handle.grouping_key
        if self._ttl_tracker is not None and self._ttl_tracker.is_key_expired(key):
            return None
        return self.state.get(key, None)

    def exists(self) -> bool:
        return self._get_map() is not None

    def getValue(self, key: tuple) -> Optional[tuple]:
        map_state = self._get_map()
        if map_state is None:
            return None
        return map_state.get(key, None)

    def containsKey(self, key: tuple) -> bool:
        map_state = self._get_map()
        if map_state is None:
            return False
        return key in map_state

    def updateValue(self, key: tuple, value: tuple) -> None:
        grouping_key = self.handle.grouping_key
        if self._ttl_tracker is not None:
            self._ttl_tracker.on_key_updated(grouping_key)
        if not self.exists():
            self.state[grouping_key] = {}
        self.state[grouping_key][key] = value

    def iterator(self) -> Iterator[tuple[tuple, tuple]]:
        map_state = self._get_map()
        if map_state is None:
            return iter([])
        return iter(map_state.items())

    def keys(self) -> Iterator[tuple]:
        map_state = self._get_map()
        if map_state is None:
            return iter([])
        return iter(map_state.keys())

    def values(self) -> Iterator[tuple]:
        map_state = self._get_map()
        if map_state is None:
            return iter([])
        return iter(map_state.values())

    def removeKey(self, key: tuple) -> None:
        map_state = self._get_map()
        if map_state is not None and key in map_state:
            del map_state[key]

    def clear(self) -> None:
        if self.handle.grouping_key in self.state:
            del self.state[self.handle.grouping_key]


class _InMemoryStatefulProcessorHandle(StatefulProcessorHandle):
    """In-memory implementation of StatefulProcessorHandle for testing."""

    def __init__(
        self, timeMode: str = "None", get_current_time_ms: Optional[Callable[[], int]] = None
    ) -> None:
        self.grouping_key = None
        self.value_states: dict[Any, _InMemoryValueState] = dict()
        self.list_states: dict[Any, _InMemoryListState] = dict()
        self.map_states: dict[Any, _InMemoryMapState] = dict()
        self._timeMode = timeMode.lower()
        self._timers = _InMemoryTimers()
        self._get_current_time_ms = get_current_time_ms or (lambda: 0)

    def setGroupingKey(self, key: Any) -> None:
        self.grouping_key = key

    def _create_ttl_tracker(self, ttlDurationMs: Optional[int]) -> Optional[_TtlTracker]:
        if ttlDurationMs is None or ttlDurationMs == 0:
            return None
        return _TtlTracker(self._get_current_time_ms, ttlDurationMs)

    def getValueState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ValueState:
        if stateName not in self.value_states:
            ttl_tracker = self._create_ttl_tracker(ttlDurationMs)
            self.value_states[stateName] = _InMemoryValueState(self, ttl_tracker)
        return self.value_states[stateName]

    def getListState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ListState:
        if stateName not in self.list_states:
            ttl_tracker = self._create_ttl_tracker(ttlDurationMs)
            self.list_states[stateName] = _InMemoryListState(self, ttl_tracker)
        return self.list_states[stateName]

    def getMapState(
        self,
        stateName: str,
        userKeySchema: Union[StructType, str],
        valueSchema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> MapState:
        if stateName not in self.map_states:
            ttl_tracker = self._create_ttl_tracker(ttlDurationMs)
            self.map_states[stateName] = _InMemoryMapState(self, ttl_tracker)
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
        - TTL for ValueState, ListState, and MapState (use ProcessingTime mode and
          ``advanceProcessingTime`` to test expiry).

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
        self.realTimeMode = realTimeMode

        # Timer-related state
        self._currentProcessingTimeMs: int = 0
        self._currentWatermarkMs: int = 0

        # Create handle with time getter for TTL support
        self.handle = _InMemoryStatefulProcessorHandle(
            timeMode=timeMode, get_current_time_ms=lambda: self._currentProcessingTimeMs
        )

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

        Returns None if the state does not exist for the given key or if it has expired due to TTL.
        """
        assert stateName in self.handle.value_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return self.handle.value_states[stateName].get()

    def setListState(self, stateName: str, key: Any, value: list[tuple]) -> None:
        """Directly set a list state for a given key."""
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        self.handle.list_states[stateName].state[key] = value

    def peekListState(self, stateName: str, key: Any) -> list[tuple]:
        """
        Peek at a list state for a given key without modifying it.

        Returns an empty list if the state does not exist for the given key or if it has expired
        due to TTL.
        """
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return list(self.handle.list_states[stateName].get())

    def setMapState(self, stateName: str, key: Any, value: dict) -> None:
        """Directly set a map state for a given key."""
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        self.handle.map_states[stateName].state[key] = dict(value)

    def peekMapState(self, stateName: str, key: Any) -> dict:
        """
        Peek at a map state for a given key without modifying it.

        Returns an empty dict if the state does not exist for the given key or if it has expired
        due to TTL.
        """
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return dict(self.handle.map_states[stateName].iterator())

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
