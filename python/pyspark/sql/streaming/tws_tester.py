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

from typing import Any, Callable, cast, Iterator, Optional, TYPE_CHECKING, Union

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
from pyspark.errors.exceptions.base import IllegalArgumentException

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["TwsTester"]


class _InMemoryTimers:
    """In-memory implementation of timers."""

    def __init__(self) -> None:
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
    """In-memory implementation of ValueState."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state: dict[Any, tuple] = dict()

    def _get_value(self) -> Optional[tuple]:
        key = self.handle.grouping_key
        return self.state.get(key, None)

    def exists(self) -> bool:
        return self._get_value() is not None

    def get(self) -> Optional[tuple]:
        return self._get_value()

    def update(self, newValue: tuple) -> None:
        key = self.handle.grouping_key
        self.state[key] = newValue

    def clear(self) -> None:
        if self.handle.grouping_key in self.state:
            del self.state[self.handle.grouping_key]


class _InMemoryListState(ListState):
    """In-memory implementation of ListState."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, list[tuple]]

    def _get_list(self) -> Optional[list[tuple]]:
        key = self.handle.grouping_key
        return self.state.get(key, None)

    def exists(self) -> bool:
        return self._get_list() is not None

    def get(self) -> Iterator[tuple]:
        result = self._get_list()
        return iter(result if result is not None else [])

    def put(self, newState: list[tuple]) -> None:
        key = self.handle.grouping_key
        self.state[key] = newState

    def appendValue(self, newState: tuple) -> None:
        key = self.handle.grouping_key
        if not self.exists():
            self.state[key] = []
        self.state[key].append(newState)

    def appendList(self, newState: list[tuple]) -> None:
        key = self.handle.grouping_key
        if not self.exists():
            self.state[key] = []
        self.state[key].extend(newState)

    def clear(self) -> None:
        if self.handle.grouping_key in self.state:
            del self.state[self.handle.grouping_key]


class _InMemoryMapState(MapState):
    """In-memory implementation of MapState."""

    def __init__(
        self,
        handle: _InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, dict[tuple, tuple]]

    def _get_map(self) -> Optional[dict[tuple, tuple]]:
        key = self.handle.grouping_key
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
    """In-memory implementation of StatefulProcessorHandle."""

    def __init__(self, timeMode: str = "None") -> None:
        self.grouping_key = None
        self.value_states: dict[Any, _InMemoryValueState] = dict()
        self.list_states: dict[Any, _InMemoryListState] = dict()
        self.map_states: dict[Any, _InMemoryMapState] = dict()
        self._timeMode = timeMode.lower()
        self._timers = _InMemoryTimers()
        self._currentWatermarkMs: int = 0

    def setGroupingKey(self, key: Any) -> None:
        self.grouping_key = key

    def setWatermark(self, watermarkMs: int) -> None:
        """Updates the current watermark. Used by TwsTester to sync watermark state."""
        self._currentWatermarkMs = watermarkMs

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
        if self._timeMode == "eventtime":
            if expiryTimestampMs <= self._currentWatermarkMs:
                raise IllegalArgumentException(
                    message=f"Cannot register timer with expiry {expiryTimestampMs} ms "
                    f"which is not later than the current watermark {self._currentWatermarkMs} ms."
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


def _asRowList(x: list[Any]) -> list[Row]:
    return cast(list[Row], x)


def _asPandasDataFrame(x: list[Any]) -> "PandasDataFrameLike":
    import pandas

    if len(x) == 0:
        return pandas.DataFrame()
    else:
        return pandas.concat(cast(list[pandas.DataFrame], x), ignore_index=True)


class TwsTester:
    """
    Testing utility for transformWithState stateful processors.

    This class enables unit testing of StatefulProcessor business logic by simulating the
    behavior of transformWithState. It processes input rows and returns output rows equivalent
    to those that would be produced by the processor in an actual Spark streaming query.

    **Supported:**
        - Processing input rows and producing output rows via :meth:`test` (Row mode) or
          :meth:`testInPandas` (Pandas mode).
        - Initial state setup via constructor parameters ``initialStateRow`` or
          ``initialStatePandas``.
        - Direct state manipulation via :meth:`updateValueState`, :meth:`updateListState`,
          :meth:`updateMapState`.
        - Direct state inspection via :meth:`peekValueState`, :meth:`peekListState`,
          :meth:`peekMapState`.
        - Timers in ProcessingTime mode (use :meth:`setProcessingTime` to fire timers).
        - Timers in EventTime mode (use :meth:`setWatermark` to manually set the watermark
          and fire expired timers).
        - Late event filtering in EventTime mode.

    **Not Supported:**
        - **TTL**. States persist indefinitely, even if TTLConfig is set.
        - **Automatic watermark propagation**: In production Spark streaming, the watermark is
          computed from event times and propagated at the end of each microbatch. TwsTester does
          not simulate this behavior because it processes keys individually rather than in batches.
          To test watermark-dependent logic, use :meth:`setWatermark` to manually set the watermark
          to the desired value before calling :meth:`test`.

    **Use Cases:**
        - **Primary**: Unit testing business logic in ``handleInputRows`` implementations.
        - **Not recommended**: End-to-end testing or performance testing - use actual Spark
          streaming queries for those scenarios.

    Parameters
    ----------
    processor : :class:`StatefulProcessor`
        The stateful processor to test.
    initialStateRow : list of tuple, optional
        Initial state for each key as a list of (key, Row) tuples (for Row mode processors).
        Cannot be specified together with ``initialStatePandas``.
    initialStatePandas : list of tuple, optional
        Initial state for each key as a list of (key, DataFrame) tuples (for Pandas mode
        processors). Cannot be specified together with ``initialStateRow``.
    timeMode : str, default "None"
        Time mode for the stateful processor. Valid values are "None", "ProcessingTime",
        or "EventTime" (case-insensitive).
    outputMode : str, default "Append"
        Output mode for the stateful processor. Valid values are "Append", "Update",
        or "Complete" (case-insensitive).
    eventTimeExtractor : callable, optional
        Function to extract event time (in milliseconds) from input rows. Required when
        ``timeMode`` is "EventTime". Used for late event filtering. For Row mode, the
        function takes a Row and returns an int. For Pandas mode, the function takes
        a DataFrame row and returns an int.

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
    >>> input_df = DataFrame({"value": ["a", "b"]})
    >>> result = tester.testInPandas("key1", input_df)
    >>> # Result: DataFrame({"key": ["key1"], "count": [2]})

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
    >>> tester.updateValueState("count", "key1", (5,))
    >>> result = tester.test("key1", [Row(value="a")])
    >>> tester.peekValueState("count", "key1")
    >>> # Returns: (6,)

    .. versionadded:: 4.2.0
    """

    def __init__(
        self,
        processor: StatefulProcessor,
        initialStateRow: Optional[list[tuple[Any, Row]]] = None,
        initialStatePandas: Optional[list[tuple[Any, "PandasDataFrameLike"]]] = None,
        timeMode: str = "None",
        outputMode: str = "Append",
        eventTimeExtractor: Optional[Callable[[Any], int]] = None,
    ) -> None:
        self.processor = processor
        self._timeMode = timeMode.lower()
        self._outputMode = outputMode
        self._eventTimeExtractor = eventTimeExtractor
        self._usingPandas = False

        # Timer-related state
        self._currentProcessingTimeMs: int = 0
        self._currentWatermarkMs: int = 0

        # Create handle
        self.handle = _InMemoryStatefulProcessorHandle(timeMode=timeMode)

        assert self._timeMode in ["none", "eventtime", "processingtime"]
        if self._timeMode == "eventtime" and eventTimeExtractor is None:
            raise PySparkValueError(
                errorClass="ARGUMENT_REQUIRED",
                messageParameters={
                    "arg_name": "eventTimeExtractor",
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
            self._usingPandas = True
            for key, df in initialStatePandas:
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState((key,), df, self._getTimerValues())

    def test(self, key: Any, input: list[Row]) -> list[Row]:
        """
        Processes input rows for a single key through the stateful processor.

        This method is used for testing **Row mode** processors (transformWithState).
        In EventTime mode, late events (where event time <= current watermark) are filtered out
        before reaching the processor, matching the behavior of real Spark streaming.

        The watermark is not automatically advanced based on event times. Use :meth:`setWatermark`
        to manually set the watermark before calling :meth:`test`.

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
        self._usingPandas = False
        self.handle.setGroupingKey(key)
        filtered_input = self._filterLateEventsRow(input)
        result_iter = self.processor.handleInputRows(
            (key,), iter(filtered_input), self._getTimerValues()
        )
        result: list[Row] = _asRowList(list(result_iter))
        return result

    def testInPandas(self, key: Any, input: "PandasDataFrameLike") -> "PandasDataFrameLike":
        """
        Processes input data for a single key through the stateful processor.

        This method does the same thing as :meth:`test`, but input and output are pandas
        DataFrames. It is used for testing **Pandas mode** processors - those intended to be
        passed to ``transformWithStateInPandas`` in a real streaming query.

        In EventTime mode, late events (where event time <= current watermark) are filtered out
        before reaching the processor, matching the behavior of real Spark streaming.

        The watermark is not automatically advanced based on event times. Use :meth:`setWatermark`
        to manually set the watermark before calling :meth:`testInPandas`.

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
        self._usingPandas = True
        self.handle.setGroupingKey(key)
        filtered_input = self._filterLateEventsPandas(input)
        result_iter = self.processor.handleInputRows(
            (key,), iter([filtered_input]), self._getTimerValues()
        )
        return _asPandasDataFrame(list(result_iter))

    def updateValueState(self, stateName: str, key: Any, value: tuple) -> None:
        """Sets the value state for a given key."""
        assert stateName in self.handle.value_states, f"State {stateName} has not been initialized."
        self.handle.value_states[stateName].state[key] = value

    def peekValueState(self, stateName: str, key: Any) -> Optional[tuple]:
        """
        Peek at a value state for a given key without modifying it.

        Returns None if the state does not exist for the given key.
        """
        assert stateName in self.handle.value_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return self.handle.value_states[stateName].get()

    def updateListState(self, stateName: str, key: Any, value: list[tuple]) -> None:
        """Sets the list state for a given key."""
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        self.handle.list_states[stateName].state[key] = value

    def peekListState(self, stateName: str, key: Any) -> list[tuple]:
        """
        Peek at a list state for a given key without modifying it.

        Returns an empty list if the state does not exist for the given key.
        """
        assert stateName in self.handle.list_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return list(self.handle.list_states[stateName].get())

    def updateMapState(self, stateName: str, key: Any, value: dict) -> None:
        """Sets the map state for a given key."""
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        self.handle.map_states[stateName].state[key] = dict(value)

    def peekMapState(self, stateName: str, key: Any) -> dict:
        """
        Peek at a map state for a given key without modifying it.

        Returns an empty dict if the state does not exist for the given key.
        """
        assert stateName in self.handle.map_states, f"State {stateName} has not been initialized."
        self.handle.setGroupingKey(key)
        return dict(self.handle.map_states[stateName].iterator())

    def deleteState(self, stateName: str, key: Any) -> None:
        """Deletes state for a given key."""
        self.handle.setGroupingKey(key)
        self.handle.deleteState(stateName)

    def _getTimerValues(self) -> TimerValues:
        """Creates TimerValues based on current time state."""
        if self._timeMode == "none":
            return TimerValues(-1, -1)
        elif self._timeMode == "processingtime":
            return TimerValues(self._currentProcessingTimeMs, -1)
        else:  # EventTime
            return TimerValues(self._currentProcessingTimeMs, self._currentWatermarkMs)

    def _handleExpiredTimers(self) -> list[Row] | "PandasDataFrameLike":
        """Handles expired timers and returns emitted rows."""
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

        if self._usingPandas:
            return _asPandasDataFrame(result)
        else:
            return _asRowList(result)

    def _filterLateEventsRow(self, values: list[Row]) -> list[Row]:
        """Filters out late events based on watermark and eventTimeExtractor."""
        if self._timeMode != "eventtime" or self._eventTimeExtractor is None:
            return values
        return [v for v in values if self._eventTimeExtractor(v) > self._currentWatermarkMs]

    def _filterLateEventsPandas(self, df: "PandasDataFrameLike") -> "PandasDataFrameLike":
        """Filters out late events based on watermark and eventTimeExtractor."""
        if self._timeMode != "eventtime" or self._eventTimeExtractor is None:
            return df
        extractor = self._eventTimeExtractor
        mask = df.apply(lambda row: extractor(row) > self._currentWatermarkMs, axis=1)
        return df[mask].reset_index(drop=True)

    def setProcessingTime(self, currentTimeMs: int) -> list[Row] | "PandasDataFrameLike":
        """
        Sets the simulated processing time and fires all expired timers.

        Call this after :meth:`test` to simulate time passage and trigger any timers registered
        with ``registerTimer()``. Timers with expiry time <= current processing time will fire,
        invoking ``handleExpiredTimer`` for each. This mirrors Spark's behavior where timers
        are processed after input data within a microbatch.

        Parameters
        ----------
        currentTimeMs : int
            The processing time to set in milliseconds.

        Returns
        -------
        list of :class:`Row` or :class:`pandas.DataFrame`
            Output rows emitted by ``handleExpiredTimer`` for all fired timers.
        """
        if self._timeMode != "processingtime":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={
                    "operation": "setProcessingTime with TimeMode other than ProcessingTime"
                },
            )
        if currentTimeMs <= self._currentProcessingTimeMs:
            raise IllegalArgumentException(
                message=f"Processing time must move forward. "
                f"Current: {self._currentProcessingTimeMs} ms, provided: {currentTimeMs} ms."
            )
        self._currentProcessingTimeMs = currentTimeMs
        return self._handleExpiredTimers()

    def setWatermark(self, currentWatermarkMs: int) -> list[Row] | "PandasDataFrameLike":
        """
        Sets the watermark and fires all expired event-time timers.

        Use this in EventTime mode to manually set the watermark. This is the only way to
        set the watermark in TwsTester, as automatic watermark propagation based on event
        times is not supported. Timers with expiry time <= new watermark will fire.

        Parameters
        ----------
        currentWatermarkMs : int
            The watermark to set in milliseconds.

        Returns
        -------
        list of :class:`Row` or :class:`pandas.DataFrame`
            Output rows emitted by ``handleExpiredTimer`` for all fired timers.
        """
        if self._timeMode != "eventtime":
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={"operation": "setWatermark with TimeMode other than EventTime"},
            )
        if currentWatermarkMs <= self._currentWatermarkMs:
            raise IllegalArgumentException(
                message=f"Watermark must move forward. "
                f"Current: {self._currentWatermarkMs} ms, provided: {currentWatermarkMs} ms."
            )
        self._currentWatermarkMs = currentWatermarkMs
        self.handle.setWatermark(currentWatermarkMs)
        return self._handleExpiredTimers()
