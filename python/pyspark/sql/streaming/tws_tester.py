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

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql.streaming.stateful_processor import (
    ListState,
    MapState,
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
    ValueState,
)
from pyspark.sql.types import Row, StructType

__all__ = ["TwsTester"]


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

    def __init__(self) -> None:
        self.grouping_key = None
        self.value_states: dict[Any, _InMemoryValueState] = dict()
        self.list_states: dict[Any, _InMemoryListState] = dict()
        self.map_states: dict[Any, _InMemoryMapState] = dict()

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
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "registerTimer"},
        )

    def deleteTimer(self, expiryTimestampMs: int) -> None:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "deleteTimer"},
        )

    def listTimers(self) -> Iterator[int]:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "listTimers"},
        )

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
            raise AssertionError(f"State {stateName} has not been initialized.")


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

    **Not Supported:**
        - **Timers**: Only TimeMode.None is supported. If the processor attempts to register or
          use timers (as if in TimeMode.EventTime or TimeMode.ProcessingTime), a
          NotImplementedError will be raised.
        - **TTL**: State TTL configurations are ignored. All state persists indefinitely.

    **Use Cases:**
        - **Primary**: Unit testing business logic in ``handleInputRows`` implementations.
        - **Not recommended**: End-to-end testing or performance testing - use actual Spark
          streaming queries for those scenarios.

    Parameters
    ----------
    processor : :class:`StatefulProcessor`
        The StatefulProcessor to test
    initialStateRow : list of tuple, optional
        Initial state for each key as a list of (key, Row) tuples (for Row mode processors).
        Cannot be specified together with ``initialStatePandas``.
    initialStatePandas : list of tuple, optional
        Initial state for each key as a list of (key, DataFrame) tuples (for Pandas mode
        processors). Cannot be specified together with ``initialStateRow``.

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
    ) -> None:
        self.processor = processor
        self.handle = _InMemoryStatefulProcessorHandle()

        self.processor.init(self.handle)

        if initialStateRow is not None:
            assert initialStatePandas is None, "Cannot specify both Row and Pandas initial states."
            for key, row in initialStateRow:
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState((key,), row, TimerValues(-1, -1))
        elif initialStatePandas is not None:
            for key, df in initialStatePandas:
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState((key,), df, TimerValues(-1, -1))

    def test(self, key: Any, input: list[Row]) -> list[Row]:
        """
        Processes input rows for a single key through the stateful processor.

        This method is used for testing **Row mode** processors (transformWithState).
        It makes exactly one call to ``handleInputRows`` with the provided key and input rows.

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
        self.handle.setGroupingKey(key)
        timer_values = TimerValues(-1, -1)
        result_iter = cast(
            Iterator[Row],
            self.processor.handleInputRows((key,), iter(input), timer_values),
        )
        return list(result_iter)
        
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
        self.handle.setGroupingKey(key)
        timer_values = TimerValues(-1, -1)
        result_iter = cast(
            Iterator[pd.DataFrame],
            self.processor.handleInputRows((key,), iter([input]), timer_values),
        )
        result_dfs: list[pd.DataFrame] = list(result_iter)
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()

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
