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

from itertools import groupby
from typing import Any, Iterator, Optional, Union

import pandas as pd

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
        self.state = dict()

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

    def __init__(self):
        self.grouping_key = None
        self.states = dict()

    def setGroupingKey(self, key):
        self.grouping_key = key

    def getValueState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ValueState:
        if stateName not in self.states:
            self.states[stateName] = _InMemoryValueState(self)
        return self.states[stateName]

    def getListState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ListState:
        if stateName not in self.states:
            self.states[stateName] = _InMemoryListState(self)
        return self.states[stateName]

    def getMapState(
        self,
        stateName: str,
        userKeySchema: Union[StructType, str],
        valueSchema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> MapState:
        if stateName not in self.states:
            self.states[stateName] = _InMemoryMapState(self)
        return self.states[stateName]

    def registerTimer(self, expiryTimestampMs: int) -> None:
        raise NotImplementedError()

    def deleteTimer(self, expiryTimestampMs: int) -> None:
        raise NotImplementedError()

    def listTimers(self) -> Iterator[int]:
        raise NotImplementedError()

    def deleteIfExists(self, stateName: str) -> None:
        if stateName in self.states:
            del self.states[stateName]


class TwsTester:
    """
    Testing utility for transformWithState stateful processors.

    This class enables unit testing of StatefulProcessor business logic by simulating the
    behavior of transformWithState and transformWithStateInPandas. It processes input data
    and returns output data equivalent to what would be produced by the processor in an
    actual Spark streaming query.

    **Supported:**
        - Processing input data and producing output via :meth:`test` (Row mode) or
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
    initialStateRow : list of :class:`Row`, optional
        Initial state for each key as a list of Rows (for Row mode processors). Cannot be
        specified together with ``initialStatePandas``.
    initialStatePandas : :class:`pandas.DataFrame`, optional
        Initial state for each key as a pandas DataFrame (for Pandas mode processors). Cannot
        be specified together with ``initialStateRow``.
    key_column_name : str, optional
        Name of the column containing the grouping key in input data. Default is "key".

    Examples
    --------
    **Example 1: Testing a Row mode processor**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(processor)
    >>> result = tester.test([
    ...     Row(key="key1", value="a"),
    ...     Row(key="key1", value="b"),
    ...     Row(key="key2", value="c")
    ... ])
    >>> # Result: [Row(key="key1", count=2), Row(key="key2", count=1)]

    **Example 2: Testing a Pandas mode processor**

    >>> processor = RunningCountStatefulProcessorFactory().pandas()
    >>> tester = TwsTester(processor)
    >>> input_df = pd.DataFrame({"key": ["key1", "key1", "key2"], "value": ["a", "b", "c"]})
    >>> result = tester.testInPandas(input_df)
    >>> # Result: pd.DataFrame({"key": ["key1", "key2"], "count": [2, 1]})

    **Example 3: Testing with initial state**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(
    ...     processor,
    ...     initialStateRow=[Row(key="key1", initial_count=10)]
    ... )
    >>> result = tester.test([Row(key="key1", value="a")])
    >>> # Result: [Row(key="key1", count=11)]

    **Example 4: Direct state manipulation**

    >>> processor = RunningCountStatefulProcessorFactory().row()
    >>> tester = TwsTester(processor)
    >>> tester.setValueState("count", "key1", (5,))
    >>> result = tester.test([Row(key="key1", value="a")])
    >>> tester.peekValueState("count", "key1")
    >>> # Returns: (6,)

    .. versionadded:: 4.0.2
    """

    def __init__(
        self,
        processor: StatefulProcessor,
        initialStateRow: Optional[list[Row]] = None,
        initialStatePandas: Optional[pd.DataFrame] = None,
        key_column_name: str = "key",
    ) -> None:
        self.processor = processor
        self.key_column_name = key_column_name
        self.handle = _InMemoryStatefulProcessorHandle()

        self.processor.init(self.handle)

        if initialStateRow is not None:
            assert initialStatePandas is None, "Cannot specify both Row and Pandas initial states."
            for row in initialStateRow:
                key = row[self.key_column_name]
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState(key, row, TimerValues(-1, -1))
        elif initialStatePandas is not None:
            for key, group_df in initialStatePandas.groupby(self.key_column_name, dropna=False):
                self.handle.setGroupingKey(key)
                for _, row_df in group_df.iterrows():
                    single_row_df = pd.DataFrame([row_df]).reset_index(drop=True)
                    self.processor.handleInitialState(key, single_row_df, TimerValues(-1, -1))

    def test(self, input: list[Row]) -> list[Row]:
        """
        Processes input rows through the stateful processor, grouped by key.

        This method is used for testing **Row mode** processors (transformWithState). It
        corresponds to processing one microbatch. ``handleInputRows`` will be called once for
        each key that appears in the input.

        To simulate real-time mode, call this method repeatedly in a loop, passing a list with
        a single Row per call.

        The input is a list of :class:`Row` objects to process, which will be automatically
        grouped by the key column specified in the constructor. Returns a list of :class:`Row`
        objects representing all output rows produced by the processor during this batch.
        """
        result: list[Row] = []
        k: str = self.key_column_name
        sorted_input = sorted(
            input,
            key=lambda row: (row[k] is not None, row[k] if row[k] is not None else ""),
        )
        for key, rows in groupby(sorted_input, key=lambda row: row[self.key_column_name]):
            self.handle.setGroupingKey(key)
            timer_values = TimerValues(-1, -1)
            result_iter: Iterator[Row] = self.processor.handleInputRows(
                (key,), iter(rows), timer_values
            )
            result += list(result_iter)
        return result

    def testInPandas(self, input: pd.DataFrame) -> pd.DataFrame:
        """
        Processes input data through the stateful processor, grouped by key.

        This method does the same thing as :meth:`test`, but input and output are pandas
        DataFrames. It is used for testing **Pandas mode** processors - those intended to be
        passed to ``transformWithStateInPandas`` in a real streaming query.

        The input is a :class:`pandas.DataFrame` to process, which will be automatically grouped by
        the key column specified in the constructor. Returns a :class:`pandas.DataFrame`
        representing all output rows produced by the processor during this batch.
        """
        result_dfs = []
        sorted_input = input.sort_values(by=self.key_column_name, na_position="first")
        for key, group_df in sorted_input.groupby(self.key_column_name, dropna=False, sort=False):
            self.handle.setGroupingKey(key)
            timer_values = TimerValues(-1, -1)
            result_iter: Iterator[pd.DataFrame] = self.processor.handleInputRows(
                (key,), iter([group_df]), timer_values
            )
            for result_df in result_iter:
                result_dfs.append(result_df)
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()

    def setValueState(self, stateName: str, key: Any, value: tuple) -> None:
        """Directly set a value state for a given key."""
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        self.handle.states[stateName].state[key] = value

    def peekValueState(self, stateName: str, key: Any) -> Optional[tuple]:
        """
        Peek at a value state for a given key without modifying it.

        Returns None if the state does not exist for the given key.
        """
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        return self.handle.states[stateName].state.get(key, None)

    def setListState(self, stateName: str, key: Any, value: list[tuple]) -> None:
        """Directly set a list state for a given key."""
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        self.handle.states[stateName].state[key] = value

    def peekListState(self, stateName: str, key: Any) -> list[tuple]:
        """
        Peek at a list state for a given key without modifying it.

        Returns an empty list if the state does not exist for the given key.
        """
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        return list(self.handle.states[stateName].state.get(key, []))

    def setMapState(self, stateName: str, key: Any, value: dict) -> None:
        """Directly set a map state for a given key."""
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        self.handle.states[stateName].state[key] = dict(value)

    def peekMapState(self, stateName: str, key: Any) -> dict:
        """
        Peek at a map state for a given key without modifying it.

        Returns an empty dict if the state does not exist for the given key.
        """
        assert stateName in self.handle.states, f"State {stateName} has not been initialized."
        return dict(self.handle.states[stateName].state.get(key, {}))
