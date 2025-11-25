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
        del self.states[stateName]


class TwsTester:
    def __init__(
        self,
        processor: StatefulProcessor,
        initialStateRow: Optional[list[Row]] = None,
        initialStatePandas: Optional[pd.DataFrame] = None,
        key_column_name="key",
    ) -> None:
        self.processor = processor
        self.key_column_name = key_column_name
        self.handle = _InMemoryStatefulProcessorHandle()

        self.processor.init(self.handle)

        if initialStateRow is not None:
            assert initialStatePandas is None, (
                "Cannot specify both Row and Pandas initial states."
            )
            for row in initialStateRow:
                key = row[self.key_column_name]
                self.handle.setGroupingKey(key)
                self.processor.handleInitialState(key, row, TimerValues(-1, -1))
        elif initialStatePandas is not None:
            for key, group_df in initialStatePandas.groupby(self.key_column_name):
                self.handle.setGroupingKey(key)
                for _, row_df in group_df.iterrows():
                    single_row_df = pd.DataFrame([row_df]).reset_index(drop=True)
                    self.processor.handleInitialState(
                        key, single_row_df, TimerValues(-1, -1)
                    )

    def test(self, input: list[Row]) -> list[Row]:
        result: list[Row] = []
        sorted_input = sorted(input, key=lambda row: row[self.key_column_name])
        for key, rows in groupby(
            sorted_input, key=lambda row: row[self.key_column_name]
        ):
            self.handle.setGroupingKey(key)
            timer_values = TimerValues(-1, -1)
            result_iter: Iterator[Row] = self.processor.handleInputRows(
                (key,), iter(rows), timer_values
            )
            result += list(result_iter)
        return result

    def testInPandas(self, input: pd.DataFrame) -> pd.DataFrame:
        result_dfs = []
        sorted_input = input.sort_values(by=self.key_column_name)
        for key, group_df in sorted_input.groupby(self.key_column_name):
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
        self.handle.states[stateName].state[key] = value

    def peekValueState(self, stateName: str, key: Any) -> Optional[tuple]:
        """Peek at a value state for a given key."""
        return self.handle.states[stateName].state.get(key, None)

    def setListState(self, stateName: str, key: Any, value: list[tuple]) -> None:
        """Directly set a list state for a given key."""
        self.handle.states[stateName].state[key] = value

    def peekListState(self, stateName: str, key: Any) -> list[tuple]:
        """Peek at a list state for a given key."""
        return list(self.handle.states[stateName].state.get(key, []))

    def setMapState(self, stateName: str, key: Any, value: dict) -> None:
        """Directly set a map state for a given key."""
        self.handle.states[stateName].state[key] = dict(value)

    def peekMapState(self, stateName: str, key: Any) -> dict:
        """Peek at a map state for a given key."""
        return dict(self.handle.states[stateName].state.get(key, {}))
