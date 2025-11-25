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

from typing import Any, List, Optional, Tuple, Union, Dict, TYPE_CHECKING, Iterator
from itertools import groupby
from pyspark.sql.types import Row


if TYPE_CHECKING:
    import pandas as pd

from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
    ValueState,
    ListState,
)
from pyspark.sql.types import StructType

__all__ = ["TwsTester"]


class InMemoryValueState(ValueState):
    """In-memory implementation of ValueState for testing."""
    
    def __init__(
        self,
        handle: InMemoryStatefulProcessorHandle,
    ) -> None:
        self.handle = handle
        self.state = dict()

    def exists(self) -> bool:
        return self.handle.grouping_key in self.state

    def get(self) -> Optional[Tuple]:
        return self.state.get(self.handle.grouping_key, None)

    def update(self, newValue: Tuple) -> None:
        self.state[self.handle.grouping_key] = newValue

    def clear(self) -> None:
        del self.state[self.handle.grouping_key]


class InMemoryListState(ListState):
    """In-memory implementation of ListState for testing."""
    
    def __init__(
        self,
        handle: "InMemoryStatefulProcessorHandle",
    ) -> None:
        self.handle = handle
        self.state = dict()  # type: dict[Any, list[tuple]]

    def exists(self) -> bool:
        return self.handle.grouping_key in self.state

    def get(self) -> Iterator[Tuple]:
        return iter(self.state.get(self.handle.grouping_key, []))

    def put(self, newState: List[Tuple]) -> None:
        self.state[self.handle.grouping_key] = newState

    def appendValue(self, newState: Tuple) -> None:
        if not self.exists():
            self.state[self.handle.grouping_key] = []
        self.state.append(newState)

    def appendList(self, newState: List[Tuple]) -> None:
        if not self.exists():
            self.state[self.handle.grouping_key] = []
        self.state.extend(newState)

    def clear(self) -> None:
        del self.state[self.handle.grouping_key]


class InMemoryStatefulProcessorHandle(StatefulProcessorHandle):
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
            self.states[stateName] = InMemoryValueState(self)
        return self.states[stateName]

    def getListState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ListState:
        if stateName not in self.states:
            self.states[stateName] = InMemoryListState(self)
        return self.states[stateName]

    


class TwsTester:
    def __init__(
        self,
        processor: StatefulProcessor,
        initialState: Optional[List[Tuple[Any, Any]]] = None,
        key_column_name="key",
    ) -> None:
        self.processor = processor
        self.key_column_name = key_column_name
        self.handle = InMemoryStatefulProcessorHandle()

        self.processor.init(self.handle)

        if initialState:
            for key, state in initialState:
                self.api_client.set_implicit_key(key)
                self.processor.handleInitialState(key, state)

    def test(self, input: list[Row]) -> list[Row]:
        result: list[Row] = []
        sorted_input = sorted(input, key=lambda row: row[self.key_column_name])
        for key, group in groupby(
            sorted_input, key=lambda row: row[self.key_column_name]
        ):
            self.handle.setGroupingKey(key)
            rows = group
            timer_values = TimerValues(-1, -1)
            result_iter: Iterator[Row] = self.processor.handleInputRows(
                (key,), iter(rows), timer_values
            )
            for result_df in result_iter:
                result.append(result_df)
        return result

    def testInPandas(self, input: "pd.DataFrame") -> "pd.DataFrame":
        import pandas as pd

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

    def setValueState(self, stateName: str, key: Any, value: Tuple) -> None:
        """Directly set a value state for a given key."""
        self.handle.states[stateName].state[key] = value

    def peekValueState(self, stateName: str, key: Any) -> Optional[Tuple]:
        """Peek at a value state for a given key."""
        return self.handle.states[stateName].state.get(key, None)
