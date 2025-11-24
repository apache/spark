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

from typing import Any, List, Optional, Tuple, Union, Dict, TYPE_CHECKING
from itertools import groupby
from pyspark.sql.types import Row


if TYPE_CHECKING:
    import pandas as pd

from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
    ValueState,
)
from pyspark.sql.types import StructType

__all__ = ["TwsTester"]


class InMemoryValueStateClient:
    def __init__(self, schema: Union[StructType, str]) -> None:
        if isinstance(schema, str):
            self.schema = StructType.fromDDL(schema)
        else:
            self.schema = schema
        self._value: Optional[Tuple] = None
        self._exists: bool = False

    def exists(self) -> bool:
        return self._exists

    def get(self) -> Optional[Tuple]:
        return self._value

    def update(self, value: Tuple) -> None:
        self._value = value
        self._exists = True

    def clear(self) -> None:
        self._value = None
        self._exists = False


class InMemoryStatefulProcessorApiClient:
    def __init__(self) -> None:
        self._current_key: Any = None
        self._value_states: Dict[Any, Dict[str, InMemoryValueStateClient]] = {}
        self._state_schemas: Dict[str, Union[StructType, str]] = {}

    def set_implicit_key(self, key: Any) -> None:
        self._current_key = key
        if key not in self._value_states:
            self._value_states[key] = {}

    def get_value_state(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> None:
        self._state_schemas[stateName] = schema

    def _get_or_create_value_state_client(
        self, stateName: str, schema: Union[StructType, str]
    ) -> InMemoryValueStateClient:
        if self._current_key not in self._value_states:
            self._value_states[self._current_key] = {}

        if stateName not in self._value_states[self._current_key]:
            self._value_states[self._current_key][stateName] = InMemoryValueStateClient(
                schema
            )

        return self._value_states[self._current_key][stateName]


class InMemoryStatefulProcessorHandle(StatefulProcessorHandle):
    def __init__(self, api_client: InMemoryStatefulProcessorApiClient) -> None:
        self._api_client = api_client

    def getValueState(
        self,
        stateName: str,
        schema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> ValueState:
        self._api_client.get_value_state(stateName, schema, ttlDurationMs)

        class InMemoryValueState(ValueState):
            def __init__(
                self,
                api_client: InMemoryStatefulProcessorApiClient,
                name: str,
                schema: Union[StructType, str],
            ) -> None:
                self._api_client = api_client
                self._name = name
                self._schema = schema

            def _get_client(self) -> InMemoryValueStateClient:
                return self._api_client._get_or_create_value_state_client(
                    self._name, self._schema
                )

            def exists(self) -> bool:
                return self._get_client().exists()

            def get(self) -> Optional[Tuple]:
                return self._get_client().get()

            def update(self, newValue: Tuple) -> None:
                self._get_client().update(newValue)

            def clear(self) -> None:
                self._get_client().clear()

        return InMemoryValueState(self._api_client, stateName, schema)


def LOG(s):
    with open("/tmp/log.txt", "a") as f:
        f.write(s + "\n")


class TwsTester:
    def __init__(
        self,
        processor: StatefulProcessor,
        initialState: Optional[List[Tuple[Any, Any]]] = None,
        key_column_name="key",
    ) -> None:
        self.processor = processor
        self.key_column_name = key_column_name

        self.api_client = InMemoryStatefulProcessorApiClient()
        self.handle = InMemoryStatefulProcessorHandle(self.api_client)

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
            self.api_client.set_implicit_key(key)
            rows = [item[1] for item in group]
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
            self.api_client.set_implicit_key(key)
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
        self.api_client.set_implicit_key(key)
        if stateName in self.api_client._state_schemas:
            schema = self.api_client._state_schemas[stateName]
            client = self.api_client._get_or_create_value_state_client(
                stateName, schema
            )
            client.update(value)

    def peekValueState(self, stateName: str, key: Any) -> Optional[Tuple]:
        self.api_client.set_implicit_key(key)
        if stateName in self.api_client._state_schemas:
            schema = self.api_client._state_schemas[stateName]
            client = self.api_client._get_or_create_value_state_client(
                stateName, schema
            )
            return client.get()
        return None
