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

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Iterator, Union

from pyspark.sql.streaming.state_api_client import StateApiClient
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike


class ValueState:
    def __init__(self,
                 state_api_client: StateApiClient,
                 state_name: str,
                 schema: Union[StructType, str]) -> None:
        self._state_api_client = state_api_client
        self._state_name = state_name
        self.schema = schema

    def exists(self) -> bool:
        return self._state_api_client.value_state_exists(self._state_name)

    def get(self) -> Any:
        return self._state_api_client.value_state_get(self._state_name)

    def update(self, new_value: Any) -> None:
        self._state_api_client.value_state_update(self._state_name, self.schema, new_value)

    def clear(self) -> None:
        self._state_api_client.value_state_clear(self._state_name)


class ListState:
    def __init__(self,
                 state_api_client: StateApiClient,
                 state_name: str) -> None:
        self._state_api_client = state_api_client
        self._state_name = state_name

    def exists(self) -> bool:
        pass

    def get(self) -> Iterator["PandasDataFrameLike"]:
        pass

    def update(self, new_value: Iterator["PandasDataFrameLike"]) -> None:
        pass

    def remove(self) -> None:
        pass


class StatefulProcessorHandle:
    def __init__(
            self,
            state_api_client: StateApiClient) -> None:
        self.state_api_client = state_api_client

    def getValueState(self, state_name: str, schema: Union[StructType, str]) -> ValueState:
        self.state_api_client.get_value_state(state_name, schema)
        return ValueState(self.state_api_client, state_name, schema)

    def getListState(self, state_name: str, schema: Union[StructType, str]) -> ListState:
        pass


class StatefulProcessor(ABC):
    @abstractmethod
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    @abstractmethod
    def handleInputRows(
            self,
            key: Any,
            rows: Iterator["PandasDataFrameLike"]) -> Iterator["PandasDataFrameLike"]:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
