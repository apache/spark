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
from typing import Any, TYPE_CHECKING

from pyspark.sql.pandas.serializers import TransformWithStateInPandasStateSerializer

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

class ValueState:
    def __init__(self,
        state_serializer: TransformWithStateInPandasStateSerializer,
        state_name: str) -> None:
        self._state_serializer = state_serializer
        self._state_name = state_name

    def exists(self) -> bool:
        pass

    def get(self) -> Any:
        self._state_serializer.send("ValueState",
            self._state_name,
            "get",
            self._state_serializer.grouping_key_tracker.getKey()
        )
        
        code = self._state_serializer.receive()
        assert code == 0

        exists = self._state_serializer.receive()
        print(f"value for {self._state_name} exists = {exists}")
        if (exists == 1):
            return self._state_serializer.readStr()
        
        return None

    def update(self, new_value: Any) -> None:
        self._state_serializer.send("ValueState",
            self._state_name,
            "set",
            self._state_serializer.grouping_key_tracker.getKey(),
            str(new_value)
        )
        code = self._state_serializer.receive()
        assert code == 0

    def remove(self) -> None:
        pass


class StatefulProcessorHandle:
    def __init__(
        self,
        state_serializer: TransformWithStateInPandasStateSerializer) -> None:
        self._state_serializer = state_serializer

    def get_value_state(self, state_name: str) -> ValueState:
        self._state_serializer.send("getState", "ValueState", state_name)
        code = self._state_serializer.receive()
        assert code == 0
        return ValueState(self._state_serializer, state_name)


class StatefulProcessor(ABC):
    @abstractmethod
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    @abstractmethod
    def handle_input_rows(self, key: Any, handle: StatefulProcessorHandle,
        rows: "PandasDataFrameLike") -> "PandasDataFrameLike":
        pass
