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

from pyspark.sql.pandas.serializers import TransformWithStateInPandasStateSerializer
from pyspark.sql.streaming.gen import StateMessage_pb2
from pyspark.sql.types import StructType

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
        value_state_call = StateMessage_pb2.ValueStateCall()
        value_state_call.get = StateMessage_pb2.ValueStateGet()

        self._state_serializer.send(value_state_call.SerializeToString())
        
        code = self._state_serializer.receive()
        assert code == 0

        exists = self._state_serializer.receive()
        print(f"value for {self._state_name} exists = {exists}")
        if (exists == 1):
            return self._state_serializer.readStr()
        
        return None

    def update(self, new_value: Any) -> None:
        value_state_call = StateMessage_pb2.ValueStateCall()
        value_state_call.update = StateMessage_pb2.ValueStateUpdate(new_value)

        self._state_serializer.send(value_state_call.SerializeToString())

        code = self._state_serializer.receive()
        assert code == 0

    def remove(self) -> None:
        pass

class StatefulProcessorHandle:
    def __init__(
        self,
        state_serializer: TransformWithStateInPandasStateSerializer) -> None:
        self._state_serializer = state_serializer

    def get_value_state(self, state_name: str, schema: Union[StructType, str]) -> ValueState:
        get_value_state = StateMessage_pb2.StatefulProcessorHandleCall()
        get_value_state.getValueState = StateMessage_pb2.GetValueState(state_name)

        self._state_serializer.send(get_value_state.SerializeToString())

        code = self._state_serializer.receive()
        assert code == 0
        return ValueState(self._state_serializer, state_name)


class StatefulProcessor(ABC):
    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def handle_input_rows(self, key: Any, rows: "PandasDataFrameLike") -> "PandasDataFrameLike":
        pass

    def getStatefulProcessorHandle(self) -> StatefulProcessorHandle:
        pass


class WrappedStatefulProcessor(StatefulProcessor):

    def __init__(self, handle: StatefulProcessorHandle,
                 udf_stateful_processor: StatefulProcessor) -> None:
        self.handle = handle
        self.udf_stateful_processor = udf_stateful_processor

    def init(self) -> None:
        self.udf_stateful_processor.init()

    def handle_input_rows(self, key: Any, rows: "PandasDataFrameLike") -> "PandasDataFrameLike":
        pass

    def getStatefulProcessorHandle(self) -> StatefulProcessorHandle:
        pass