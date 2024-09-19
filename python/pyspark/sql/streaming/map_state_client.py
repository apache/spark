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
from typing import Any, Dict, Iterator, List, Union, cast, Tuple

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.types import Row, StructType, TYPE_CHECKING, _parse_datatype_string
from pyspark.errors import PySparkRuntimeError
import uuid

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["MapStateClient"]


class MapStateClient:
    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client
        # Dictionaries to store the mapping between iterator id and a tuple of pandas DataFrame
        # and the index of the last row that was read.
        self.key_value_dict: Dict[str, Tuple["PandasDataFrameLike", int]] = {}
        self.dict: Dict[str, Tuple["PandasDataFrameLike", int]] = {}

    def exists(self, state_name: str) -> bool:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        exists_call = stateMessage.Exists()
        map_state_call = stateMessage.MapStateCall(stateName=state_name, exists=exists_call)
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status == 0:
            return True
        elif status == 2:
            # Expect status code is 2 when state variable doesn't have a value.
            return False
        else:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(
                f"Error checking map state exists: " f"{response_message[1]}"
            )

    def get_value(self, state_name: str, key_schema: Union[StructType, str], key: Tuple) -> Any:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(key_schema, str):
            key_schema = cast(StructType, _parse_datatype_string(key_schema))
        bytes = self._stateful_processor_api_client._serialize_to_bytes(key_schema, key)
        get_value_call = stateMessage.GetValue(key=bytes)
        map_state_call = stateMessage.MapStateCall(
            stateName=state_name, getValue=get_value_call
        )
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status == 0:
            if len(response_message[2]) == 0:
                return None
            row = self._stateful_processor_api_client._deserialize_from_bytes(response_message[2])
            return row
        else:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting value: " f"{response_message[1]}")

    def get(self, state_name: str, iterator_id: str) -> Any:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if iterator_id in self.pandas_df_dict:
            # If the state is already in the dictionary, return the next row.
            pandas_df, index = self.pandas_df_dict[iterator_id]
        else:
            # If the state is not in the dictionary, fetch the state from the server.
            get_call = stateMessage.ListStateGet(iteratorId=iterator_id)
            list_state_call = stateMessage.ListStateCall(
                stateName=state_name, listStateGet=get_call
            )
            state_variable_request = stateMessage.StateVariableRequest(
                listStateCall=list_state_call
            )
            message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

            self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
            response_message = self._stateful_processor_api_client._receive_proto_message()
            status = response_message[0]
            if status == 0:
                iterator = self._stateful_processor_api_client._read_arrow_state()
                batch = next(iterator)
                pandas_df = batch.to_pandas()
                index = 0
            else:
                raise StopIteration()

        new_index = index + 1
        if new_index < len(pandas_df):
            # Update the index in the dictionary.
            self.pandas_df_dict[iterator_id] = (pandas_df, new_index)
        else:
            # If the index is at the end of the DataFrame, remove the state from the dictionary.
            self.pandas_df_dict.pop(iterator_id, None)
        pandas_row = pandas_df.iloc[index]
        return Row(**pandas_row.to_dict())

    def append_value(self, state_name: str, schema: Union[StructType, str], value: Tuple) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        bytes = self._stateful_processor_api_client._serialize_to_bytes(schema, value)
        append_value_call = stateMessage.AppendValue(value=bytes)
        list_state_call = stateMessage.ListStateCall(
            stateName=state_name, appendValue=append_value_call
        )
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def append_list(
        self, state_name: str, schema: Union[StructType, str], values: List[Tuple]
    ) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        append_list_call = stateMessage.AppendList()
        list_state_call = stateMessage.ListStateCall(
            stateName=state_name, appendList=append_list_call
        )
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())

        self._stateful_processor_api_client._send_arrow_state(schema, values)
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def put(self, state_name: str, schema: Union[StructType, str], values: List[Tuple]) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        put_call = stateMessage.ListStatePut()
        list_state_call = stateMessage.ListStateCall(stateName=state_name, listStatePut=put_call)
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())

        self._stateful_processor_api_client._send_arrow_state(schema, values)
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating value state: " f"{response_message[1]}")

    def clear(self, state_name: str) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        clear_call = stateMessage.Clear()
        list_state_call = stateMessage.ListStateCall(stateName=state_name, clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(listStateCall=list_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error clearing value state: " f"{response_message[1]}")


class ListStateIterator:
    def __init__(self, list_state_client: ListStateClient, state_name: str):
        self.list_state_client = list_state_client
        self.state_name = state_name
        # Generate a unique identifier for the iterator to make sure iterators from the same
        # list state do not interfere with each other.
        self.iterator_id = str(uuid.uuid4())

    def __iter__(self) -> Iterator[Row]:
        return self

    def __next__(self) -> Row:
        return self.list_state_client.get(self.state_name, self.iterator_id)
