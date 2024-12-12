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
from typing import Dict, Iterator, Union, Tuple, Optional

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.types import StructType, TYPE_CHECKING
from pyspark.errors import PySparkRuntimeError
import uuid

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["MapStateClient"]


class MapStateClient:
    def __init__(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
        user_key_schema: Union[StructType, str],
        value_schema: Union[StructType, str],
    ) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client
        if isinstance(user_key_schema, str):
            self.user_key_schema = self._stateful_processor_api_client._parse_string_schema(
                user_key_schema
            )
        else:
            self.user_key_schema = user_key_schema
        if isinstance(value_schema, str):
            self.value_schema = self._stateful_processor_api_client._parse_string_schema(
                value_schema
            )
        else:
            self.value_schema = value_schema
        # Dictionaries to store the mapping between iterator id and a tuple of pandas DataFrame
        # and the index of the last row that was read.
        self.user_key_value_pair_iterator_cursors: Dict[str, Tuple["PandasDataFrameLike", int]] = {}
        self.user_key_or_value_iterator_cursors: Dict[str, Tuple["PandasDataFrameLike", int]] = {}

    def exists(self, state_name: str) -> bool:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

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
            raise PySparkRuntimeError(f"Error checking map state exists: {response_message[1]}")

    def get_value(self, state_name: str, user_key: Tuple) -> Optional[Tuple]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        bytes = self._stateful_processor_api_client._serialize_to_bytes(
            self.user_key_schema, user_key
        )
        get_value_call = stateMessage.GetValue(userKey=bytes)
        map_state_call = stateMessage.MapStateCall(stateName=state_name, getValue=get_value_call)
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
            raise PySparkRuntimeError(f"Error getting value: {response_message[1]}")

    def contains_key(self, state_name: str, user_key: Tuple) -> bool:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        bytes = self._stateful_processor_api_client._serialize_to_bytes(
            self.user_key_schema, user_key
        )
        contains_key_call = stateMessage.ContainsKey(userKey=bytes)
        map_state_call = stateMessage.MapStateCall(
            stateName=state_name, containsKey=contains_key_call
        )
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status == 0:
            return True
        elif status == 2:
            # Expect status code is 2 when the given key doesn't exist in the map state.
            return False
        else:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(
                f"Error checking if map state contains key: {response_message[1]}"
            )

    def update_value(
        self,
        state_name: str,
        user_key: Tuple,
        value: Tuple,
    ) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        key_bytes = self._stateful_processor_api_client._serialize_to_bytes(
            self.user_key_schema, user_key
        )
        value_bytes = self._stateful_processor_api_client._serialize_to_bytes(
            self.value_schema, value
        )
        update_value_call = stateMessage.UpdateValue(userKey=key_bytes, value=value_bytes)
        map_state_call = stateMessage.MapStateCall(
            stateName=state_name, updateValue=update_value_call
        )
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error updating map state value: {response_message[1]}")

    def get_key_value_pair(self, state_name: str, iterator_id: str) -> Tuple[Tuple, Tuple]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if iterator_id in self.user_key_value_pair_iterator_cursors:
            # If the state is already in the dictionary, return the next row.
            pandas_df, index = self.user_key_value_pair_iterator_cursors[iterator_id]
        else:
            # If the state is not in the dictionary, fetch the state from the server.
            iterator_call = stateMessage.Iterator(iteratorId=iterator_id)
            map_state_call = stateMessage.MapStateCall(stateName=state_name, iterator=iterator_call)
            state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
            message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

            self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
            response_message = self._stateful_processor_api_client._receive_proto_message()
            status = response_message[0]
            if status == 0:
                iterator = self._stateful_processor_api_client._read_arrow_state()
                # We need to exhaust the iterator here to make sure all the arrow batches are read,
                # even though there is only one batch in the iterator. Otherwise, the stream might
                # block further reads since it thinks there might still be some arrow batches left.
                # We only need to read the first batch in the iterator because it's guaranteed that
                # there would only be one batch sent from the JVM side.
                data_batch = None
                for batch in iterator:
                    if data_batch is None:
                        data_batch = batch
                if data_batch is None:
                    # TODO(SPARK-49233): Classify user facing errors.
                    raise PySparkRuntimeError("Error getting map state entry.")
                pandas_df = data_batch.to_pandas()
                index = 0
            else:
                raise StopIteration()

        new_index = index + 1
        if new_index < len(pandas_df):
            # Update the index in the dictionary.
            self.user_key_value_pair_iterator_cursors[iterator_id] = (pandas_df, new_index)
        else:
            # If the index is at the end of the DataFrame, remove the state from the dictionary.
            self.user_key_value_pair_iterator_cursors.pop(iterator_id, None)
        key_row_bytes = pandas_df.iloc[index, 0]
        value_row_bytes = pandas_df.iloc[index, 1]
        key_row = self._stateful_processor_api_client._deserialize_from_bytes(key_row_bytes)
        value_row = self._stateful_processor_api_client._deserialize_from_bytes(value_row_bytes)
        return tuple(key_row), tuple(value_row)

    def get_row(self, state_name: str, iterator_id: str, is_key: bool) -> Tuple:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if iterator_id in self.user_key_or_value_iterator_cursors:
            # If the state is already in the dictionary, return the next row.
            pandas_df, index = self.user_key_or_value_iterator_cursors[iterator_id]
        else:
            # If the state is not in the dictionary, fetch the state from the server.
            if is_key:
                keys_call = stateMessage.Keys(iteratorId=iterator_id)
                map_state_call = stateMessage.MapStateCall(stateName=state_name, keys=keys_call)
            else:
                values_call = stateMessage.Values(iteratorId=iterator_id)
                map_state_call = stateMessage.MapStateCall(stateName=state_name, values=values_call)
            state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
            message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

            self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
            response_message = self._stateful_processor_api_client._receive_proto_message()
            status = response_message[0]
            if status == 0:
                iterator = self._stateful_processor_api_client._read_arrow_state()
                # We need to exhaust the iterator here to make sure all the arrow batches are read,
                # even though there is only one batch in the iterator. Otherwise, the stream might
                # block further reads since it thinks there might still be some arrow batches left.
                # We only need to read the first batch in the iterator because it's guaranteed that
                # there would only be one batch sent from the JVM side.
                data_batch = None
                for batch in iterator:
                    if data_batch is None:
                        data_batch = batch
                if data_batch is None:
                    entry_name = "key"
                    if not is_key:
                        entry_name = "value"
                    # TODO(SPARK-49233): Classify user facing errors.
                    raise PySparkRuntimeError(f"Error getting map state {entry_name}.")
                pandas_df = data_batch.to_pandas()
                index = 0
            else:
                raise StopIteration()

        new_index = index + 1
        if new_index < len(pandas_df):
            # Update the index in the dictionary.
            self.user_key_or_value_iterator_cursors[iterator_id] = (pandas_df, new_index)
        else:
            # If the index is at the end of the DataFrame, remove the state from the dictionary.
            self.user_key_or_value_iterator_cursors.pop(iterator_id, None)
        pandas_row = pandas_df.iloc[index]
        return tuple(pandas_row)

    def remove_key(self, state_name: str, key: Tuple) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        bytes = self._stateful_processor_api_client._serialize_to_bytes(self.user_key_schema, key)
        remove_key_call = stateMessage.RemoveKey(userKey=bytes)
        map_state_call = stateMessage.MapStateCall(stateName=state_name, removeKey=remove_key_call)
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error removing key from map state: {response_message[1]}")

    def clear(self, state_name: str) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        clear_call = stateMessage.Clear()
        map_state_call = stateMessage.MapStateCall(stateName=state_name, clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(mapStateCall=map_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error clearing map state: " f"{response_message[1]}")


class MapStateIterator:
    def __init__(self, map_state_client: MapStateClient, state_name: str, is_key: bool):
        self.map_state_client = map_state_client
        self.state_name = state_name
        # Generate a unique identifier for the iterator to make sure iterators from the same
        # map state do not interfere with each other.
        self.iterator_id = str(uuid.uuid4())
        self.is_key = is_key

    def __iter__(self) -> Iterator[Tuple]:
        return self

    def __next__(self) -> Tuple:
        return self.map_state_client.get_row(self.state_name, self.iterator_id, self.is_key)


class MapStateKeyValuePairIterator:
    def __init__(self, map_state_client: MapStateClient, state_name: str):
        self.map_state_client = map_state_client
        self.state_name = state_name
        # Generate a unique identifier for the iterator to make sure iterators from the same
        # map state do not interfere with each other.
        self.iterator_id = str(uuid.uuid4())

    def __iter__(self) -> Iterator[Tuple[Tuple, Tuple]]:
        return self

    def __next__(self) -> Tuple[Tuple, Tuple]:
        return self.map_state_client.get_key_value_pair(self.state_name, self.iterator_id)
