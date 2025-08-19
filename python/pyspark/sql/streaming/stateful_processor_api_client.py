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
from enum import Enum
import json
import os
import socket
from typing import Any, Dict, List, Union, Optional, Tuple, Iterator

from pyspark.serializers import write_int, read_int, UTF8Deserializer
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import (
    StructType,
    TYPE_CHECKING,
    Row,
)
from pyspark.sql.pandas.types import convert_pandas_using_numpy_type
from pyspark.serializers import CPickleSerializer
from pyspark.errors import PySparkRuntimeError
import uuid

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["StatefulProcessorApiClient", "StatefulProcessorHandleState"]


class StatefulProcessorHandleState(Enum):
    PRE_INIT = 0
    CREATED = 1
    INITIALIZED = 2
    DATA_PROCESSED = 3
    TIMER_PROCESSED = 4
    CLOSED = 5


class StatefulProcessorApiClient:
    def __init__(
        self, state_server_port: int, key_schema: StructType, is_driver: bool = False
    ) -> None:
        self.key_schema = key_schema
        self._client_socket = socket.socket()
        self._client_socket.connect(("localhost", state_server_port))

        # SPARK-51667: We have a pattern of sending messages continuously from one side
        # (Python -> JVM, and vice versa) before getting response from other side. Since most
        # messages we are sending are small, this triggers the bad combination of Nagle's algorithm
        # and delayed ACKs, which can cause a significant delay on the latency.
        # See SPARK-51667 for more details on how this can be a problem.
        #
        # Disabling either would work, but it's more common to disable Nagle's algorithm; there is
        # lot less reference to disabling delayed ACKs, while there are lots of resources to
        # disable Nagle's algorithm.
        self._client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.sockfile = self._client_socket.makefile(
            "rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
        )
        if is_driver:
            self.handle_state = StatefulProcessorHandleState.PRE_INIT
        else:
            self.handle_state = StatefulProcessorHandleState.CREATED
        self.utf8_deserializer = UTF8Deserializer()
        self.pickleSer = CPickleSerializer()
        self.serializer = ArrowStreamSerializer()
        # Dictionaries to store the mapping between iterator id and a tuple of pandas DataFrame
        # and the index of the last row that was read.
        self.list_timer_iterator_cursors: Dict[str, Tuple["PandasDataFrameLike", int]] = {}
        # statefulProcessorApiClient is initialized per batch per partition,
        # so we will have new timestamps for a new batch
        self._batch_timestamp = -1
        self._watermark_timestamp = -1

    def set_handle_state(self, state: StatefulProcessorHandleState) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if state == StatefulProcessorHandleState.PRE_INIT:
            proto_state = stateMessage.PRE_INIT
        elif state == StatefulProcessorHandleState.CREATED:
            proto_state = stateMessage.CREATED
        elif state == StatefulProcessorHandleState.INITIALIZED:
            proto_state = stateMessage.INITIALIZED
        elif state == StatefulProcessorHandleState.DATA_PROCESSED:
            proto_state = stateMessage.DATA_PROCESSED
        elif state == StatefulProcessorHandleState.TIMER_PROCESSED:
            proto_state = stateMessage.TIMER_PROCESSED
        else:
            proto_state = stateMessage.CLOSED
        set_handle_state = stateMessage.SetHandleState(state=proto_state)
        handle_call = stateMessage.StatefulProcessorCall(setHandleState=set_handle_state)
        message = stateMessage.StateRequest(statefulProcessorCall=handle_call)

        self._send_proto_message(message.SerializeToString())

        response_message = self._receive_proto_message()
        status = response_message[0]
        if status == 0:
            self.handle_state = state
        else:
            # TODO(SPARK-49233): Classify errors thrown by internal methods.
            raise PySparkRuntimeError(f"Error setting handle state: " f"{response_message[1]}")

    def set_implicit_key(self, key: Tuple) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        key_bytes = self._serialize_to_bytes(self.key_schema, key)
        set_implicit_key = stateMessage.SetImplicitKey(key=key_bytes)
        request = stateMessage.ImplicitGroupingKeyRequest(setImplicitKey=set_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify errors thrown by internal methods.
            raise PySparkRuntimeError(f"Error setting implicit key: " f"{response_message[1]}")

    def remove_implicit_key(self) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        remove_implicit_key = stateMessage.RemoveImplicitKey()
        request = stateMessage.ImplicitGroupingKeyRequest(removeImplicitKey=remove_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify errors thrown by internal methods.
            raise PySparkRuntimeError(f"Error removing implicit key: " f"{response_message[1]}")

    def get_value_state(
        self, state_name: str, schema: Union[StructType, str], ttl_duration_ms: Optional[int]
    ) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = self._parse_string_schema(schema)

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        state_call_command.schema = schema.json()
        if ttl_duration_ms is not None:
            state_call_command.ttl.durationMs = ttl_duration_ms
        call = stateMessage.StatefulProcessorCall(getValueState=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error initializing value state: " f"{response_message[1]}")

    def get_list_state(
        self, state_name: str, schema: Union[StructType, str], ttl_duration_ms: Optional[int]
    ) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = self._parse_string_schema(schema)

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        state_call_command.schema = schema.json()
        if ttl_duration_ms is not None:
            state_call_command.ttl.durationMs = ttl_duration_ms
        call = stateMessage.StatefulProcessorCall(getListState=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error initializing list state: " f"{response_message[1]}")

    def register_timer(self, expiry_time_stamp_ms: int) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        register_call = stateMessage.RegisterTimer(expiryTimestampMs=expiry_time_stamp_ms)
        state_call_command = stateMessage.TimerStateCallCommand(register=register_call)
        call = stateMessage.StatefulProcessorCall(timerStateCall=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error register timer: " f"{response_message[1]}")

    def delete_timer(self, expiry_time_stamp_ms: int) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        delete_call = stateMessage.DeleteTimer(expiryTimestampMs=expiry_time_stamp_ms)
        state_call_command = stateMessage.TimerStateCallCommand(delete=delete_call)
        call = stateMessage.StatefulProcessorCall(timerStateCall=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error deleting timer: " f"{response_message[1]}")

    def get_list_timer_row(self, iterator_id: str) -> int:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if iterator_id in self.list_timer_iterator_cursors:
            # if the iterator is already in the dictionary, return the next row
            pandas_df, index = self.list_timer_iterator_cursors[iterator_id]
        else:
            list_call = stateMessage.ListTimers(iteratorId=iterator_id)
            state_call_command = stateMessage.TimerStateCallCommand(list=list_call)
            call = stateMessage.StatefulProcessorCall(timerStateCall=state_call_command)
            message = stateMessage.StateRequest(statefulProcessorCall=call)

            self._send_proto_message(message.SerializeToString())
            response_message = self._receive_proto_message()
            status = response_message[0]
            if status == 0:
                iterator = self._read_arrow_state()
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
            self.list_timer_iterator_cursors[iterator_id] = (pandas_df, new_index)
        else:
            # If the index is at the end of the DataFrame, remove the state from the dictionary.
            self.list_timer_iterator_cursors.pop(iterator_id, None)
        return pandas_df.at[index, "timestamp"].item()

    def get_expiry_timers_iterator(
        self, expiry_timestamp: int
    ) -> Iterator[list[Tuple[Tuple, int]]]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        while True:
            expiry_timer_call = stateMessage.ExpiryTimerRequest(expiryTimestampMs=expiry_timestamp)
            timer_request = stateMessage.TimerRequest(expiryTimerRequest=expiry_timer_call)
            message = stateMessage.StateRequest(timerRequest=timer_request)

            self._send_proto_message(message.SerializeToString())
            response_message = self._receive_proto_message()
            status = response_message[0]
            if status == 1:
                break
            elif status == 0:
                result_list = []
                iterator = self._read_arrow_state()
                for batch in iterator:
                    batch_df = batch.to_pandas()
                    for i in range(batch.num_rows):
                        deserialized_key = self.pickleSer.loads(batch_df.at[i, "key"])
                        timestamp = batch_df.at[i, "timestamp"].item()
                        result_list.append((tuple(deserialized_key), timestamp))
                yield result_list
            else:
                # TODO(SPARK-49233): Classify user facing errors.
                raise PySparkRuntimeError(f"Error getting expiry timers: " f"{response_message[1]}")

    def get_timestamps(self, time_mode: str) -> Tuple[int, int]:
        if time_mode.lower() == "none":
            return -1, -1
        else:
            if self._batch_timestamp == -1:
                self._batch_timestamp = self._get_batch_timestamp()
            if self._watermark_timestamp == -1:
                self._watermark_timestamp = self._get_watermark_timestamp()
        return self._batch_timestamp, self._watermark_timestamp

    def get_map_state(
        self,
        state_name: str,
        user_key_schema: Union[StructType, str],
        value_schema: Union[StructType, str],
        ttl_duration_ms: Optional[int],
    ) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        if isinstance(user_key_schema, str):
            user_key_schema = self._parse_string_schema(user_key_schema)
        if isinstance(value_schema, str):
            value_schema = self._parse_string_schema(value_schema)

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        state_call_command.schema = user_key_schema.json()
        state_call_command.mapStateValueSchema = value_schema.json()
        if ttl_duration_ms is not None:
            state_call_command.ttl.durationMs = ttl_duration_ms
        call = stateMessage.StatefulProcessorCall(getMapState=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error initializing map state: " f"{response_message[1]}")

    def delete_if_exists(self, state_name: str) -> None:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        call = stateMessage.StatefulProcessorCall(deleteIfExists=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error deleting state: " f"{response_message[1]}")

    def _get_batch_timestamp(self) -> int:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        get_processing_time_call = stateMessage.GetProcessingTime()
        timer_value_call = stateMessage.TimerValueRequest(
            getProcessingTimer=get_processing_time_call
        )
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message_with_long_value()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(
                f"Error getting processing timestamp: " f"{response_message[1]}"
            )
        else:
            timestamp = response_message[2]
            return timestamp

    def _get_watermark_timestamp(self) -> int:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        get_watermark_call = stateMessage.GetWatermark()
        timer_value_call = stateMessage.TimerValueRequest(getWatermark=get_watermark_call)
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message_with_long_value()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(
                f"Error getting eventtime timestamp: " f"{response_message[1]}"
            )
        else:
            timestamp = response_message[2]
            return timestamp

    def _send_proto_message(self, message: bytes) -> None:
        # Writing zero here to indicate message version. This allows us to evolve the message
        # format or even changing the message protocol in the future.
        write_int(0, self.sockfile)
        write_int(len(message), self.sockfile)
        self.sockfile.write(message)
        self.sockfile.flush()

    def _receive_proto_message(self) -> Tuple[int, str, bytes]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        length = read_int(self.sockfile)
        bytes = self.sockfile.read(length)
        message = stateMessage.StateResponse()
        message.ParseFromString(bytes)
        return message.statusCode, message.errorMessage, message.value

    def _receive_proto_message_with_long_value(self) -> Tuple[int, str, int]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        length = read_int(self.sockfile)
        bytes = self.sockfile.read(length)
        message = stateMessage.StateResponseWithLongTypeVal()
        message.ParseFromString(bytes)
        return message.statusCode, message.errorMessage, message.value

    def _receive_proto_message_with_string_value(self) -> Tuple[int, str, str]:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        length = read_int(self.sockfile)
        bytes = self.sockfile.read(length)
        message = stateMessage.StateResponseWithStringTypeVal()
        message.ParseFromString(bytes)
        return message.statusCode, message.errorMessage, message.value

    def _receive_str(self) -> str:
        return self.utf8_deserializer.loads(self.sockfile)

    def _serialize_to_bytes(self, schema: StructType, data: Tuple) -> bytes:
        from pyspark.testing.utils import have_numpy

        if have_numpy:
            import numpy as np

            def normalize_value(v: Any) -> Any:
                # Convert NumPy types to Python primitive types.
                if isinstance(v, np.generic):
                    return v.tolist()
                # List / tuple: recursively normalize each element
                if isinstance(v, (list, tuple)):
                    return type(v)(normalize_value(e) for e in v)
                # Dict: normalize both keys and values
                if isinstance(v, dict):
                    return {normalize_value(k): normalize_value(val) for k, val in v.items()}
                # Address a couple of pandas dtypes too.
                elif hasattr(v, "to_pytimedelta"):
                    return v.to_pytimedelta()
                elif hasattr(v, "to_pydatetime"):
                    return v.to_pydatetime()
                else:
                    return v

            converted = [normalize_value(v) for v in data]
        else:
            converted = list(data)

        field_names = [f.name for f in schema.fields]
        row_value = Row(**dict(zip(field_names, converted)))

        return self.pickleSer.dumps(schema.toInternal(row_value))

    def _deserialize_from_bytes(self, value: bytes) -> Any:
        return self.pickleSer.loads(value)

    def _send_arrow_state(self, schema: StructType, state: List[Tuple]) -> None:
        import pyarrow as pa
        import pandas as pd

        column_names = [field.name for field in schema.fields]
        pandas_df = convert_pandas_using_numpy_type(
            pd.DataFrame(state, columns=column_names), schema
        )
        batch = pa.RecordBatch.from_pandas(pandas_df)
        self.serializer.dump_stream(iter([batch]), self.sockfile)
        self.sockfile.flush()

    def _read_arrow_state(self) -> Any:
        return self.serializer.load_stream(self.sockfile)

    # Parse a string schema into a StructType schema. This method will perform an API call to
    # JVM side to parse the schema string.
    def _parse_string_schema(self, schema: str) -> StructType:
        import pyspark.sql.streaming.proto.StateMessage_pb2 as stateMessage

        parse_string_schema_call = stateMessage.ParseStringSchema(schema=schema)
        utils_request = stateMessage.UtilsRequest(parseStringSchema=parse_string_schema_call)
        message = stateMessage.StateRequest(utilsRequest=utils_request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message_with_string_value()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error parsing string schema: " f"{response_message[1]}")
        else:
            return StructType.fromJson(json.loads(response_message[2]))


class ListTimerIterator:
    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient):
        # Generate a unique identifier for the iterator to make sure iterators on the
        # same partition won't interfere with each other
        self.iterator_id = str(uuid.uuid4())
        self.stateful_processor_api_client = stateful_processor_api_client

    def __iter__(self) -> Iterator[int]:
        return self

    def __next__(self) -> int:
        return self.stateful_processor_api_client.get_list_timer_row(self.iterator_id)
