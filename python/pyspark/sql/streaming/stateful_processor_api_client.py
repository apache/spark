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
import os
import socket
from typing import Any, Union, Optional, cast, Tuple, Iterator

from pyspark.serializers import write_int, read_int, UTF8Deserializer
from pyspark.sql.pandas.serializers import ArrowStreamSerializer
from pyspark.sql.types import StructType, _parse_datatype_string, Row
from pyspark.sql.utils import has_numpy
from pyspark.serializers import CPickleSerializer
from pyspark.errors import PySparkRuntimeError

__all__ = ["StatefulProcessorApiClient", "StatefulProcessorHandleState"]


class StatefulProcessorHandleState(Enum):
    CREATED = 1
    INITIALIZED = 2
    DATA_PROCESSED = 3
    TIMER_PROCESSED = 4
    CLOSED = 5


class StatefulProcessorApiClient:
    def __init__(self, state_server_port: int, key_schema: StructType) -> None:
        self.key_schema = key_schema
        self._client_socket = socket.socket()
        self._client_socket.connect(("localhost", state_server_port))
        self.sockfile = self._client_socket.makefile(
            "rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
        )
        self.handle_state = StatefulProcessorHandleState.CREATED
        self.utf8_deserializer = UTF8Deserializer()
        self.pickleSer = CPickleSerializer()
        self.serializer = ArrowStreamSerializer()

    def set_handle_state(self, state: StatefulProcessorHandleState) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if state == StatefulProcessorHandleState.CREATED:
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
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

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
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

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
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))

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

    def register_timer(self, expiry_time_stamp_ms: int) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

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
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        delete_call = stateMessage.DeleteTimer(expiryTimestampMs=expiry_time_stamp_ms)
        state_call_command = stateMessage.TimerStateCallCommand(delete=delete_call)
        call = stateMessage.StatefulProcessorCall(timerStateCall=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error delete timers: " f"{response_message[1]}")

    def list_timers(self) -> list[int]:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        list_call = stateMessage.ListTimers()
        state_call_command = stateMessage.TimerStateCallCommand(list=list_call)
        call = stateMessage.StatefulProcessorCall(timerStateCall=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status == 1:
            return []
        elif status == 0:
            iterator = self._read_arrow_state()
            batch = next(iterator)
            result_list = []
            batch_df = batch.to_pandas()
            for i in range(batch.num_rows):
                timestamp = batch_df.at[i, 'timestamp'].item()
                result_list.append(timestamp)
            return result_list
        else:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting expiry timers: " f"{response_message[1]}")

    def get_expiry_timers_iterator(self, expiry_timestamp: int) -> Iterator[list[Any, int]]:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage
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
                iterator = self._read_arrow_state()
                batch = next(iterator)
                result_list = []
                key_fields = [field.name for field in self.key_schema.fields]
                # TODO any better way to restore a grouping object from a batch?
                batch_df = batch.to_pandas()
                for i in range(batch.num_rows):
                    key = batch_df.at[i, 'key'].get(key_fields[0])
                    timestamp = batch_df.at[i, 'timestamp'].item()
                    result_list.append((key, timestamp))
                yield result_list
            else:
                # TODO(SPARK-49233): Classify user facing errors.
                raise PySparkRuntimeError(f"Error getting expiry timers: " f"{response_message[1]}")

    def get_batch_timestamp(self) -> int:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_processing_time_call = stateMessage.GetProcessingTime()
        timer_value_call = stateMessage.TimerValueRequest(getProcessingTimer=get_processing_time_call)
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting processing timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            timestamp = self._deserialize_long_from_bytes(response_message[2])
            return timestamp

    def get_watermark_timestamp(self) -> int:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_watermark_call = stateMessage.GetWatermark()
        timer_value_call = stateMessage.TimerValueRequest(getWatermark=get_watermark_call)
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._send_proto_message(message.SerializeToString())
        response_message = self._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting eventtime timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            timestamp = self._deserialize_long_from_bytes(response_message[2])
            return timestamp

    def _send_proto_message(self, message: bytes) -> None:
        # Writing zero here to indicate message version. This allows us to evolve the message
        # format or even changing the message protocol in the future.
        write_int(0, self.sockfile)
        write_int(len(message), self.sockfile)
        self.sockfile.write(message)
        self.sockfile.flush()

    def _receive_proto_message(self) -> Tuple[int, str, bytes]:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        length = read_int(self.sockfile)
        bytes = self.sockfile.read(length)
        message = stateMessage.StateResponse()
        message.ParseFromString(bytes)
        return message.statusCode, message.errorMessage, message.value

    def _receive_str(self) -> str:
        return self.utf8_deserializer.loads(self.sockfile)

    def _serialize_to_bytes(self, schema: StructType, data: Tuple) -> bytes:
        converted = []
        if has_numpy:
            import numpy as np

            # In order to convert NumPy types to Python primitive types.
            for v in data:
                if isinstance(v, np.generic):
                    converted.append(v.tolist())
                # Address a couple of pandas dtypes too.
                elif hasattr(v, "to_pytimedelta"):
                    converted.append(v.to_pytimedelta())
                elif hasattr(v, "to_pydatetime"):
                    converted.append(v.to_pydatetime())
                else:
                    converted.append(v)
        else:
            converted = list(data)

        row_value = Row(*converted)
        return self.pickleSer.dumps(schema.toInternal(row_value))

    def _deserialize_long_from_bytes(self, values: bytes) -> int:
        return int.from_bytes(values, byteorder='big')

    def _deserialize_from_bytes(self, value: bytes) -> Any:
        return self.pickleSer.loads(value)

    def _read_arrow_state(self) -> Any:
        return self.serializer.load_stream(self.sockfile)
