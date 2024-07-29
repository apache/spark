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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from enum import Enum
import os
import socket
from typing import Union, cast, Tuple

import pyspark.sql.streaming.proto as stateMessage
from pyspark.serializers import write_int, read_int, UTF8Deserializer
from pyspark.sql.types import StructType, _parse_datatype_string, Row
from pyspark.sql.utils import has_numpy
from pyspark.serializers import CPickleSerializer
from pyspark.errors import PySparkRuntimeError

__all__ = ["StatefulProcessorHandleState", "StatefulProcessorApiClient"]


class StatefulProcessorHandleState(Enum):
    CREATED = 1
    INITIALIZED = 2
    DATA_PROCESSED = 3
    CLOSED = 4


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

    def set_handle_state(self, state: StatefulProcessorHandleState) -> None:
        proto_state = self._get_proto_state(state)
        set_handle_state = stateMessage.SetHandleState(state=proto_state)
        handle_call = stateMessage.StatefulProcessorCall(setHandleState=set_handle_state)
        message = stateMessage.StateRequest(statefulProcessorCall=handle_call)

        self._send_proto_message(message)

        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if status == 0:
            self.handle_state = state
        else:
            raise PySparkRuntimeError(
                f"Error setting handle state: " f"{response_message.errorMessage}"
            )

    def set_implicit_key(self, key: Tuple) -> None:
        key_bytes = self._serialize_to_bytes(self.key_schema, key)
        set_implicit_key = stateMessage.SetImplicitKey(key=key_bytes)
        request = stateMessage.ImplicitGroupingKeyRequest(setImplicitKey=set_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if status != 0:
            raise PySparkRuntimeError(
                f"Error setting implicit key: " f"{response_message.errorMessage}"
            )

    def remove_implicit_key(self) -> None:
        print("calling remove_implicit_key on python side")
        remove_implicit_key = stateMessage.RemoveImplicitKey()
        request = stateMessage.ImplicitGroupingKeyRequest(removeImplicitKey=remove_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if status != 0:
            raise PySparkRuntimeError(
                f"Error removing implicit key: " f"{response_message.errorMessage}"
            )

    def get_value_state(self, state_name: str, schema: Union[StructType, str]) -> None:
        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        state_call_command.schema = schema.json()
        call = stateMessage.StatefulProcessorCall(getValueState=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if status != 0:
            raise PySparkRuntimeError(
                f"Error initializing value state: " f"{response_message.errorMessage}"
            )

    def _get_proto_state(
        self, state: StatefulProcessorHandleState
    ) -> stateMessage.HandleState.ValueType:
        if state == StatefulProcessorHandleState.CREATED:
            return stateMessage.CREATED
        elif state == StatefulProcessorHandleState.INITIALIZED:
            return stateMessage.INITIALIZED
        elif state == StatefulProcessorHandleState.DATA_PROCESSED:
            return stateMessage.DATA_PROCESSED
        else:
            return stateMessage.CLOSED

    def _send_proto_message(self, message: stateMessage.StateRequest) -> None:
        serialized_msg = message.SerializeToString()
        # Writing zero here to indicate message version. This allows us to evolve the message
        # format or even changing the message protocol in the future.
        write_int(0, self.sockfile)
        write_int(len(serialized_msg), self.sockfile)
        self.sockfile.write(serialized_msg)
        self.sockfile.flush()

    def _receive_proto_message(self) -> stateMessage.StateResponse:
        serialized_msg = self._receive_str()
        # proto3 will not serialize the message if the value is default, in this case 0
        if len(serialized_msg) == 0:
            return stateMessage.StateResponse(statusCode=0)
        message = stateMessage.StateResponse()
        message.ParseFromString(serialized_msg.encode("utf-8"))
        return message

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

    def _receive_and_deserialize(self):
        length = read_int(self.sockfile)
        bytes = self.sockfile.read(length)
        return self.pickleSer.loads(bytes)
