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
from typing import Any, Union, cast

import pyspark.sql.streaming.StateMessage_pb2 as stateMessage
from pyspark.serializers import write_int, read_int, UTF8Deserializer
from pyspark.sql.types import StructType, _parse_datatype_string


class StatefulProcessorHandleState(Enum):
    CREATED = 1
    INITIALIZED = 2
    DATA_PROCESSED = 3
    CLOSED = 4


class StateApiClient:
    def __init__(
            self,
            state_server_id: int) -> None:
        self._client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_address = f'./uds_{state_server_id}.sock'
        self._client_socket.connect(server_address)
        self.sockfile = self._client_socket.makefile("rwb",
                                                     int(os.environ.get("SPARK_BUFFER_SIZE",65536)))
        print(f"client is ready - connection established")
        self.handle_state = StatefulProcessorHandleState.CREATED
        self.utf8_deserializer = UTF8Deserializer()

    def set_handle_state(self, state: StatefulProcessorHandleState) -> None:
        print(f"setting handle state to: {state}")
        proto_state = self._get_proto_state(state)
        set_handle_state = stateMessage.SetHandleState(state=proto_state)
        handle_call = stateMessage.StatefulProcessorCall(setHandleState=set_handle_state)
        message = stateMessage.StateRequest(statefulProcessorCall=handle_call)

        self._send_proto_message(message)

        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if (status == 0):
            self.handle_state = state
        else:
            raise Exception(f"Error setting handle state: {response_message.errorMessage}")
        print(f"setHandleState status= {status}")

    def set_implicit_key(self, key: str) -> None:
        print(f"setting implicit key: {key}")
        set_implicit_key = stateMessage.SetImplicitKey(key=key)
        request = stateMessage.ImplicitGroupingKeyRequest(setImplicitKey=set_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        print(f"setImplicitKey status= {status}")
        if (status != 0):
            raise Exception(f"Error setting implicit key: {response_message.errorMessage}")

    def remove_implicit_key(self) -> None:
        print(f"removing implicit key")
        remove_implicit_key = stateMessage.RemoveImplicitKey()
        request = stateMessage.ImplicitGroupingKeyRequest(removeImplicitKey=remove_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        print(f"removeImplicitKey status= {status}")
        if (status != 0):
            raise Exception(f"Error removing implicit key: {response_message.errorMessage}")

    def get_value_state(self, state_name: str, schema: Union[StructType, str]) -> None:
        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))

        print(f"initializing value state: {state_name}")

        state_call_command = stateMessage.StateCallCommand()
        state_call_command.stateName = state_name
        state_call_command.schema = schema.json()
        call = stateMessage.StatefulProcessorCall(getValueState=state_call_command)
        message = stateMessage.StateRequest(statefulProcessorCall=call)

        self._send_proto_message(message)
        response_message = self._receive_proto_message()
        status = response_message.statusCode
        if (status != 0):
            raise Exception(f"Error initializing value state: {response_message.errorMessage}")
        print(f"getValueState status= {status}")

    def _get_proto_state(self,
                         state: StatefulProcessorHandleState) -> stateMessage.HandleState.ValueType:
        if (state == StatefulProcessorHandleState.CREATED):
            return stateMessage.CREATED
        elif (state == StatefulProcessorHandleState.INITIALIZED):
            return stateMessage.INITIALIZED
        elif (state == StatefulProcessorHandleState.DATA_PROCESSED):
            return stateMessage.DATA_PROCESSED
        else:
            return stateMessage.CLOSED

    def _send_proto_message(self, message: stateMessage.StateRequest) -> None:
        serialized_msg = message.SerializeToString()
        print(f"sending message -- len = {len(serialized_msg)} {str(serialized_msg)}")
        write_int(0, self.sockfile)
        write_int(len(serialized_msg), self.sockfile)
        self.sockfile.write(serialized_msg)
        self.sockfile.flush()

    def _receive_proto_message(self) -> stateMessage.StateResponse:
        serialized_msg = self._receive_str()
        print(f"received response message -- len = {len(serialized_msg)} {str(serialized_msg)}")
        # proto3 will not serialize the message if the value is default, in this case 0
        if (len(serialized_msg) == 0):
            return stateMessage.StateResponse(statusCode=0)
        message = stateMessage.StateResponse()
        message.ParseFromString(serialized_msg.encode('utf-8'))
        print(f"received response message -- status = {str(message.statusCode)},"
              f" message = {message.errorMessage}")
        return message

    def _receive_str(self) -> str:
        return self.utf8_deserializer.loads(self.sockfile)
