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
            state_server_port: int) -> None:
        self._client_socket = socket.socket()
        self._client_socket.connect(("localhost", state_server_port))
        self.sockfile = self._client_socket.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE",
                                                                               65536)))
        print(f"client is ready - connection established")
        self.handle_state = StatefulProcessorHandleState.CREATED
        self.utf8_deserializer = UTF8Deserializer()
        # place holder, will remove when actual implementation is done
        # self.setHandleState(StatefulProcessorHandleState.CLOSED)

    def set_handle_state(self, state: StatefulProcessorHandleState) -> None:
        print(f"setting handle state to: {state}")
        proto_state = self._get_proto_state(state)
        set_handle_state = stateMessage.SetHandleState(state=proto_state)
        handle_call = stateMessage.StatefulProcessorCall(setHandleState=set_handle_state)
        message = stateMessage.StateRequest(statefulProcessorCall=handle_call)

        self._send_proto_message(message)
        status = read_int(self.sockfile)

        if (status == 0):
            self.handle_state = state
        print(f"setHandleState status= {status}")

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
        status = read_int(self.sockfile)
        print(f"getValueState status= {status}")

    def value_state_exists(self, state_name: str) -> bool:
        print(f"checking value state exists: {state_name}")
        exists_call = stateMessage.Exists(stateName=state_name)
        value_state_call = stateMessage.ValueStateCall(exists=exists_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"valueStateExists status= {status}")
        if (status == 0):
            return True
        else:
            return False

    def value_state_get(self, state_name: str) -> Any:
        print(f"getting value state: {state_name}")
        get_call = stateMessage.Get(stateName=state_name)
        value_state_call = stateMessage.ValueStateCall(get=get_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"valueStateGet status= {status}")
        if (status == 0):
            return self.utf8_deserializer.loads(self.sockfile)
        else:
            return None

    def value_state_update(self, state_name: str, schema: Union[StructType, str], value: str) -> None:
        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        print(f"updating value state: {state_name}")
        byteStr = value.encode('utf-8')
        update_call = stateMessage.Update(stateName=state_name, schema=schema.json(), value=byteStr)
        value_state_call = stateMessage.ValueStateCall(update=update_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"valueStateUpdate status= {status}")

    def value_state_clear(self, state_name: str) -> None:
        print(f"clearing value state: {state_name}")
        clear_call = stateMessage.Clear(stateName=state_name)
        value_state_call = stateMessage.ValueStateCall(clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"valueStateClear status= {status}")

    def set_implicit_key(self, key: str) -> None:
        print(f"setting implicit key: {key}")
        set_implicit_key = stateMessage.SetImplicitKey(key=key)
        request = stateMessage.ImplicitGroupingKeyRequest(setImplicitKey=set_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"setImplicitKey status= {status}")

    def remove_implicit_key(self) -> None:
        print(f"removing implicit key")
        remove_implicit_key = stateMessage.RemoveImplicitKey()
        request = stateMessage.ImplicitGroupingKeyRequest(removeImplicitKey=remove_implicit_key)
        message = stateMessage.StateRequest(implicitGroupingKeyRequest=request)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"removeImplicitKey status= {status}")

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
