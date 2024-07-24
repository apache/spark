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

from typing import Any, Union, cast, Tuple

from pyspark.sql.streaming.state_api_client import StateApiClient
import pyspark.sql.streaming.StateMessage_pb2 as stateMessage
from pyspark.sql.types import Row, StructType, _parse_datatype_string
from pyspark.errors import PySparkRuntimeError


class ValueStateClient:
    def __init__(
            self,
            state_api_client: StateApiClient, ) -> None:
        self._state_api_client = state_api_client

    def exists(self, state_name: str) -> bool:
        exists_call = stateMessage.Exists()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, exists=exists_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._state_api_client._send_proto_message(message)
        response_message = self._state_api_client._receive_proto_message()
        status = response_message.statusCode
        if (status == 0):
            return True
        elif (status == 1):
            # server returns 1 if the state does not exist
            return False
        else:
            raise PySparkRuntimeError(f"Error checking value state exists: "
                                      f"{response_message.errorMessage}")

    def get(self, state_name: str) -> Any:
        get_call = stateMessage.Get()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, get=get_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._state_api_client._send_proto_message(message)
        response_message = self._state_api_client._receive_proto_message()
        status = response_message.statusCode
        if (status == 0):
            return self._state_api_client._receive_and_deserialize()
        else:
            raise PySparkRuntimeError(f"Error getting value state: {response_message.errorMessage}")

    def update(self, state_name: str, schema: Union[StructType, str], value: Tuple) -> None:
        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        bytes = self._state_api_client._serialize_to_bytes(schema, value)
        update_call = stateMessage.ValueStateUpdate(schema=schema.json(), value=bytes)
        value_state_call = stateMessage.ValueStateCall(stateName=state_name,
                                                       valueStateUpdate=update_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._state_api_client._send_proto_message(message)
        response_message = self._state_api_client._receive_proto_message()
        status = response_message.statusCode
        if (status != 0):
            raise PySparkRuntimeError(f"Error updating value state: "
                                      f"{response_message.errorMessage}")

    def clear(self, state_name: str) -> None:
        clear_call = stateMessage.Clear()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._state_api_client._send_proto_message(message)
        response_message = self._state_api_client._receive_proto_message()
        status = response_message.statusCode
        if (status != 0):
            raise PySparkRuntimeError(f"Error clearing value state: "
                                      f"{response_message.errorMessage}")
