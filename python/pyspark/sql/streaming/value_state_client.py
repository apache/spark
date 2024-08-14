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

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.types import StructType, _parse_datatype_string
from pyspark.errors import PySparkRuntimeError

__all__ = ["ValueStateClient"]


class ValueStateClient:
    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client

    def exists(self, state_name: str) -> bool:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        exists_call = stateMessage.Exists()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, exists=exists_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status == 0:
            return True
        elif status == 1 and "doesn't exist" in response_message[1]:
            return False
        else:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "exists()",
                    "object": "ValueState",
                },
            )

    def get(self, state_name: str) -> Any:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_call = stateMessage.Get()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, get=get_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
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
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "get()",
                    "object": "ValueState",
                },
            )

    def update(self, state_name: str, schema: Union[StructType, str], value: Tuple) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        bytes = self._stateful_processor_api_client._serialize_to_bytes(schema, value)
        update_call = stateMessage.ValueStateUpdate(value=bytes)
        value_state_call = stateMessage.ValueStateCall(
            stateName=state_name, valueStateUpdate=update_call
        )
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "update()",
                    "object": "ValueState",
                },
            )

    def clear(self, state_name: str) -> None:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        clear_call = stateMessage.Clear()
        value_state_call = stateMessage.ValueStateCall(stateName=state_name, clear=clear_call)
        state_variable_request = stateMessage.StateVariableRequest(valueStateCall=value_state_call)
        message = stateMessage.StateRequest(stateVariableRequest=state_variable_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "clear()",
                    "object": "ValueState",
                },
            )
