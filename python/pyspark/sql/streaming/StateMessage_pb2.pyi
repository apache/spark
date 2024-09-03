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

from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar, Mapping, Optional, Union

CLOSED: HandleState
CREATED: HandleState
DATA_PROCESSED: HandleState
DESCRIPTOR: _descriptor.FileDescriptor
INITIALIZED: HandleState

class AppendList(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class AppendValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: ClassVar[int]
    value: bytes
    def __init__(self, value: Optional[bytes] = ...) -> None: ...

class Clear(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Exists(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Get(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ImplicitGroupingKeyRequest(_message.Message):
    __slots__ = ["removeImplicitKey", "setImplicitKey"]
    REMOVEIMPLICITKEY_FIELD_NUMBER: ClassVar[int]
    SETIMPLICITKEY_FIELD_NUMBER: ClassVar[int]
    removeImplicitKey: RemoveImplicitKey
    setImplicitKey: SetImplicitKey
    def __init__(
        self,
        setImplicitKey: Optional[Union[SetImplicitKey, Mapping]] = ...,
        removeImplicitKey: Optional[Union[RemoveImplicitKey, Mapping]] = ...,
    ) -> None: ...

class ListStateCall(_message.Message):
    __slots__ = ["appendList", "appendValue", "clear", "exists", "get", "listStatePut", "stateName"]
    APPENDLIST_FIELD_NUMBER: ClassVar[int]
    APPENDVALUE_FIELD_NUMBER: ClassVar[int]
    CLEAR_FIELD_NUMBER: ClassVar[int]
    EXISTS_FIELD_NUMBER: ClassVar[int]
    GET_FIELD_NUMBER: ClassVar[int]
    LISTSTATEPUT_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    appendList: AppendList
    appendValue: AppendValue
    clear: Clear
    exists: Exists
    get: Get
    listStatePut: ListStatePut
    stateName: str
    def __init__(
        self,
        stateName: Optional[str] = ...,
        exists: Optional[Union[Exists, Mapping]] = ...,
        get: Optional[Union[Get, Mapping]] = ...,
        listStatePut: Optional[Union[ListStatePut, Mapping]] = ...,
        appendValue: Optional[Union[AppendValue, Mapping]] = ...,
        appendList: Optional[Union[AppendList, Mapping]] = ...,
        clear: Optional[Union[Clear, Mapping]] = ...,
    ) -> None: ...

class ListStatePut(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RemoveImplicitKey(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class SetHandleState(_message.Message):
    __slots__ = ["state"]
    STATE_FIELD_NUMBER: ClassVar[int]
    state: HandleState
    def __init__(self, state: Optional[Union[HandleState, str]] = ...) -> None: ...

class SetImplicitKey(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: ClassVar[int]
    key: bytes
    def __init__(self, key: Optional[bytes] = ...) -> None: ...

class StateCallCommand(_message.Message):
    __slots__ = ["schema", "stateName"]
    SCHEMA_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    schema: str
    stateName: str
    def __init__(self, stateName: Optional[str] = ..., schema: Optional[str] = ...) -> None: ...

class StateRequest(_message.Message):
    __slots__ = [
        "implicitGroupingKeyRequest",
        "stateVariableRequest",
        "statefulProcessorCall",
        "version",
    ]
    IMPLICITGROUPINGKEYREQUEST_FIELD_NUMBER: ClassVar[int]
    STATEFULPROCESSORCALL_FIELD_NUMBER: ClassVar[int]
    STATEVARIABLEREQUEST_FIELD_NUMBER: ClassVar[int]
    VERSION_FIELD_NUMBER: ClassVar[int]
    implicitGroupingKeyRequest: ImplicitGroupingKeyRequest
    stateVariableRequest: StateVariableRequest
    statefulProcessorCall: StatefulProcessorCall
    version: int
    def __init__(
        self,
        version: Optional[int] = ...,
        statefulProcessorCall: Optional[Union[StatefulProcessorCall, Mapping]] = ...,
        stateVariableRequest: Optional[Union[StateVariableRequest, Mapping]] = ...,
        implicitGroupingKeyRequest: Optional[Union[ImplicitGroupingKeyRequest, Mapping]] = ...,
    ) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ["errorMessage", "statusCode", "value"]
    ERRORMESSAGE_FIELD_NUMBER: ClassVar[int]
    STATUSCODE_FIELD_NUMBER: ClassVar[int]
    VALUE_FIELD_NUMBER: ClassVar[int]
    errorMessage: str
    statusCode: int
    value: bytes
    def __init__(
        self,
        statusCode: Optional[int] = ...,
        errorMessage: Optional[str] = ...,
        value: Optional[bytes] = ...,
    ) -> None: ...

class StateVariableRequest(_message.Message):
    __slots__ = ["listStateCall", "valueStateCall"]
    LISTSTATECALL_FIELD_NUMBER: ClassVar[int]
    VALUESTATECALL_FIELD_NUMBER: ClassVar[int]
    listStateCall: ListStateCall
    valueStateCall: ValueStateCall
    def __init__(
        self,
        valueStateCall: Optional[Union[ValueStateCall, Mapping]] = ...,
        listStateCall: Optional[Union[ListStateCall, Mapping]] = ...,
    ) -> None: ...

class StatefulProcessorCall(_message.Message):
    __slots__ = ["getListState", "getMapState", "getValueState", "setHandleState"]
    GETLISTSTATE_FIELD_NUMBER: ClassVar[int]
    GETMAPSTATE_FIELD_NUMBER: ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: ClassVar[int]
    SETHANDLESTATE_FIELD_NUMBER: ClassVar[int]
    getListState: StateCallCommand
    getMapState: StateCallCommand
    getValueState: StateCallCommand
    setHandleState: SetHandleState
    def __init__(
        self,
        setHandleState: Optional[Union[SetHandleState, Mapping]] = ...,
        getValueState: Optional[Union[StateCallCommand, Mapping]] = ...,
        getListState: Optional[Union[StateCallCommand, Mapping]] = ...,
        getMapState: Optional[Union[StateCallCommand, Mapping]] = ...,
    ) -> None: ...

class ValueStateCall(_message.Message):
    __slots__ = ["clear", "exists", "get", "stateName", "valueStateUpdate"]
    CLEAR_FIELD_NUMBER: ClassVar[int]
    EXISTS_FIELD_NUMBER: ClassVar[int]
    GET_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    VALUESTATEUPDATE_FIELD_NUMBER: ClassVar[int]
    clear: Clear
    exists: Exists
    get: Get
    stateName: str
    valueStateUpdate: ValueStateUpdate
    def __init__(
        self,
        stateName: Optional[str] = ...,
        exists: Optional[Union[Exists, Mapping]] = ...,
        get: Optional[Union[Get, Mapping]] = ...,
        valueStateUpdate: Optional[Union[ValueStateUpdate, Mapping]] = ...,
        clear: Optional[Union[Clear, Mapping]] = ...,
    ) -> None: ...

class ValueStateUpdate(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: ClassVar[int]
    value: bytes
    def __init__(self, value: Optional[bytes] = ...) -> None: ...

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
