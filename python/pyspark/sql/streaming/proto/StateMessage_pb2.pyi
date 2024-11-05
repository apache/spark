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
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREATED: _ClassVar[HandleState]
    INITIALIZED: _ClassVar[HandleState]
    DATA_PROCESSED: _ClassVar[HandleState]
    TIMER_PROCESSED: _ClassVar[HandleState]
    CLOSED: _ClassVar[HandleState]

CREATED: HandleState
INITIALIZED: HandleState
DATA_PROCESSED: HandleState
TIMER_PROCESSED: HandleState
CLOSED: HandleState

class StateRequest(_message.Message):
    __slots__ = (
        "version",
        "statefulProcessorCall",
        "stateVariableRequest",
        "implicitGroupingKeyRequest",
        "timerRequest",
    )
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATEFULPROCESSORCALL_FIELD_NUMBER: _ClassVar[int]
    STATEVARIABLEREQUEST_FIELD_NUMBER: _ClassVar[int]
    IMPLICITGROUPINGKEYREQUEST_FIELD_NUMBER: _ClassVar[int]
    TIMERREQUEST_FIELD_NUMBER: _ClassVar[int]
    version: int
    statefulProcessorCall: StatefulProcessorCall
    stateVariableRequest: StateVariableRequest
    implicitGroupingKeyRequest: ImplicitGroupingKeyRequest
    timerRequest: TimerRequest
    def __init__(
        self,
        version: _Optional[int] = ...,
        statefulProcessorCall: _Optional[_Union[StatefulProcessorCall, _Mapping]] = ...,
        stateVariableRequest: _Optional[_Union[StateVariableRequest, _Mapping]] = ...,
        implicitGroupingKeyRequest: _Optional[_Union[ImplicitGroupingKeyRequest, _Mapping]] = ...,
        timerRequest: _Optional[_Union[TimerRequest, _Mapping]] = ...,
    ) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ("statusCode", "errorMessage", "value")
    STATUSCODE_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    statusCode: int
    errorMessage: str
    value: bytes
    def __init__(
        self,
        statusCode: _Optional[int] = ...,
        errorMessage: _Optional[str] = ...,
        value: _Optional[bytes] = ...,
    ) -> None: ...

class StateResponseWithLongTypeVal(_message.Message):
    __slots__ = ("statusCode", "errorMessage", "value")
    STATUSCODE_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    statusCode: int
    errorMessage: str
    value: int
    def __init__(
        self,
        statusCode: _Optional[int] = ...,
        errorMessage: _Optional[str] = ...,
        value: _Optional[int] = ...,
    ) -> None: ...

class StatefulProcessorCall(_message.Message):
    __slots__ = ("setHandleState", "getValueState", "getListState", "getMapState", "timerStateCall")
    SETHANDLESTATE_FIELD_NUMBER: _ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: _ClassVar[int]
    GETLISTSTATE_FIELD_NUMBER: _ClassVar[int]
    GETMAPSTATE_FIELD_NUMBER: _ClassVar[int]
    TIMERSTATECALL_FIELD_NUMBER: _ClassVar[int]
    setHandleState: SetHandleState
    getValueState: StateCallCommand
    getListState: StateCallCommand
    getMapState: StateCallCommand
    timerStateCall: TimerStateCallCommand
    def __init__(
        self,
        setHandleState: _Optional[_Union[SetHandleState, _Mapping]] = ...,
        getValueState: _Optional[_Union[StateCallCommand, _Mapping]] = ...,
        getListState: _Optional[_Union[StateCallCommand, _Mapping]] = ...,
        getMapState: _Optional[_Union[StateCallCommand, _Mapping]] = ...,
        timerStateCall: _Optional[_Union[TimerStateCallCommand, _Mapping]] = ...,
    ) -> None: ...

class StateVariableRequest(_message.Message):
    __slots__ = ("valueStateCall", "listStateCall", "mapStateCall")
    VALUESTATECALL_FIELD_NUMBER: _ClassVar[int]
    LISTSTATECALL_FIELD_NUMBER: _ClassVar[int]
    MAPSTATECALL_FIELD_NUMBER: _ClassVar[int]
    valueStateCall: ValueStateCall
    listStateCall: ListStateCall
    mapStateCall: MapStateCall
    def __init__(
        self,
        valueStateCall: _Optional[_Union[ValueStateCall, _Mapping]] = ...,
        listStateCall: _Optional[_Union[ListStateCall, _Mapping]] = ...,
        mapStateCall: _Optional[_Union[MapStateCall, _Mapping]] = ...,
    ) -> None: ...

class ImplicitGroupingKeyRequest(_message.Message):
    __slots__ = ("setImplicitKey", "removeImplicitKey")
    SETIMPLICITKEY_FIELD_NUMBER: _ClassVar[int]
    REMOVEIMPLICITKEY_FIELD_NUMBER: _ClassVar[int]
    setImplicitKey: SetImplicitKey
    removeImplicitKey: RemoveImplicitKey
    def __init__(
        self,
        setImplicitKey: _Optional[_Union[SetImplicitKey, _Mapping]] = ...,
        removeImplicitKey: _Optional[_Union[RemoveImplicitKey, _Mapping]] = ...,
    ) -> None: ...

class TimerRequest(_message.Message):
    __slots__ = ("timerValueRequest", "expiryTimerRequest")
    TIMERVALUEREQUEST_FIELD_NUMBER: _ClassVar[int]
    EXPIRYTIMERREQUEST_FIELD_NUMBER: _ClassVar[int]
    timerValueRequest: TimerValueRequest
    expiryTimerRequest: ExpiryTimerRequest
    def __init__(
        self,
        timerValueRequest: _Optional[_Union[TimerValueRequest, _Mapping]] = ...,
        expiryTimerRequest: _Optional[_Union[ExpiryTimerRequest, _Mapping]] = ...,
    ) -> None: ...

class TimerValueRequest(_message.Message):
    __slots__ = ("getProcessingTimer", "getWatermark")
    GETPROCESSINGTIMER_FIELD_NUMBER: _ClassVar[int]
    GETWATERMARK_FIELD_NUMBER: _ClassVar[int]
    getProcessingTimer: GetProcessingTime
    getWatermark: GetWatermark
    def __init__(
        self,
        getProcessingTimer: _Optional[_Union[GetProcessingTime, _Mapping]] = ...,
        getWatermark: _Optional[_Union[GetWatermark, _Mapping]] = ...,
    ) -> None: ...

class ExpiryTimerRequest(_message.Message):
    __slots__ = ("expiryTimestampMs",)
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: _ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: _Optional[int] = ...) -> None: ...

class GetProcessingTime(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetWatermark(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StateCallCommand(_message.Message):
    __slots__ = ("stateName", "schema", "mapStateValueSchema", "ttl")
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    MAPSTATEVALUESCHEMA_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    schema: str
    mapStateValueSchema: str
    ttl: TTLConfig
    def __init__(
        self,
        stateName: _Optional[str] = ...,
        schema: _Optional[str] = ...,
        mapStateValueSchema: _Optional[str] = ...,
        ttl: _Optional[_Union[TTLConfig, _Mapping]] = ...,
    ) -> None: ...

class TimerStateCallCommand(_message.Message):
    __slots__ = ("register", "delete", "list")
    REGISTER_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    register: RegisterTimer
    delete: DeleteTimer
    list: ListTimers
    def __init__(
        self,
        register: _Optional[_Union[RegisterTimer, _Mapping]] = ...,
        delete: _Optional[_Union[DeleteTimer, _Mapping]] = ...,
        list: _Optional[_Union[ListTimers, _Mapping]] = ...,
    ) -> None: ...

class ValueStateCall(_message.Message):
    __slots__ = ("stateName", "exists", "get", "valueStateUpdate", "clear")
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    VALUESTATEUPDATE_FIELD_NUMBER: _ClassVar[int]
    CLEAR_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    exists: Exists
    get: Get
    valueStateUpdate: ValueStateUpdate
    clear: Clear
    def __init__(
        self,
        stateName: _Optional[str] = ...,
        exists: _Optional[_Union[Exists, _Mapping]] = ...,
        get: _Optional[_Union[Get, _Mapping]] = ...,
        valueStateUpdate: _Optional[_Union[ValueStateUpdate, _Mapping]] = ...,
        clear: _Optional[_Union[Clear, _Mapping]] = ...,
    ) -> None: ...

class ListStateCall(_message.Message):
    __slots__ = (
        "stateName",
        "exists",
        "listStateGet",
        "listStatePut",
        "appendValue",
        "appendList",
        "clear",
    )
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    LISTSTATEGET_FIELD_NUMBER: _ClassVar[int]
    LISTSTATEPUT_FIELD_NUMBER: _ClassVar[int]
    APPENDVALUE_FIELD_NUMBER: _ClassVar[int]
    APPENDLIST_FIELD_NUMBER: _ClassVar[int]
    CLEAR_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    exists: Exists
    listStateGet: ListStateGet
    listStatePut: ListStatePut
    appendValue: AppendValue
    appendList: AppendList
    clear: Clear
    def __init__(
        self,
        stateName: _Optional[str] = ...,
        exists: _Optional[_Union[Exists, _Mapping]] = ...,
        listStateGet: _Optional[_Union[ListStateGet, _Mapping]] = ...,
        listStatePut: _Optional[_Union[ListStatePut, _Mapping]] = ...,
        appendValue: _Optional[_Union[AppendValue, _Mapping]] = ...,
        appendList: _Optional[_Union[AppendList, _Mapping]] = ...,
        clear: _Optional[_Union[Clear, _Mapping]] = ...,
    ) -> None: ...

class MapStateCall(_message.Message):
    __slots__ = (
        "stateName",
        "exists",
        "getValue",
        "containsKey",
        "updateValue",
        "iterator",
        "keys",
        "values",
        "removeKey",
        "clear",
    )
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    GETVALUE_FIELD_NUMBER: _ClassVar[int]
    CONTAINSKEY_FIELD_NUMBER: _ClassVar[int]
    UPDATEVALUE_FIELD_NUMBER: _ClassVar[int]
    ITERATOR_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    REMOVEKEY_FIELD_NUMBER: _ClassVar[int]
    CLEAR_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    exists: Exists
    getValue: GetValue
    containsKey: ContainsKey
    updateValue: UpdateValue
    iterator: Iterator
    keys: Keys
    values: Values
    removeKey: RemoveKey
    clear: Clear
    def __init__(
        self,
        stateName: _Optional[str] = ...,
        exists: _Optional[_Union[Exists, _Mapping]] = ...,
        getValue: _Optional[_Union[GetValue, _Mapping]] = ...,
        containsKey: _Optional[_Union[ContainsKey, _Mapping]] = ...,
        updateValue: _Optional[_Union[UpdateValue, _Mapping]] = ...,
        iterator: _Optional[_Union[Iterator, _Mapping]] = ...,
        keys: _Optional[_Union[Keys, _Mapping]] = ...,
        values: _Optional[_Union[Values, _Mapping]] = ...,
        removeKey: _Optional[_Union[RemoveKey, _Mapping]] = ...,
        clear: _Optional[_Union[Clear, _Mapping]] = ...,
    ) -> None: ...

class SetImplicitKey(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    def __init__(self, key: _Optional[bytes] = ...) -> None: ...

class RemoveImplicitKey(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Exists(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Get(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RegisterTimer(_message.Message):
    __slots__ = ("expiryTimestampMs",)
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: _ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: _Optional[int] = ...) -> None: ...

class DeleteTimer(_message.Message):
    __slots__ = ("expiryTimestampMs",)
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: _ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: _Optional[int] = ...) -> None: ...

class ListTimers(_message.Message):
    __slots__ = ("iteratorId",)
    ITERATORID_FIELD_NUMBER: _ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: _Optional[str] = ...) -> None: ...

class ValueStateUpdate(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class Clear(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListStateGet(_message.Message):
    __slots__ = ("iteratorId",)
    ITERATORID_FIELD_NUMBER: _ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: _Optional[str] = ...) -> None: ...

class ListStatePut(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AppendValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class AppendList(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetValue(_message.Message):
    __slots__ = ("userKey",)
    USERKEY_FIELD_NUMBER: _ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: _Optional[bytes] = ...) -> None: ...

class ContainsKey(_message.Message):
    __slots__ = ("userKey",)
    USERKEY_FIELD_NUMBER: _ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: _Optional[bytes] = ...) -> None: ...

class UpdateValue(_message.Message):
    __slots__ = ("userKey", "value")
    USERKEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    userKey: bytes
    value: bytes
    def __init__(self, userKey: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...

class Iterator(_message.Message):
    __slots__ = ("iteratorId",)
    ITERATORID_FIELD_NUMBER: _ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: _Optional[str] = ...) -> None: ...

class Keys(_message.Message):
    __slots__ = ("iteratorId",)
    ITERATORID_FIELD_NUMBER: _ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: _Optional[str] = ...) -> None: ...

class Values(_message.Message):
    __slots__ = ("iteratorId",)
    ITERATORID_FIELD_NUMBER: _ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: _Optional[str] = ...) -> None: ...

class RemoveKey(_message.Message):
    __slots__ = ("userKey",)
    USERKEY_FIELD_NUMBER: _ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: _Optional[bytes] = ...) -> None: ...

class SetHandleState(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: HandleState
    def __init__(self, state: _Optional[_Union[HandleState, str]] = ...) -> None: ...

class TTLConfig(_message.Message):
    __slots__ = ("durationMs",)
    DURATIONMS_FIELD_NUMBER: _ClassVar[int]
    durationMs: int
    def __init__(self, durationMs: _Optional[int] = ...) -> None: ...
