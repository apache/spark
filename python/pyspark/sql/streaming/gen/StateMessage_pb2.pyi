from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREATED: _ClassVar[HandleState]
    INITIALIZED: _ClassVar[HandleState]
    DATA_PROCESSED: _ClassVar[HandleState]
    CLOSED: _ClassVar[HandleState]
CREATED: HandleState
INITIALIZED: HandleState
DATA_PROCESSED: HandleState
CLOSED: HandleState

class StateRequest(_message.Message):
    __slots__ = ("version", "statefulProcessorHandleCall", "valueStateCall")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATEFULPROCESSORHANDLECALL_FIELD_NUMBER: _ClassVar[int]
    VALUESTATECALL_FIELD_NUMBER: _ClassVar[int]
    version: int
    statefulProcessorHandleCall: StatefulProcessorHandleCall
    valueStateCall: ValueStateCall
    def __init__(self, version: _Optional[int] = ..., statefulProcessorHandleCall: _Optional[_Union[StatefulProcessorHandleCall, _Mapping]] = ..., valueStateCall: _Optional[_Union[ValueStateCall, _Mapping]] = ...) -> None: ...

class StatefulProcessorHandleCall(_message.Message):
    __slots__ = ("setHandleState", "getValueState")
    SETHANDLESTATE_FIELD_NUMBER: _ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: _ClassVar[int]
    setHandleState: SetHandleState
    getValueState: GetValueState
    def __init__(self, setHandleState: _Optional[_Union[SetHandleState, _Mapping]] = ..., getValueState: _Optional[_Union[GetValueState, _Mapping]] = ...) -> None: ...

class GetValueState(_message.Message):
    __slots__ = ("stateName",)
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    def __init__(self, stateName: _Optional[str] = ...) -> None: ...

class ValueStateCall(_message.Message):
    __slots__ = ("exists", "get", "update")
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    exists: ValueStateExists
    get: ValueStateGet
    update: ValueStateUpdate
    def __init__(self, exists: _Optional[_Union[ValueStateExists, _Mapping]] = ..., get: _Optional[_Union[ValueStateGet, _Mapping]] = ..., update: _Optional[_Union[ValueStateUpdate, _Mapping]] = ...) -> None: ...

class ValueStateExists(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ValueStateGet(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ValueStateUpdate(_message.Message):
    __slots__ = ("updatedValue",)
    UPDATEDVALUE_FIELD_NUMBER: _ClassVar[int]
    updatedValue: bytes
    def __init__(self, updatedValue: _Optional[bytes] = ...) -> None: ...

class SetHandleState(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: HandleState
    def __init__(self, state: _Optional[_Union[HandleState, str]] = ...) -> None: ...
