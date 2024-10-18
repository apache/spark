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
    def __init__(self, setImplicitKey: Optional[Union[SetImplicitKey, Mapping]] = ..., removeImplicitKey: Optional[Union[RemoveImplicitKey, Mapping]] = ...) -> None: ...

class IsFirstBatch(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListStateCall(_message.Message):
    __slots__ = ["appendList", "appendValue", "clear", "exists", "listStateGet", "listStatePut", "stateName"]
    APPENDLIST_FIELD_NUMBER: ClassVar[int]
    APPENDVALUE_FIELD_NUMBER: ClassVar[int]
    CLEAR_FIELD_NUMBER: ClassVar[int]
    EXISTS_FIELD_NUMBER: ClassVar[int]
    LISTSTATEGET_FIELD_NUMBER: ClassVar[int]
    LISTSTATEPUT_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    appendList: AppendList
    appendValue: AppendValue
    clear: Clear
    exists: Exists
    listStateGet: ListStateGet
    listStatePut: ListStatePut
    stateName: str
    def __init__(self, stateName: Optional[str] = ..., exists: Optional[Union[Exists, Mapping]] = ..., listStateGet: Optional[Union[ListStateGet, Mapping]] = ..., listStatePut: Optional[Union[ListStatePut, Mapping]] = ..., appendValue: Optional[Union[AppendValue, Mapping]] = ..., appendList: Optional[Union[AppendList, Mapping]] = ..., clear: Optional[Union[Clear, Mapping]] = ...) -> None: ...

class ListStateGet(_message.Message):
    __slots__ = ["iteratorId"]
    ITERATORID_FIELD_NUMBER: ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: Optional[str] = ...) -> None: ...

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
    __slots__ = ["schema", "stateName", "ttl"]
    SCHEMA_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    TTL_FIELD_NUMBER: ClassVar[int]
    schema: str
    stateName: str
    ttl: TTLConfig
    def __init__(self, stateName: Optional[str] = ..., schema: Optional[str] = ..., ttl: Optional[Union[TTLConfig, Mapping]] = ...) -> None: ...

class StateRequest(_message.Message):
    __slots__ = ["implicitGroupingKeyRequest", "stateVariableRequest", "statefulProcessorCall", "version"]
    IMPLICITGROUPINGKEYREQUEST_FIELD_NUMBER: ClassVar[int]
    STATEFULPROCESSORCALL_FIELD_NUMBER: ClassVar[int]
    STATEVARIABLEREQUEST_FIELD_NUMBER: ClassVar[int]
    VERSION_FIELD_NUMBER: ClassVar[int]
    implicitGroupingKeyRequest: ImplicitGroupingKeyRequest
    stateVariableRequest: StateVariableRequest
    statefulProcessorCall: StatefulProcessorCall
    version: int
    def __init__(self, version: Optional[int] = ..., statefulProcessorCall: Optional[Union[StatefulProcessorCall, Mapping]] = ..., stateVariableRequest: Optional[Union[StateVariableRequest, Mapping]] = ..., implicitGroupingKeyRequest: Optional[Union[ImplicitGroupingKeyRequest, Mapping]] = ...) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ["errorMessage", "statusCode", "value"]
    ERRORMESSAGE_FIELD_NUMBER: ClassVar[int]
    STATUSCODE_FIELD_NUMBER: ClassVar[int]
    VALUE_FIELD_NUMBER: ClassVar[int]
    errorMessage: str
    statusCode: int
    value: bytes
    def __init__(self, statusCode: Optional[int] = ..., errorMessage: Optional[str] = ..., value: Optional[bytes] = ...) -> None: ...

class StateVariableRequest(_message.Message):
    __slots__ = ["listStateCall", "valueStateCall"]
    LISTSTATECALL_FIELD_NUMBER: ClassVar[int]
    VALUESTATECALL_FIELD_NUMBER: ClassVar[int]
    listStateCall: ListStateCall
    valueStateCall: ValueStateCall
    def __init__(self, valueStateCall: Optional[Union[ValueStateCall, Mapping]] = ..., listStateCall: Optional[Union[ListStateCall, Mapping]] = ...) -> None: ...

class StatefulProcessorCall(_message.Message):
    __slots__ = ["getListState", "getMapState", "getValueState", "setHandleState", "utilsCall"]
    GETLISTSTATE_FIELD_NUMBER: ClassVar[int]
    GETMAPSTATE_FIELD_NUMBER: ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: ClassVar[int]
    SETHANDLESTATE_FIELD_NUMBER: ClassVar[int]
    UTILSCALL_FIELD_NUMBER: ClassVar[int]
    getListState: StateCallCommand
    getMapState: StateCallCommand
    getValueState: StateCallCommand
    setHandleState: SetHandleState
    utilsCall: UtilsCallCommand
    def __init__(self, setHandleState: Optional[Union[SetHandleState, Mapping]] = ..., getValueState: Optional[Union[StateCallCommand, Mapping]] = ..., getListState: Optional[Union[StateCallCommand, Mapping]] = ..., getMapState: Optional[Union[StateCallCommand, Mapping]] = ..., utilsCall: Optional[Union[UtilsCallCommand, Mapping]] = ...) -> None: ...

class TTLConfig(_message.Message):
    __slots__ = ["durationMs"]
    DURATIONMS_FIELD_NUMBER: ClassVar[int]
    durationMs: int
    def __init__(self, durationMs: Optional[int] = ...) -> None: ...

class UtilsCallCommand(_message.Message):
    __slots__ = ["isFirstBatch"]
    ISFIRSTBATCH_FIELD_NUMBER: ClassVar[int]
    isFirstBatch: IsFirstBatch
    def __init__(self, isFirstBatch: Optional[Union[IsFirstBatch, Mapping]] = ...) -> None: ...

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
    def __init__(self, stateName: Optional[str] = ..., exists: Optional[Union[Exists, Mapping]] = ..., get: Optional[Union[Get, Mapping]] = ..., valueStateUpdate: Optional[Union[ValueStateUpdate, Mapping]] = ..., clear: Optional[Union[Clear, Mapping]] = ...) -> None: ...

class ValueStateUpdate(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: ClassVar[int]
    value: bytes
    def __init__(self, value: Optional[bytes] = ...) -> None: ...

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
