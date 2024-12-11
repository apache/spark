from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar, Mapping, Optional, Union

CLOSED: HandleState
CREATED: HandleState
DATA_PROCESSED: HandleState
DESCRIPTOR: _descriptor.FileDescriptor
INITIALIZED: HandleState
PRE_INIT: HandleState
TIMER_PROCESSED: HandleState

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

class ContainsKey(_message.Message):
    __slots__ = ["userKey"]
    USERKEY_FIELD_NUMBER: ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: Optional[bytes] = ...) -> None: ...

class DeleteTimer(_message.Message):
    __slots__ = ["expiryTimestampMs"]
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: Optional[int] = ...) -> None: ...

class Exists(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ExpiryTimerRequest(_message.Message):
    __slots__ = ["expiryTimestampMs"]
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: Optional[int] = ...) -> None: ...

class Get(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetProcessingTime(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetValue(_message.Message):
    __slots__ = ["userKey"]
    USERKEY_FIELD_NUMBER: ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: Optional[bytes] = ...) -> None: ...

class GetWatermark(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ImplicitGroupingKeyRequest(_message.Message):
    __slots__ = ["removeImplicitKey", "setImplicitKey"]
    REMOVEIMPLICITKEY_FIELD_NUMBER: ClassVar[int]
    SETIMPLICITKEY_FIELD_NUMBER: ClassVar[int]
    removeImplicitKey: RemoveImplicitKey
    setImplicitKey: SetImplicitKey
    def __init__(self, setImplicitKey: Optional[Union[SetImplicitKey, Mapping]] = ..., removeImplicitKey: Optional[Union[RemoveImplicitKey, Mapping]] = ...) -> None: ...

class Iterator(_message.Message):
    __slots__ = ["iteratorId"]
    ITERATORID_FIELD_NUMBER: ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: Optional[str] = ...) -> None: ...

class Keys(_message.Message):
    __slots__ = ["iteratorId"]
    ITERATORID_FIELD_NUMBER: ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: Optional[str] = ...) -> None: ...

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

class ListTimers(_message.Message):
    __slots__ = ["iteratorId"]
    ITERATORID_FIELD_NUMBER: ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: Optional[str] = ...) -> None: ...

class MapStateCall(_message.Message):
    __slots__ = ["clear", "containsKey", "exists", "getValue", "iterator", "keys", "removeKey", "stateName", "updateValue", "values"]
    CLEAR_FIELD_NUMBER: ClassVar[int]
    CONTAINSKEY_FIELD_NUMBER: ClassVar[int]
    EXISTS_FIELD_NUMBER: ClassVar[int]
    GETVALUE_FIELD_NUMBER: ClassVar[int]
    ITERATOR_FIELD_NUMBER: ClassVar[int]
    KEYS_FIELD_NUMBER: ClassVar[int]
    REMOVEKEY_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    UPDATEVALUE_FIELD_NUMBER: ClassVar[int]
    VALUES_FIELD_NUMBER: ClassVar[int]
    clear: Clear
    containsKey: ContainsKey
    exists: Exists
    getValue: GetValue
    iterator: Iterator
    keys: Keys
    removeKey: RemoveKey
    stateName: str
    updateValue: UpdateValue
    values: Values
    def __init__(self, stateName: Optional[str] = ..., exists: Optional[Union[Exists, Mapping]] = ..., getValue: Optional[Union[GetValue, Mapping]] = ..., containsKey: Optional[Union[ContainsKey, Mapping]] = ..., updateValue: Optional[Union[UpdateValue, Mapping]] = ..., iterator: Optional[Union[Iterator, Mapping]] = ..., keys: Optional[Union[Keys, Mapping]] = ..., values: Optional[Union[Values, Mapping]] = ..., removeKey: Optional[Union[RemoveKey, Mapping]] = ..., clear: Optional[Union[Clear, Mapping]] = ...) -> None: ...

class RegisterTimer(_message.Message):
    __slots__ = ["expiryTimestampMs"]
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: Optional[int] = ...) -> None: ...

class RemoveImplicitKey(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RemoveKey(_message.Message):
    __slots__ = ["userKey"]
    USERKEY_FIELD_NUMBER: ClassVar[int]
    userKey: bytes
    def __init__(self, userKey: Optional[bytes] = ...) -> None: ...

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
    __slots__ = ["mapStateValueSchema", "schema", "stateName", "ttl"]
    MAPSTATEVALUESCHEMA_FIELD_NUMBER: ClassVar[int]
    SCHEMA_FIELD_NUMBER: ClassVar[int]
    STATENAME_FIELD_NUMBER: ClassVar[int]
    TTL_FIELD_NUMBER: ClassVar[int]
    mapStateValueSchema: str
    schema: str
    stateName: str
    ttl: TTLConfig
    def __init__(self, stateName: Optional[str] = ..., schema: Optional[str] = ..., mapStateValueSchema: Optional[str] = ..., ttl: Optional[Union[TTLConfig, Mapping]] = ...) -> None: ...

class StateRequest(_message.Message):
    __slots__ = ["implicitGroupingKeyRequest", "stateVariableRequest", "statefulProcessorCall", "timerRequest", "version"]
    IMPLICITGROUPINGKEYREQUEST_FIELD_NUMBER: ClassVar[int]
    STATEFULPROCESSORCALL_FIELD_NUMBER: ClassVar[int]
    STATEVARIABLEREQUEST_FIELD_NUMBER: ClassVar[int]
    TIMERREQUEST_FIELD_NUMBER: ClassVar[int]
    VERSION_FIELD_NUMBER: ClassVar[int]
    implicitGroupingKeyRequest: ImplicitGroupingKeyRequest
    stateVariableRequest: StateVariableRequest
    statefulProcessorCall: StatefulProcessorCall
    timerRequest: TimerRequest
    version: int
    def __init__(self, version: Optional[int] = ..., statefulProcessorCall: Optional[Union[StatefulProcessorCall, Mapping]] = ..., stateVariableRequest: Optional[Union[StateVariableRequest, Mapping]] = ..., implicitGroupingKeyRequest: Optional[Union[ImplicitGroupingKeyRequest, Mapping]] = ..., timerRequest: Optional[Union[TimerRequest, Mapping]] = ...) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ["errorMessage", "statusCode", "value"]
    ERRORMESSAGE_FIELD_NUMBER: ClassVar[int]
    STATUSCODE_FIELD_NUMBER: ClassVar[int]
    VALUE_FIELD_NUMBER: ClassVar[int]
    errorMessage: str
    statusCode: int
    value: bytes
    def __init__(self, statusCode: Optional[int] = ..., errorMessage: Optional[str] = ..., value: Optional[bytes] = ...) -> None: ...

class StateResponseWithLongTypeVal(_message.Message):
    __slots__ = ["errorMessage", "statusCode", "value"]
    ERRORMESSAGE_FIELD_NUMBER: ClassVar[int]
    STATUSCODE_FIELD_NUMBER: ClassVar[int]
    VALUE_FIELD_NUMBER: ClassVar[int]
    errorMessage: str
    statusCode: int
    value: int
    def __init__(self, statusCode: Optional[int] = ..., errorMessage: Optional[str] = ..., value: Optional[int] = ...) -> None: ...

class StateVariableRequest(_message.Message):
    __slots__ = ["listStateCall", "mapStateCall", "valueStateCall"]
    LISTSTATECALL_FIELD_NUMBER: ClassVar[int]
    MAPSTATECALL_FIELD_NUMBER: ClassVar[int]
    VALUESTATECALL_FIELD_NUMBER: ClassVar[int]
    listStateCall: ListStateCall
    mapStateCall: MapStateCall
    valueStateCall: ValueStateCall
    def __init__(self, valueStateCall: Optional[Union[ValueStateCall, Mapping]] = ..., listStateCall: Optional[Union[ListStateCall, Mapping]] = ..., mapStateCall: Optional[Union[MapStateCall, Mapping]] = ...) -> None: ...

class StatefulProcessorCall(_message.Message):
    __slots__ = ["deleteIfExists", "getListState", "getMapState", "getValueState", "setHandleState", "timerStateCall"]
    DELETEIFEXISTS_FIELD_NUMBER: ClassVar[int]
    GETLISTSTATE_FIELD_NUMBER: ClassVar[int]
    GETMAPSTATE_FIELD_NUMBER: ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: ClassVar[int]
    SETHANDLESTATE_FIELD_NUMBER: ClassVar[int]
    TIMERSTATECALL_FIELD_NUMBER: ClassVar[int]
    deleteIfExists: StateCallCommand
    getListState: StateCallCommand
    getMapState: StateCallCommand
    getValueState: StateCallCommand
    setHandleState: SetHandleState
    timerStateCall: TimerStateCallCommand
    def __init__(self, setHandleState: Optional[Union[SetHandleState, Mapping]] = ..., getValueState: Optional[Union[StateCallCommand, Mapping]] = ..., getListState: Optional[Union[StateCallCommand, Mapping]] = ..., getMapState: Optional[Union[StateCallCommand, Mapping]] = ..., timerStateCall: Optional[Union[TimerStateCallCommand, Mapping]] = ..., deleteIfExists: Optional[Union[StateCallCommand, Mapping]] = ...) -> None: ...

class TTLConfig(_message.Message):
    __slots__ = ["durationMs"]
    DURATIONMS_FIELD_NUMBER: ClassVar[int]
    durationMs: int
    def __init__(self, durationMs: Optional[int] = ...) -> None: ...

class TimerRequest(_message.Message):
    __slots__ = ["expiryTimerRequest", "timerValueRequest"]
    EXPIRYTIMERREQUEST_FIELD_NUMBER: ClassVar[int]
    TIMERVALUEREQUEST_FIELD_NUMBER: ClassVar[int]
    expiryTimerRequest: ExpiryTimerRequest
    timerValueRequest: TimerValueRequest
    def __init__(self, timerValueRequest: Optional[Union[TimerValueRequest, Mapping]] = ..., expiryTimerRequest: Optional[Union[ExpiryTimerRequest, Mapping]] = ...) -> None: ...

class TimerStateCallCommand(_message.Message):
    __slots__ = ["delete", "list", "register"]
    DELETE_FIELD_NUMBER: ClassVar[int]
    LIST_FIELD_NUMBER: ClassVar[int]
    REGISTER_FIELD_NUMBER: ClassVar[int]
    delete: DeleteTimer
    list: ListTimers
    register: RegisterTimer
    def __init__(self, register: Optional[Union[RegisterTimer, Mapping]] = ..., delete: Optional[Union[DeleteTimer, Mapping]] = ..., list: Optional[Union[ListTimers, Mapping]] = ...) -> None: ...

class TimerValueRequest(_message.Message):
    __slots__ = ["getProcessingTimer", "getWatermark"]
    GETPROCESSINGTIMER_FIELD_NUMBER: ClassVar[int]
    GETWATERMARK_FIELD_NUMBER: ClassVar[int]
    getProcessingTimer: GetProcessingTime
    getWatermark: GetWatermark
    def __init__(self, getProcessingTimer: Optional[Union[GetProcessingTime, Mapping]] = ..., getWatermark: Optional[Union[GetWatermark, Mapping]] = ...) -> None: ...

class UpdateValue(_message.Message):
    __slots__ = ["userKey", "value"]
    USERKEY_FIELD_NUMBER: ClassVar[int]
    VALUE_FIELD_NUMBER: ClassVar[int]
    userKey: bytes
    value: bytes
    def __init__(self, userKey: Optional[bytes] = ..., value: Optional[bytes] = ...) -> None: ...

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

class Values(_message.Message):
    __slots__ = ["iteratorId"]
    ITERATORID_FIELD_NUMBER: ClassVar[int]
    iteratorId: str
    def __init__(self, iteratorId: Optional[str] = ...) -> None: ...

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
