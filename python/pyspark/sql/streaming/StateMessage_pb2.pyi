from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar, Mapping, Optional, Union

CLOSED: HandleState
CREATED: HandleState
DATA_PROCESSED: HandleState
DESCRIPTOR: _descriptor.FileDescriptor
INITIALIZED: HandleState
TIMER_PROCESSED: HandleState

class Clear(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

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

class ListTimers(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RegisterTimer(_message.Message):
    __slots__ = ["expiryTimestampMs"]
    EXPIRYTIMESTAMPMS_FIELD_NUMBER: ClassVar[int]
    expiryTimestampMs: int
    def __init__(self, expiryTimestampMs: Optional[int] = ...) -> None: ...

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

class StateVariableRequest(_message.Message):
    __slots__ = ["valueStateCall"]
    VALUESTATECALL_FIELD_NUMBER: ClassVar[int]
    valueStateCall: ValueStateCall
    def __init__(self, valueStateCall: Optional[Union[ValueStateCall, Mapping]] = ...) -> None: ...

class StatefulProcessorCall(_message.Message):
    __slots__ = ["getListState", "getMapState", "getValueState", "setHandleState", "timerStateCall"]
    GETLISTSTATE_FIELD_NUMBER: ClassVar[int]
    GETMAPSTATE_FIELD_NUMBER: ClassVar[int]
    GETVALUESTATE_FIELD_NUMBER: ClassVar[int]
    SETHANDLESTATE_FIELD_NUMBER: ClassVar[int]
    TIMERSTATECALL_FIELD_NUMBER: ClassVar[int]
    getListState: StateCallCommand
    getMapState: StateCallCommand
    getValueState: StateCallCommand
    setHandleState: SetHandleState
    timerStateCall: TimerStateCallCommand
    def __init__(self, setHandleState: Optional[Union[SetHandleState, Mapping]] = ..., getValueState: Optional[Union[StateCallCommand, Mapping]] = ..., getListState: Optional[Union[StateCallCommand, Mapping]] = ..., getMapState: Optional[Union[StateCallCommand, Mapping]] = ..., timerStateCall: Optional[Union[TimerStateCallCommand, Mapping]] = ...) -> None: ...

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
