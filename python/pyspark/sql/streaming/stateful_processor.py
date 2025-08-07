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

from abc import ABC, abstractmethod
from typing import Any, List, TYPE_CHECKING, Iterator, Optional, Union, Tuple

from pyspark.sql.streaming.stateful_processor_api_client import (
    StatefulProcessorApiClient,
    ListTimerIterator,
)
from pyspark.sql.streaming.list_state_client import ListStateClient, ListStateIterator
from pyspark.sql.streaming.map_state_client import (
    MapStateClient,
    MapStateIterator,
    MapStateKeyValuePairIterator,
)
from pyspark.sql.streaming.value_state_client import ValueStateClient
from pyspark.sql.types import StructType, Row

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["StatefulProcessor", "StatefulProcessorHandle"]


class ValueState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture single value
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(self, valueStateClient: ValueStateClient, stateName: str) -> None:
        self._valueStateClient = valueStateClient
        self._stateName = stateName

    def exists(self) -> bool:
        """
        Whether state exists or not.
        """
        return self._valueStateClient.exists(self._stateName)

    def get(self) -> Optional[Tuple]:
        """
        Get the state value if it exists. Returns None if the state variable does not have a value.
        """
        return self._valueStateClient.get(self._stateName)

    def update(self, newValue: Tuple) -> None:
        """
        Update the value of the state.
        """
        self._valueStateClient.update(self._stateName, newValue)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._valueStateClient.clear(self._stateName)


class TimerValues:
    """
    Class used for arbitrary stateful operations with transformWithState to access processing
    time or event time for current batch.
    .. versionadded:: 4.0.0
    """

    def __init__(self, currentProcessingTimeInMs: int = -1, currentWatermarkInMs: int = -1) -> None:
        self._currentProcessingTimeInMs = currentProcessingTimeInMs
        self._currentWatermarkInMs = currentWatermarkInMs

    def getCurrentProcessingTimeInMs(self) -> int:
        """
        Get processing time for current batch, return timestamp in millisecond.
        """
        return self._currentProcessingTimeInMs

    def getCurrentWatermarkInMs(self) -> int:
        """
        Get watermark for current batch, return timestamp in millisecond.
        """
        return self._currentWatermarkInMs


class ExpiredTimerInfo:
    """
    Class used to provide access to expired timer's expiry time.
    .. versionadded:: 4.0.0
    """

    def __init__(self, expiryTimeInMs: int = -1) -> None:
        self._expiryTimeInMs = expiryTimeInMs

    def getExpiryTimeInMs(self) -> int:
        """
        Get the timestamp for expired timer, return timestamp in millisecond.
        """
        return self._expiryTimeInMs


class ListState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture list value
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(self, listStateClient: ListStateClient, stateName: str) -> None:
        self._listStateClient = listStateClient
        self._stateName = stateName

    def exists(self) -> bool:
        """
        Whether list state exists or not.
        """
        return self._listStateClient.exists(self._stateName)

    def get(self) -> Iterator[Tuple]:
        """
        Get list state with an iterator.
        """
        return ListStateIterator(self._listStateClient, self._stateName)

    def put(self, newState: List[Tuple]) -> None:
        """
        Update the values of the list state.
        """
        self._listStateClient.put(self._stateName, newState)

    def appendValue(self, newState: Tuple) -> None:
        """
        Append a new value to the list state.
        """
        self._listStateClient.append_value(self._stateName, newState)

    def appendList(self, newState: List[Tuple]) -> None:
        """
        Append a list of new values to the list state.
        """
        self._listStateClient.append_list(self._stateName, newState)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._listStateClient.clear(self._stateName)


class MapState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture single map
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(
        self,
        mapStateClient: MapStateClient,
        stateName: str,
    ) -> None:
        self._mapStateClient = mapStateClient
        self._stateName = stateName

    def exists(self) -> bool:
        """
        Whether state exists or not.
        """
        return self._mapStateClient.exists(self._stateName)

    def getValue(self, key: Tuple) -> Optional[Tuple]:
        """
        Get the state value for given user key if it exists.
        """
        return self._mapStateClient.get_value(self._stateName, key)

    def containsKey(self, key: Tuple) -> bool:
        """
        Check if the user key is contained in the map.
        """
        return self._mapStateClient.contains_key(self._stateName, key)

    def updateValue(self, key: Tuple, value: Tuple) -> None:
        """
        Update value for given user key.
        """
        return self._mapStateClient.update_value(self._stateName, key, value)

    def iterator(self) -> Iterator[Tuple[Tuple, Tuple]]:
        """
        Get the map associated with grouping key.
        """
        return MapStateKeyValuePairIterator(self._mapStateClient, self._stateName)

    def keys(self) -> Iterator[Tuple]:
        """
        Get the list of keys present in map associated with grouping key.
        """
        return MapStateIterator(self._mapStateClient, self._stateName, True)

    def values(self) -> Iterator[Tuple]:
        """
        Get the list of values present in map associated with grouping key.
        """
        return MapStateIterator(self._mapStateClient, self._stateName, False)

    def removeKey(self, key: Tuple) -> None:
        """
        Remove user key from map state.
        """
        return self._mapStateClient.remove_key(self._stateName, key)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._mapStateClient.clear(self._stateName)


class StatefulProcessorHandle:
    """
    Represents the operation handle provided to the stateful processor used in transformWithState
    API.

    .. versionadded:: 4.0.0
    """

    def __init__(self, statefulProcessorApiClient: StatefulProcessorApiClient) -> None:
        self._statefulProcessorApiClient = statefulProcessorApiClient

    def getValueState(
        self, stateName: str, schema: Union[StructType, str], ttlDurationMs: Optional[int] = None
    ) -> ValueState:
        """
        Function to create new or return existing single value state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        stateName : str
            name of the state variable
        schema : :class:`pyspark.sql.types.DataType` or str
            The schema of the state variable. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        ttlDurationMs: int
            Time to live duration of the state in milliseconds. State values will not be returned
            past ttlDuration and will be eventually removed from the state store. Any state update
            resets the expiration time to current processing time plus ttlDuration.
            If ttl is not specified the state will never expire.
        """
        self._statefulProcessorApiClient.get_value_state(stateName, schema, ttlDurationMs)
        return ValueState(ValueStateClient(self._statefulProcessorApiClient, schema), stateName)

    def getListState(
        self, stateName: str, schema: Union[StructType, str], ttlDurationMs: Optional[int] = None
    ) -> ListState:
        """
        Function to create new or return existing single value state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        stateName : str
            name of the state variable
        schema : :class:`pyspark.sql.types.DataType` or str
            The schema of the state variable. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        ttlDurationMs: int
            Time to live duration of the state in milliseconds. State values will not be returned
            past ttlDuration and will be eventually removed from the state store. Any state update
            resets the expiration time to current processing time plus ttlDuration.
            If ttl is not specified the state will never expire.
        """
        self._statefulProcessorApiClient.get_list_state(stateName, schema, ttlDurationMs)
        return ListState(ListStateClient(self._statefulProcessorApiClient, schema), stateName)

    def getMapState(
        self,
        stateName: str,
        userKeySchema: Union[StructType, str],
        valueSchema: Union[StructType, str],
        ttlDurationMs: Optional[int] = None,
    ) -> MapState:
        """
        Function to create new or return existing single map state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        stateName : str
            name of the state variable
        userKeySchema : :class:`pyspark.sql.types.DataType` or str
            The schema of the key of map state. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        valueSchema : :class:`pyspark.sql.types.DataType` or str
            The schema of the value of map state The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        ttlDurationMs: int
            Time to live duration of the state in milliseconds. State values will not be returned
            past ttlDuration and will be eventually removed from the state store. Any state update
            resets the expiration time to current processing time plus ttlDuration.
            If ttl is not specified the state will never expire.
        """
        self._statefulProcessorApiClient.get_map_state(
            stateName, userKeySchema, valueSchema, ttlDurationMs
        )
        return MapState(
            MapStateClient(self._statefulProcessorApiClient, userKeySchema, valueSchema),
            stateName,
        )

    def registerTimer(self, expiryTimestampMs: int) -> None:
        """
        Register a timer for a given expiry timestamp in milliseconds for the grouping key.
        """
        self._statefulProcessorApiClient.register_timer(expiryTimestampMs)

    def deleteTimer(self, expiryTimestampMs: int) -> None:
        """
        Delete a timer for a given expiry timestamp in milliseconds for the grouping key.
        """
        self._statefulProcessorApiClient.delete_timer(expiryTimestampMs)

    def listTimers(self) -> Iterator[int]:
        """
        List all timers of their expiry timestamps in milliseconds for the grouping key.
        """
        return ListTimerIterator(self._statefulProcessorApiClient)

    def deleteIfExists(self, stateName: str) -> None:
        """
        Function to delete and purge state variable if defined previously
        """
        self._statefulProcessorApiClient.delete_if_exists(stateName)


class StatefulProcessor(ABC):
    """
    Class that represents the arbitrary stateful logic that needs to be provided by the user to
    perform stateful manipulations on keyed streams.

    NOTE: Type of input data and return are different by which method is called, such as:

    `transformWithStateInPandas` - :class:`pandas.DataFrame`
    `transformWithState` - :class:`pyspark.sql.Row`

    and the implementation of this class must follow the described type assignment, which implies
    an implementation has to be bound to a method.

    .. versionadded:: 4.0.0
    """

    @abstractmethod
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Function that will be invoked as the first method that allows for users to initialize all
        their state variables and perform other init actions before handling data.

        Parameters
        ----------
        handle : :class:`pyspark.sql.streaming.stateful_processor.StatefulProcessorHandle`
            Handle to the stateful processor that provides access to the state store and other
            stateful processing related APIs.
        """
        ...

    @abstractmethod
    def handleInputRows(
        self,
        key: Any,
        rows: Union[Iterator["PandasDataFrameLike"], Iterator[Row]],
        timerValues: TimerValues,
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        """
        Function that will allow users to interact with input data rows along with the grouping key.

        Type of input data and return are different by which method is called, such as:

        For `transformWithStateInPandas`, it should take parameters
        (key, Iterator[`pandas.DataFrame`]) and return another Iterator[`pandas.DataFrame`].
        For `transformWithState`, it should take parameters (key, Iterator[`pyspark.sql.Row`])
        and return another Iterator[`pyspark.sql.Row`].

        Note that the function should not make a guess of the number of elements in the iterator.
        To process all data, the `handleInputRows` function needs to iterate all elements and
        process them. On the other hand, the `handleInputRows` function is not strictly required
        to iterate through all elements in the iterator if it intends to read a part of data.

        Parameters
        ----------
        key : Any
            grouping key.
        rows : iterable of :class:`pandas.DataFrame` or iterable of :class:`pyspark.sql.Row`
            iterator of input rows associated with grouping key
        timerValues: TimerValues
                     Timer value for the current batch that process the input rows.
                     Users can get the processing or event time timestamp from TimerValues.
        """
        ...

    def handleExpiredTimer(
        self, key: Any, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        """
        Optional to implement. Will act return an empty iterator if not defined.
        Function that will be invoked when a timer is fired for a given key. Users can choose to
        evict state, register new timers and optionally provide output rows.

        Type of return is different by which method is called, such as:

        For `transformWithStateInPandas`, it should return Iterator[`pandas.DataFrame`].
        For `transformWithState`, it should return Iterator[`pyspark.sql.Row`].

        Parameters
        ----------
        key : Any
            grouping key.
        timerValues: TimerValues
                   Timer value for the current batch that process the input rows.
                   Users can get the processing or event time timestamp from TimerValues.
        expiredTimerInfo: ExpiredTimerInfo
                        Instance of ExpiredTimerInfo that provides access to expired timer.
        """
        return iter([])

    def close(self) -> None:
        """
        Function called as the last method that allows for users to perform any cleanup or teardown
        operations.
        """
        ...

    def handleInitialState(
        self, key: Any, initialState: Union["PandasDataFrameLike", Row], timerValues: TimerValues
    ) -> None:
        """
        Optional to implement. Will act as no-op if not defined or no initial state input.
        Function that will be invoked only in the first batch for users to process initial states.

        Type of initial state is different by which method is called, such as:

        For `transformWithStateInPandas`, it should take `pandas.DataFrame`.
        For `transformWithState`, it should take `pyspark.sql.Row`.

        Parameters
        ----------
        key : Any
            grouping key.
        initialState: :class:`pandas.DataFrame` or :class:`pyspark.sql.Row`
                      One dataframe/row in the initial state associated with the key.
        timerValues: TimerValues
                     Timer value for the current batch that process the input rows.
                     Users can get the processing or event time timestamp from TimerValues.
        """
        pass
