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

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.streaming.list_state_client import ListStateClient, ListStateIterator
from pyspark.sql.streaming.map_state_client import MapStateClient, MapStateIterator, MapStateKeyValuePairIterator
from pyspark.sql.streaming.value_state_client import ValueStateClient
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

__all__ = ["StatefulProcessor", "StatefulProcessorHandle"]


class ValueState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture single value
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(
        self, value_state_client: ValueStateClient, state_name: str, schema: Union[StructType, str]
    ) -> None:
        self._value_state_client = value_state_client
        self._state_name = state_name
        self.schema = schema

    def exists(self) -> bool:
        """
        Whether state exists or not.
        """
        return self._value_state_client.exists(self._state_name)

    def get(self) -> Optional[Tuple]:
        """
        Get the state value if it exists. Returns None if the state variable does not have a value.
        """
        return self._value_state_client.get(self._state_name)

    def update(self, new_value: Any) -> None:
        """
        Update the value of the state.
        """
        self._value_state_client.update(self._state_name, self.schema, new_value)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._value_state_client.clear(self._state_name)


class ListState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture list value
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(
        self, list_state_client: ListStateClient, state_name: str, schema: Union[StructType, str]
    ) -> None:
        self._list_state_client = list_state_client
        self._state_name = state_name
        self.schema = schema

    def exists(self) -> bool:
        """
        Whether list state exists or not.
        """
        return self._list_state_client.exists(self._state_name)

    def get(self) -> Iterator[Tuple]:
        """
        Get list state with an iterator.
        """
        return ListStateIterator(self._list_state_client, self._state_name)

    def put(self, new_state: List[Tuple]) -> None:
        """
        Update the values of the list state.
        """
        self._list_state_client.put(self._state_name, self.schema, new_state)

    def append_value(self, new_state: Tuple) -> None:
        """
        Append a new value to the list state.
        """
        self._list_state_client.append_value(self._state_name, self.schema, new_state)

    def append_list(self, new_state: List[Tuple]) -> None:
        """
        Append a list of new values to the list state.
        """
        self._list_state_client.append_list(self._state_name, self.schema, new_state)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._list_state_client.clear(self._state_name)


class MapState:
    """
    Class used for arbitrary stateful operations with transformWithState to capture single map
    state.

    .. versionadded:: 4.0.0
    """

    def __init__(
        self,
        map_state_client: MapStateClient,
        state_name: str,
        key_schema: Union[StructType, str],
        value_schema: Union[StructType, str]
    ) -> None:
        self._map_state_client = map_state_client
        self._state_name = state_name
        self.key_schema = key_schema
        self.value_schema = value_schema

    def exists(self) -> bool:
        """
        Whether state exists or not.
        """
        return self._map_state_client.exists(self._state_name)

    def get_value(self, key: Tuple) -> Optional[Tuple]:
        """
        Get the state value if it exists.
        """
        return self._map_state_client.get_value(self._state_name, self.key_schema, key)

    def contains_key(self, key: Tuple) -> bool:
        """
        Check if the user key is contained in the map.
        """
        return self._map_state_client.contains_key(self._state_name, self.key_schema, key)

    def update_value(self, key: Tuple, value: Tuple) -> None:
        """
        Update value for given user key.
        """
        return self._map_state_client.update_value(self._state_name, self.key_schema, key,
                                                   self.value_schema, value)

    def iterator(self) -> Iterator[Tuple[Tuple, Tuple]]:
        """
        Get the map associated with grouping key.
        """
        return MapStateKeyValuePairIterator(self._map_state_client, self._state_name)

    def keys(self) -> Iterator[Tuple]:
        """
        Get the list of keys present in map associated with grouping key.
        """
        return MapStateIterator(self._map_state_client, self._state_name, True)

    def values(self) -> Iterator[Tuple]:
        """
        Get the list of values present in map associated with grouping key.
        """
        return MapStateIterator(self._map_state_client, self._state_name, False)

    def remove_key(self, key: Tuple) -> None:
        """
        Remove user key from map state.
        """
        return self._map_state_client.remove_key(self._state_name, self.key_schema, key)

    def clear(self) -> None:
        """
        Remove this state.
        """
        self._map_state_client.clear(self._state_name)


class StatefulProcessorHandle:
    """
    Represents the operation handle provided to the stateful processor used in transformWithState
    API.

    .. versionadded:: 4.0.0
    """

    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient) -> None:
        self.stateful_processor_api_client = stateful_processor_api_client

    def getValueState(
        self, state_name: str, schema: Union[StructType, str], ttl_duration_ms: Optional[int] = None
    ) -> ValueState:
        """
        Function to create new or return existing single value state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        state_name : str
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
        self.stateful_processor_api_client.get_value_state(state_name, schema, ttl_duration_ms)
        return ValueState(ValueStateClient(self.stateful_processor_api_client), state_name, schema)

    def getListState(self, state_name: str, schema: Union[StructType, str]) -> ListState:
        """
        Function to create new or return existing single value state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        state_name : str
            name of the state variable
        schema : :class:`pyspark.sql.types.DataType` or str
            The schema of the state variable. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        """
        self.stateful_processor_api_client.get_list_state(state_name, schema)
        return ListState(ListStateClient(self.stateful_processor_api_client), state_name, schema)

    def getMapState(
        self,
        state_name: str,
        key_schema: Union[StructType, str],
        value_schema: Union[StructType, str]
    ) -> MapState:
        """
        Function to create new or return existing single map state variable of given type.
        The user must ensure to call this function only within the `init()` method of the
        :class:`StatefulProcessor`.

        Parameters
        ----------
        state_name : str
            name of the state variable
        key_schema : :class:`pyspark.sql.types.DataType` or str
            The schema of the key of map state. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        value_schema : :class:`pyspark.sql.types.DataType` or str
            The schema of the value of map state The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        """
        self.stateful_processor_api_client.get_map_state(state_name, key_schema, value_schema)
        return MapState(MapStateClient(self.stateful_processor_api_client), state_name,
                        key_schema, value_schema)


class StatefulProcessor(ABC):
    """
    Class that represents the arbitrary stateful logic that needs to be provided by the user to
    perform stateful manipulations on keyed streams.

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
        self, key: Any, rows: Iterator["PandasDataFrameLike"]
    ) -> Iterator["PandasDataFrameLike"]:
        """
        Function that will allow users to interact with input data rows along with the grouping key.
        It should take parameters (key, Iterator[`pandas.DataFrame`]) and return another
        Iterator[`pandas.DataFrame`]. For each group, all columns are passed together as
        `pandas.DataFrame` to the function, and the returned `pandas.DataFrame` across all
        invocations are combined as a :class:`DataFrame`. Note that the function should not make a
        guess of the number of elements in the iterator. To process all data, the `handleInputRows`
        function needs to iterate all elements and process them. On the other hand, the
        `handleInputRows` function is not strictly required to iterate through all elements in the
        iterator if it intends to read a part of data.

        Parameters
        ----------
        key : Any
            grouping key.
        rows : iterable of :class:`pandas.DataFrame`
            iterator of input rows associated with grouping key
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Function called as the last method that allows for users to perform any cleanup or teardown
        operations.
        """
        ...
