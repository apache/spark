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
from typing import Any, TYPE_CHECKING, Iterator, Union

from pyspark.sql.streaming.state_api_client import StateApiClient
from pyspark.sql.streaming.value_state_client import ValueStateClient

import pandas as pd
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, ShortType,
    FloatType, DoubleType, DecimalType, StringType, BooleanType,
    DateType, TimestampType
)

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike


class ValueState:
    def __init__(self,
            value_state_client: ValueStateClient,
            state_name: str,
            schema: Union[StructType, str]) -> None:
        self._value_state_client = value_state_client
        self._state_name = state_name
        self.schema = schema

    def exists(self) -> bool:
        return self._value_state_client.exists(self._state_name)

    def get(self) -> Any:
        value_str = self._value_state_client.get(self._state_name)
        columns = [field.name for field in self.schema.fields]
        dtypes = {}
        for field in self.schema.fields:
            if isinstance(field.dataType, (IntegerType, LongType, ShortType)):
                dtypes[field.name] = 'int64'
            elif isinstance(field.dataType, (FloatType, DoubleType, DecimalType)):
                dtypes[field.name] = 'float64'
            elif isinstance(field.dataType, BooleanType):
                dtypes[field.name] = 'bool'
            elif isinstance(field.dataType, (DateType, TimestampType)):
                dtypes[field.name] = 'datetime64[ns]'
            elif isinstance(field.dataType, StringType):
                dtypes[field.name] = 'object'

        # Create the DataFrame using the values and schema
        df = pd.DataFrame([[value_str]], columns=columns)
        # Cast the columns to the appropriate types as defined in the schema
        df = df.astype(dtypes)
        return df

    def update(self, new_value: Any) -> None:
        self._value_state_client.update(self._state_name, self.schema, new_value)

    def clear(self) -> None:
        self._value_state_client.clear(self._state_name)


class StatefulProcessorHandle:
    def __init__(
            self,
            state_api_client: StateApiClient) -> None:
        self.state_api_client = state_api_client

    def getValueState(self, state_name: str, schema: Union[StructType, str]) -> ValueState:
        self.state_api_client.get_value_state(state_name, schema)
        return ValueState(ValueStateClient(self.state_api_client), state_name, schema)


class StatefulProcessor(ABC):
    @abstractmethod
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    @abstractmethod
    def handleInputRows(
            self,
            key: Any,
            rows: Iterator["PandasDataFrameLike"]) -> Iterator["PandasDataFrameLike"]:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
