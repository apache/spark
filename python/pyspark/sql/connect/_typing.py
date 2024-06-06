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
from types import FunctionType
from typing import Any, Callable, Iterable, Union, Optional, NewType, Protocol, Tuple, TypeVar
import datetime
import decimal

import pyarrow
from pandas.core.frame import DataFrame as PandasDataFrame

from pyspark.sql.column import Column
from pyspark.sql.connect.types import DataType
from pyspark.sql.streaming.state import GroupState


ColumnOrName = Union[Column, str]
ColumnOrName_ = TypeVar("ColumnOrName_", bound=ColumnOrName)

ColumnOrNameOrOrdinal = Union[Column, str, int]

PrimitiveType = Union[bool, float, int, str]

OptionalPrimitiveType = Optional[PrimitiveType]

LiteralType = PrimitiveType

DecimalLiteral = decimal.Decimal

DateTimeLiteral = Union[datetime.datetime, datetime.date]

DataTypeOrString = Union[DataType, str]

DataFrameLike = PandasDataFrame

PandasMapIterFunction = Callable[[Iterable[DataFrameLike]], Iterable[DataFrameLike]]

ArrowMapIterFunction = Callable[[Iterable[pyarrow.RecordBatch]], Iterable[pyarrow.RecordBatch]]

PandasGroupedMapFunction = Union[
    Callable[[DataFrameLike], DataFrameLike],
    Callable[[Any, DataFrameLike], DataFrameLike],
]

GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)

PandasCogroupedMapFunction = Callable[[DataFrameLike, DataFrameLike], DataFrameLike]

PandasGroupedMapFunctionWithState = Callable[
    [Any, Iterable[DataFrameLike], GroupState], Iterable[DataFrameLike]
]
ArrowGroupedMapFunction = Union[
    Callable[[pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table], pyarrow.Table],
]
ArrowCogroupedMapFunction = Union[
    Callable[[pyarrow.Table, pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table, pyarrow.Table], pyarrow.Table],
]


class UserDefinedFunctionLike(Protocol):
    func: Callable[..., Any]
    evalType: int
    deterministic: bool

    @property
    def returnType(self) -> DataType:
        ...

    def __call__(self, *args: ColumnOrName) -> Column:
        ...

    def asNondeterministic(self) -> "UserDefinedFunctionLike":
        ...


class UserDefinedFunctionCallable(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...
