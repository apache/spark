#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from typing_extensions import Literal, Protocol

import datetime
import decimal
import pstats

from pyspark._typing import PrimitiveType
from pyspark.profiler import CodeMapDict
import pyspark.sql.types
from pyspark.sql.column import Column
from pyspark.sql.tvf_argument import TableValuedFunctionArgument

ColumnOrName = Union[Column, str]
TVFArgumentOrName = Union[TableValuedFunctionArgument, str]
ColumnOrNameOrOrdinal = Union[Column, str, int]
DecimalLiteral = decimal.Decimal
DateTimeLiteral = Union[datetime.datetime, datetime.date]
LiteralType = PrimitiveType
AtomicDataTypeOrString = Union[pyspark.sql.types.AtomicType, str]
DataTypeOrString = Union[pyspark.sql.types.DataType, str]
OptionalPrimitiveType = Optional[PrimitiveType]

AtomicValue = TypeVar(
    "AtomicValue",
    datetime.datetime,
    datetime.date,
    decimal.Decimal,
    bool,
    str,
    int,
    float,
)

RowLike = TypeVar("RowLike", List[Any], Tuple[Any, ...], pyspark.sql.types.Row)

SQLBatchedUDFType = Literal[100]
SQLArrowBatchedUDFType = Literal[101]
SQLTableUDFType = Literal[300]
SQLArrowTableUDFType = Literal[301]

class SupportsOpen(Protocol):
    def open(self, partition_id: int, epoch_id: int) -> bool: ...

class SupportsProcess(Protocol):
    def process(self, row: pyspark.sql.types.Row) -> None: ...

class SupportsClose(Protocol):
    def close(self, error: Exception) -> None: ...

class UserDefinedFunctionLike(Protocol):
    func: Callable[..., Any]
    evalType: int
    deterministic: bool
    @property
    def returnType(self) -> pyspark.sql.types.DataType: ...
    def __call__(self, *args: ColumnOrName) -> Column: ...
    def asNondeterministic(self) -> UserDefinedFunctionLike: ...

ProfileResults = Dict[int, Tuple[Optional[pstats.Stats], Optional[CodeMapDict]]]
