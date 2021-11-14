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
    Iterable,
    NewType,
    Tuple,
    Type,
    Union,
)
from typing_extensions import Protocol, Literal
from types import FunctionType

from pyspark.sql._typing import LiteralType
from pyspark.sql.pandas._typing.protocols.frame import DataFrameLike as DataFrameLike
from pyspark.sql.pandas._typing.protocols.series import SeriesLike as SeriesLike

import pandas.core.frame  # type: ignore[import]
import pandas.core.series  # type: ignore[import]

import pyarrow  # type: ignore[import]

# POC compatibility annotations
PandasDataFrame: Type[DataFrameLike] = pandas.core.frame.DataFrame
PandasSeries: Type[SeriesLike] = pandas.core.series.Series

DataFrameOrSeriesLike = Union[DataFrameLike, SeriesLike]

# UDF annotations
PandasScalarUDFType = Literal[200]
PandasScalarIterUDFType = Literal[204]
PandasGroupedMapUDFType = Literal[201]
PandasCogroupedMapUDFType = Literal[206]
PandasGroupedAggUDFType = Literal[202]
PandasMapIterUDFType = Literal[205]
ArrowMapIterUDFType = Literal[207]

class PandasVariadicScalarToScalarFunction(Protocol):
    def __call__(self, *_: DataFrameOrSeriesLike) -> SeriesLike: ...

PandasScalarToScalarFunction = Union[
    PandasVariadicScalarToScalarFunction,
    Callable[[DataFrameOrSeriesLike], SeriesLike],
    Callable[[DataFrameOrSeriesLike, DataFrameOrSeriesLike], SeriesLike],
    Callable[
        [DataFrameOrSeriesLike, DataFrameOrSeriesLike, DataFrameOrSeriesLike],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        SeriesLike,
    ],
]

class PandasVariadicScalarToStructFunction(Protocol):
    def __call__(self, *_: DataFrameOrSeriesLike) -> DataFrameLike: ...

PandasScalarToStructFunction = Union[
    PandasVariadicScalarToStructFunction,
    Callable[[DataFrameOrSeriesLike], DataFrameLike],
    Callable[[DataFrameOrSeriesLike, DataFrameOrSeriesLike], DataFrameLike],
    Callable[
        [DataFrameOrSeriesLike, DataFrameOrSeriesLike, DataFrameOrSeriesLike],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
            DataFrameOrSeriesLike,
        ],
        DataFrameLike,
    ],
]

PandasScalarIterFunction = Callable[
    [Iterable[Union[DataFrameOrSeriesLike, Tuple[DataFrameOrSeriesLike, ...]]]],
    Iterable[SeriesLike],
]

PandasGroupedMapFunction = Union[
    Callable[[DataFrameLike], DataFrameLike],
    Callable[[Any, DataFrameLike], DataFrameLike],
]

class PandasVariadicGroupedAggFunction(Protocol):
    def __call__(self, *_: SeriesLike) -> LiteralType: ...

PandasGroupedAggFunction = Union[
    Callable[[SeriesLike], LiteralType],
    Callable[[SeriesLike, SeriesLike], LiteralType],
    Callable[[SeriesLike, SeriesLike, SeriesLike], LiteralType],
    Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType],
    Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType],
    Callable[
        [SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike],
        LiteralType,
    ],
    Callable[
        [
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
        ],
        LiteralType,
    ],
    Callable[
        [
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
        ],
        LiteralType,
    ],
    Callable[
        [
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
        ],
        LiteralType,
    ],
    Callable[
        [
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
            SeriesLike,
        ],
        LiteralType,
    ],
    PandasVariadicGroupedAggFunction,
]

PandasMapIterFunction = Callable[[Iterable[DataFrameLike]], Iterable[DataFrameLike]]

ArrowMapIterFunction = Callable[[Iterable[pyarrow.RecordBatch]], Iterable[pyarrow.RecordBatch]]

PandasCogroupedMapFunction = Callable[[DataFrameLike, DataFrameLike], DataFrameLike]

GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)
