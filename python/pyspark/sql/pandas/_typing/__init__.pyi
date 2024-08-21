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
    TypeVar,
    Union,
)
from typing_extensions import Protocol, Literal
from types import FunctionType

from pyspark.sql._typing import LiteralType
from pyspark.sql.streaming.state import GroupState
from pandas.core.frame import DataFrame as PandasDataFrame
from pandas.core.series import Series as PandasSeries
from numpy import ndarray as NDArray

import pyarrow

ArrayLike = NDArray
DataFrameLike = PandasDataFrame
SeriesLike = PandasSeries
DataFrameOrSeriesLike = Union[DataFrameLike, SeriesLike]
DataFrameOrSeriesLike_ = TypeVar("DataFrameOrSeriesLike_", bound=DataFrameOrSeriesLike)

# UDF annotations
PandasScalarUDFType = Literal[200]
PandasGroupedMapUDFType = Literal[201]
PandasGroupedAggUDFType = Literal[202]
PandasWindowAggUDFType = Literal[203]
PandasScalarIterUDFType = Literal[204]
PandasMapIterUDFType = Literal[205]
PandasCogroupedMapUDFType = Literal[206]
ArrowMapIterUDFType = Literal[207]
PandasGroupedMapUDFWithStateType = Literal[208]
ArrowGroupedMapUDFType = Literal[209]
ArrowCogroupedMapUDFType = Literal[210]
PandasGroupedMapUDFTransformWithStateType = Literal[211]

class PandasVariadicScalarToScalarFunction(Protocol):
    def __call__(self, *_: DataFrameOrSeriesLike_) -> DataFrameOrSeriesLike_: ...

PandasScalarToScalarFunction = Union[
    PandasVariadicScalarToScalarFunction,
    Callable[[DataFrameOrSeriesLike_], DataFrameOrSeriesLike_],
    Callable[[DataFrameOrSeriesLike_, DataFrameOrSeriesLike_], DataFrameOrSeriesLike_],
    Callable[
        [DataFrameOrSeriesLike_, DataFrameOrSeriesLike_, DataFrameOrSeriesLike_],
        DataFrameOrSeriesLike_,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        SeriesLike,
    ],
]

class PandasVariadicScalarToStructFunction(Protocol):
    def __call__(self, *_: DataFrameOrSeriesLike_) -> DataFrameLike: ...

PandasScalarToStructFunction = Union[
    PandasVariadicScalarToStructFunction,
    Callable[[DataFrameOrSeriesLike_], DataFrameLike],
    Callable[[DataFrameOrSeriesLike_, DataFrameOrSeriesLike_], DataFrameLike],
    Callable[
        [DataFrameOrSeriesLike_, DataFrameOrSeriesLike_, DataFrameOrSeriesLike_],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
    Callable[
        [
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
            DataFrameOrSeriesLike_,
        ],
        DataFrameLike,
    ],
]

PandasScalarIterFunction = Union[
    Callable[[Iterable[DataFrameOrSeriesLike_]], Iterable[SeriesLike]],
    Callable[[Tuple[DataFrameOrSeriesLike_, ...]], Iterable[SeriesLike]],
]

PandasGroupedMapFunction = Union[
    Callable[[DataFrameLike], DataFrameLike],
    Callable[[Any, DataFrameLike], DataFrameLike],
]

PandasGroupedMapFunctionWithState = Callable[
    [Any, Iterable[DataFrameLike], GroupState], Iterable[DataFrameLike]
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

PandasCogroupedMapFunction = Union[
    Callable[[DataFrameLike, DataFrameLike], DataFrameLike],
    Callable[[Any, DataFrameLike, DataFrameLike], DataFrameLike],
]

ArrowGroupedMapFunction = Union[
    Callable[[pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table], pyarrow.Table],
    Callable[[pyarrow.Table], Iterable[pyarrow.RecordBatch]],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table], Iterable[pyarrow.RecordBatch]],
]
ArrowCogroupedMapFunction = Union[
    Callable[[pyarrow.Table, pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table, pyarrow.Table], pyarrow.Table],
    Callable[[pyarrow.Table, pyarrow.Table], Iterable[pyarrow.RecordBatch]],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table, pyarrow.Table], Iterable[pyarrow.RecordBatch]],
]

GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)
