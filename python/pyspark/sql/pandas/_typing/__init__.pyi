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
    Iterator,
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
PandasGroupedMapUDFTransformWithStateInitStateType = Literal[212]
GroupedMapUDFTransformWithStateType = Literal[213]
GroupedMapUDFTransformWithStateInitStateType = Literal[214]
ArrowGroupedMapIterUDFType = Literal[215]
PandasGroupedMapIterUDFType = Literal[216]

# Arrow UDFs
ArrowScalarUDFType = Literal[250]
ArrowScalarIterUDFType = Literal[251]
ArrowGroupedAggUDFType = Literal[252]
ArrowWindowAggUDFType = Literal[253]

class ArrowVariadicScalarToScalarFunction(Protocol):
    def __call__(self, *_: pyarrow.Array) -> pyarrow.Array: ...

ArrowScalarToScalarFunction = Union[
    ArrowVariadicScalarToScalarFunction,
    Callable[[pyarrow.Array], pyarrow.Array],
    Callable[[pyarrow.Array, pyarrow.Array], pyarrow.Array],
    Callable[[pyarrow.Array, pyarrow.Array, pyarrow.Array], pyarrow.Array],
    Callable[[pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array], pyarrow.Array],
    Callable[
        [pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array], pyarrow.Array
    ],
    Callable[
        [pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array],
        pyarrow.Array,
    ],
    Callable[
        [
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
        ],
        pyarrow.Array,
    ],
    Callable[
        [
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
        ],
        pyarrow.Array,
    ],
    Callable[
        [
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
        ],
        pyarrow.Array,
    ],
    Callable[
        [
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
            pyarrow.Array,
        ],
        pyarrow.Array,
    ],
]

ArrowScalarIterFunction = Union[
    Callable[[Iterable[pyarrow.Array]], Iterable[pyarrow.Array]],
    Callable[[Tuple[pyarrow.Array, ...]], Iterable[pyarrow.Array]],
]

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
    Callable[[Iterator[DataFrameLike]], Iterator[DataFrameLike]],
    Callable[[Any, Iterator[DataFrameLike]], Iterator[DataFrameLike]],
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

ArrowGroupedMapTableFunction = Union[
    Callable[[pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table], pyarrow.Table],
]
ArrowGroupedMapIterFunction = Union[
    Callable[[Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]],
    Callable[
        [Tuple[pyarrow.Scalar, ...], Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]
    ],
]
ArrowGroupedMapFunction = Union[ArrowGroupedMapTableFunction, ArrowGroupedMapIterFunction]

ArrowCogroupedMapFunction = Union[
    Callable[[pyarrow.Table, pyarrow.Table], pyarrow.Table],
    Callable[[Tuple[pyarrow.Scalar, ...], pyarrow.Table, pyarrow.Table], pyarrow.Table],
]

GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)
