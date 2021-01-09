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

from typing import overload
from typing import (
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from pyspark.ml._typing import M, P, T, ParamMap

import _thread

import abc
from abc import abstractmethod
from pyspark import since as since  # noqa: F401
from pyspark.ml.common import inherit_doc as inherit_doc  # noqa: F401
from pyspark.ml.param.shared import (
    HasFeaturesCol as HasFeaturesCol,
    HasInputCol as HasInputCol,
    HasLabelCol as HasLabelCol,
    HasOutputCol as HasOutputCol,
    HasPredictionCol as HasPredictionCol,
    Params as Params,
)
from pyspark.sql.functions import udf as udf  # noqa: F401
from pyspark.sql.types import (  # noqa: F401
    DataType,
    StructField as StructField,
    StructType as StructType,
)

from pyspark.sql.dataframe import DataFrame

class _FitMultipleIterator:
    fitSingleModel: Callable[[int], Transformer]
    numModel: int
    counter: int = ...
    lock: _thread.LockType
    def __init__(
        self, fitSingleModel: Callable[[int], Transformer], numModels: int
    ) -> None: ...
    def __iter__(self) -> _FitMultipleIterator: ...
    def __next__(self) -> Tuple[int, Transformer]: ...
    def next(self) -> Tuple[int, Transformer]: ...

class Estimator(Generic[M], Params, metaclass=abc.ABCMeta):
    @overload
    def fit(self, dataset: DataFrame, params: Optional[ParamMap] = ...) -> M: ...
    @overload
    def fit(
        self, dataset: DataFrame, params: Union[List[ParamMap], Tuple[ParamMap]]
    ) -> List[M]: ...
    def fitMultiple(
        self, dataset: DataFrame, params: Sequence[ParamMap]
    ) -> Iterable[Tuple[int, M]]: ...

class Transformer(Params, metaclass=abc.ABCMeta):
    def transform(
        self, dataset: DataFrame, params: Optional[ParamMap] = ...
    ) -> DataFrame: ...

class Model(Transformer, metaclass=abc.ABCMeta): ...

class UnaryTransformer(HasInputCol, HasOutputCol, Transformer, metaclass=abc.ABCMeta):
    def createTransformFunc(self) -> Callable: ...
    def outputDataType(self) -> DataType: ...
    def validateInputType(self, inputType: DataType) -> None: ...
    def transformSchema(self, schema: StructType) -> StructType: ...
    def setInputCol(self: M, value: str) -> M: ...
    def setOutputCol(self: M, value: str) -> M: ...

class _PredictorParams(HasLabelCol, HasFeaturesCol, HasPredictionCol): ...

class Predictor(Estimator[M], _PredictorParams, metaclass=abc.ABCMeta):
    def setLabelCol(self: P, value: str) -> P: ...
    def setFeaturesCol(self: P, value: str) -> P: ...
    def setPredictionCol(self: P, value: str) -> P: ...

class PredictionModel(Generic[T], Model, _PredictorParams, metaclass=abc.ABCMeta):
    def setFeaturesCol(self: M, value: str) -> M: ...
    def setPredictionCol(self: M, value: str) -> M: ...
    @property
    @abc.abstractmethod
    def numFeatures(self) -> int: ...
    @abstractmethod
    def predict(self, value: T) -> float: ...
