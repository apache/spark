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
from typing import Optional, Union

from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib._typing import VectorLike
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint, LinearModel, StreamingLinearAlgorithm
from pyspark.mllib.util import Saveable, Loader
from pyspark.streaming.dstream import DStream

from numpy import float64, ndarray  # type: ignore[import]

class LinearClassificationModel(LinearModel):
    def __init__(self, weights: Vector, intercept: float) -> None: ...
    def setThreshold(self, value: float) -> None: ...
    @property
    def threshold(self) -> Optional[float]: ...
    def clearThreshold(self) -> None: ...
    @overload
    def predict(self, test: VectorLike) -> Union[int, float, float64]: ...
    @overload
    def predict(self, test: RDD[VectorLike]) -> RDD[Union[int, float]]: ...

class LogisticRegressionModel(LinearClassificationModel):
    def __init__(
        self, weights: Vector, intercept: float, numFeatures: int, numClasses: int
    ) -> None: ...
    @property
    def numFeatures(self) -> int: ...
    @property
    def numClasses(self) -> int: ...
    @overload
    def predict(self, x: VectorLike) -> Union[int, float]: ...
    @overload
    def predict(self, x: RDD[VectorLike]) -> RDD[Union[int, float]]: ...
    def save(self, sc: SparkContext, path: str) -> None: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> LogisticRegressionModel: ...

class LogisticRegressionWithSGD:
    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = ...,
        step: float = ...,
        miniBatchFraction: float = ...,
        initialWeights: Optional[VectorLike] = ...,
        regParam: float = ...,
        regType: str = ...,
        intercept: bool = ...,
        validateData: bool = ...,
        convergenceTol: float = ...,
    ) -> LogisticRegressionModel: ...

class LogisticRegressionWithLBFGS:
    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = ...,
        initialWeights: Optional[VectorLike] = ...,
        regParam: float = ...,
        regType: str = ...,
        intercept: bool = ...,
        corrections: int = ...,
        tolerance: float = ...,
        validateData: bool = ...,
        numClasses: int = ...,
    ) -> LogisticRegressionModel: ...

class SVMModel(LinearClassificationModel):
    def __init__(self, weights: Vector, intercept: float) -> None: ...
    @overload
    def predict(self, x: VectorLike) -> float64: ...
    @overload
    def predict(self, x: RDD[VectorLike]) -> RDD[float64]: ...
    def save(self, sc: SparkContext, path: str) -> None: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> SVMModel: ...

class SVMWithSGD:
    @classmethod
    def train(
        cls,
        data: RDD[LabeledPoint],
        iterations: int = ...,
        step: float = ...,
        regParam: float = ...,
        miniBatchFraction: float = ...,
        initialWeights: Optional[VectorLike] = ...,
        regType: str = ...,
        intercept: bool = ...,
        validateData: bool = ...,
        convergenceTol: float = ...,
    ) -> SVMModel: ...

class NaiveBayesModel(Saveable, Loader[NaiveBayesModel]):
    labels: ndarray
    pi: ndarray
    theta: ndarray
    def __init__(self, labels: ndarray, pi: ndarray, theta: ndarray) -> None: ...
    @overload
    def predict(self, x: VectorLike) -> float64: ...
    @overload
    def predict(self, x: RDD[VectorLike]) -> RDD[float64]: ...
    def save(self, sc: SparkContext, path: str) -> None: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> NaiveBayesModel: ...

class NaiveBayes:
    @classmethod
    def train(cls, data: RDD[VectorLike], lambda_: float = ...) -> NaiveBayesModel: ...

class StreamingLogisticRegressionWithSGD(StreamingLinearAlgorithm):
    stepSize: float
    numIterations: int
    regParam: float
    miniBatchFraction: float
    convergenceTol: float
    def __init__(
        self,
        stepSize: float = ...,
        numIterations: int = ...,
        miniBatchFraction: float = ...,
        regParam: float = ...,
        convergenceTol: float = ...,
    ) -> None: ...
    def setInitialWeights(
        self, initialWeights: VectorLike
    ) -> StreamingLogisticRegressionWithSGD: ...
    def trainOn(self, dstream: DStream[LabeledPoint]) -> None: ...
