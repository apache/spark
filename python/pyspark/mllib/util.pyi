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

from typing import Generic, List, Optional, TypeVar

from pyspark.mllib._typing import VectorLike
from pyspark.context import SparkContext
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame

T = TypeVar("T")

class MLUtils:
    @staticmethod
    def loadLibSVMFile(
        sc: SparkContext,
        path: str,
        numFeatures: int = ...,
        minPartitions: Optional[int] = ...,
    ) -> RDD[LabeledPoint]: ...
    @staticmethod
    def saveAsLibSVMFile(data: RDD[LabeledPoint], dir: str) -> None: ...
    @staticmethod
    def loadLabeledPoints(
        sc: SparkContext, path: str, minPartitions: Optional[int] = ...
    ) -> RDD[LabeledPoint]: ...
    @staticmethod
    def appendBias(data: Vector) -> Vector: ...
    @staticmethod
    def loadVectors(sc: SparkContext, path: str) -> RDD[Vector]: ...
    @staticmethod
    def convertVectorColumnsToML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertVectorColumnsFromML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertMatrixColumnsToML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertMatrixColumnsFromML(dataset: DataFrame, *cols: str) -> DataFrame: ...

class Saveable:
    def save(self, sc: SparkContext, path: str) -> None: ...

class JavaSaveable(Saveable):
    def save(self, sc: SparkContext, path: str) -> None: ...

class Loader(Generic[T]):
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> T: ...

class JavaLoader(Loader[T]):
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> T: ...

class LinearDataGenerator:
    @staticmethod
    def generateLinearInput(
        intercept: float,
        weights: VectorLike,
        xMean: VectorLike,
        xVariance: VectorLike,
        nPoints: int,
        seed: int,
        eps: float,
    ) -> List[LabeledPoint]: ...
    @staticmethod
    def generateLinearRDD(
        sc: SparkContext,
        nexamples: int,
        nfeatures: int,
        eps: float,
        nParts: int = ...,
        intercept: float = ...,
    ) -> RDD[LabeledPoint]: ...
