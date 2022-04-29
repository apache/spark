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

from typing import List, Optional, overload, Union
from typing_extensions import Literal

from numpy import ndarray

from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.linalg import Vector, Matrix
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat.test import ChiSqTestResult, KolmogorovSmirnovTestResult
from pyspark.rdd import RDD

CorrelationMethod = Union[Literal["spearman"], Literal["pearson"]]

class MultivariateStatisticalSummary(JavaModelWrapper):
    def mean(self) -> ndarray: ...
    def variance(self) -> ndarray: ...
    def count(self) -> int: ...
    def numNonzeros(self) -> ndarray: ...
    def max(self) -> ndarray: ...
    def min(self) -> ndarray: ...
    def normL1(self) -> ndarray: ...
    def normL2(self) -> ndarray: ...

class Statistics:
    @staticmethod
    def colStats(rdd: RDD[Vector]) -> MultivariateStatisticalSummary: ...
    @overload
    @staticmethod
    def corr(
        x: RDD[Vector], *, method: Optional[CorrelationMethod] = ...
    ) -> Matrix: ...
    @overload
    @staticmethod
    def corr(
        x: RDD[float], y: RDD[float], method: Optional[CorrelationMethod] = ...
    ) -> float: ...
    @overload
    @staticmethod
    def chiSqTest(observed: Matrix) -> ChiSqTestResult: ...
    @overload
    @staticmethod
    def chiSqTest(
        observed: Vector, expected: Optional[Vector] = ...
    ) -> ChiSqTestResult: ...
    @overload
    @staticmethod
    def chiSqTest(observed: RDD[LabeledPoint]) -> List[ChiSqTestResult]: ...
    @staticmethod
    def kolmogorovSmirnovTest(
        data: RDD[float], distName: Literal["norm"] = ..., *params: float
    ) -> KolmogorovSmirnovTestResult: ...
