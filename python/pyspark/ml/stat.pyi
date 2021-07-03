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

from typing import Optional

from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame

from py4j.java_gateway import JavaObject  # type: ignore[import]

class ChiSquareTest:
    @staticmethod
    def test(
        dataset: DataFrame, featuresCol: str, labelCol: str, flatten: bool = ...
    ) -> DataFrame: ...

class Correlation:
    @staticmethod
    def corr(dataset: DataFrame, column: str, method: str = ...) -> DataFrame: ...

class KolmogorovSmirnovTest:
    @staticmethod
    def test(
        dataset: DataFrame, sampleCol: str, distName: str, *params: float
    ) -> DataFrame: ...

class Summarizer:
    @staticmethod
    def mean(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def sum(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def variance(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def std(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def count(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def numNonZeros(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def max(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def min(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def normL1(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def normL2(col: Column, weightCol: Optional[Column] = ...) -> Column: ...
    @staticmethod
    def metrics(*metrics: str) -> SummaryBuilder: ...

class SummaryBuilder(JavaWrapper):
    def __init__(self, jSummaryBuilder: JavaObject) -> None: ...
    def summary(
        self, featuresCol: Column, weightCol: Optional[Column] = ...
    ) -> Column: ...

class MultivariateGaussian:
    mean: Vector
    cov: Matrix
    def __init__(self, mean: Vector, cov: Matrix) -> None: ...
