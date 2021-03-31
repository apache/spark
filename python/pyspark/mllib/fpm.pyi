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

from typing import Generic, List, TypeVar
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.util import JavaSaveable, JavaLoader

T = TypeVar("T")

class FPGrowthModel(
    JavaModelWrapper, JavaSaveable, JavaLoader[FPGrowthModel], Generic[T]
):
    def freqItemsets(self) -> RDD[FPGrowth.FreqItemset[T]]: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> FPGrowthModel: ...

class FPGrowth:
    @classmethod
    def train(
        cls, data: RDD[List[T]], minSupport: float = ..., numPartitions: int = ...
    ) -> FPGrowthModel[T]: ...
    class FreqItemset(Generic[T]):
        items: List[T]
        freq: int

class PrefixSpanModel(JavaModelWrapper, Generic[T]):
    def freqSequences(self) -> RDD[PrefixSpan.FreqSequence[T]]: ...

class PrefixSpan:
    @classmethod
    def train(
        cls,
        data: RDD[List[List[T]]],
        minSupport: float = ...,
        maxPatternLength: int = ...,
        maxLocalProjDBSize: int = ...,
    ) -> PrefixSpanModel[T]: ...
    class FreqSequence(tuple, Generic[T]):
        sequence: List[T]
        freq: int
