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

from typing import List, Optional, Tuple, Type, Union

import array
from collections import namedtuple

from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.util import JavaLoader, JavaSaveable

class Rating(namedtuple("Rating", ["user", "product", "rating"])):
    def __reduce__(self) -> Tuple[Type[Rating], Tuple[int, int, float]]: ...

class MatrixFactorizationModel(
    JavaModelWrapper, JavaSaveable, JavaLoader[MatrixFactorizationModel]
):
    def predict(self, user: int, product: int) -> float: ...
    def predictAll(self, user_product: RDD[Tuple[int, int]]) -> RDD[Rating]: ...
    def userFeatures(self) -> RDD[Tuple[int, array.array]]: ...
    def productFeatures(self) -> RDD[Tuple[int, array.array]]: ...
    def recommendUsers(self, product: int, num: int) -> List[Rating]: ...
    def recommendProducts(self, user: int, num: int) -> List[Rating]: ...
    def recommendProductsForUsers(
        self, num: int
    ) -> RDD[Tuple[int, Tuple[Rating, ...]]]: ...
    def recommendUsersForProducts(
        self, num: int
    ) -> RDD[Tuple[int, Tuple[Rating, ...]]]: ...
    @property
    def rank(self) -> int: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> MatrixFactorizationModel: ...

class ALS:
    @classmethod
    def train(
        cls,
        ratings: Union[RDD[Rating], RDD[Tuple[int, int, float]]],
        rank: int,
        iterations: int = ...,
        lambda_: float = ...,
        blocks: int = ...,
        nonnegative: bool = ...,
        seed: Optional[int] = ...,
    ) -> MatrixFactorizationModel: ...
    @classmethod
    def trainImplicit(
        cls,
        ratings: Union[RDD[Rating], RDD[Tuple[int, int, float]]],
        rank: int,
        iterations: int = ...,
        lambda_: float = ...,
        blocks: int = ...,
        alpha: float = ...,
        nonnegative: bool = ...,
        seed: Optional[int] = ...,
    ) -> MatrixFactorizationModel: ...
