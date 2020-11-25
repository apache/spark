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
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib.linalg import Vector

class RandomRDDs:
    @staticmethod
    def uniformRDD(
        sc: SparkContext,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def normalRDD(
        sc: SparkContext,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def logNormalRDD(
        sc: SparkContext,
        mean: float,
        std: float,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def poissonRDD(
        sc: SparkContext,
        mean: float,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def exponentialRDD(
        sc: SparkContext,
        mean: float,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def gammaRDD(
        sc: SparkContext,
        shape: float,
        scale: float,
        size: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[float]: ...
    @staticmethod
    def uniformVectorRDD(
        sc: SparkContext,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
    @staticmethod
    def normalVectorRDD(
        sc: SparkContext,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
    @staticmethod
    def logNormalVectorRDD(
        sc: SparkContext,
        mean: float,
        std: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
    @staticmethod
    def poissonVectorRDD(
        sc: SparkContext,
        mean: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
    @staticmethod
    def exponentialVectorRDD(
        sc: SparkContext,
        mean: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
    @staticmethod
    def gammaVectorRDD(
        sc: SparkContext,
        shape: float,
        scale: float,
        numRows: int,
        numCols: int,
        numPartitions: Optional[int] = ...,
        seed: Optional[int] = ...,
    ) -> RDD[Vector]: ...
