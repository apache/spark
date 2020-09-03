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

from typing import Hashable, Iterable, Optional, Tuple, TypeVar

from pyspark.resultiterable import ResultIterable
import pyspark.rdd

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
U = TypeVar("U")

def python_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[V, U]]]: ...
def python_right_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[V, Optional[U]]]]: ...
def python_left_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[Optional[V], U]]]: ...
def python_full_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[Optional[V], Optional[U]]]]: ...
def python_cogroup(
    rdds: Iterable[pyspark.rdd.RDD[Tuple[K, V]]], numPartitions: int
) -> pyspark.rdd.RDD[Tuple[K, Tuple[ResultIterable[V], ...]]]: ...
