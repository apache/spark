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
    Hashable,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
import datetime
from pyspark.rdd import RDD
import pyspark.serializers
from pyspark.storagelevel import StorageLevel
import pyspark.streaming.context

from py4j.java_gateway import JavaObject

S = TypeVar("S")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

class DStream(Generic[T_co]):
    is_cached: bool
    is_checkpointed: bool
    def __init__(
        self,
        jdstream: JavaObject,
        ssc: pyspark.streaming.context.StreamingContext,
        jrdd_deserializer: pyspark.serializers.Serializer,
    ) -> None: ...
    def context(self) -> pyspark.streaming.context.StreamingContext: ...
    def count(self) -> DStream[int]: ...
    def filter(self, f: Callable[[T_co], bool]) -> DStream[T_co]: ...
    def flatMap(
        self: DStream[T_co],
        f: Callable[[T_co], Iterable[U]],
        preservesPartitioning: bool = ...,
    ) -> DStream[U]: ...
    def map(
        self: DStream[T_co], f: Callable[[T_co], U], preservesPartitioning: bool = ...
    ) -> DStream[U]: ...
    def mapPartitions(
        self, f: Callable[[Iterable[T_co]], Iterable[U]], preservesPartitioning: bool = ...
    ) -> DStream[U]: ...
    def mapPartitionsWithIndex(
        self,
        f: Callable[[int, Iterable[T_co]], Iterable[U]],
        preservesPartitioning: bool = ...,
    ) -> DStream[U]: ...
    def reduce(self, func: Callable[[T_co, T_co], T_co]) -> DStream[T_co]: ...
    def reduceByKey(
        self: DStream[Tuple[K, V]],
        func: Callable[[V, V], V],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, V]]: ...
    def combineByKey(
        self: DStream[Tuple[K, V]],
        createCombiner: Callable[[V], U],
        mergeValue: Callable[[U, V], U],
        mergeCombiners: Callable[[U, U], U],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, U]]: ...
    def partitionBy(
        self: DStream[Tuple[K, V]],
        numPartitions: int,
        partitionFunc: Callable[[K], int] = ...,
    ) -> DStream[Tuple[K, V]]: ...
    @overload
    def foreachRDD(self, func: Callable[[RDD[T_co]], None]) -> None: ...
    @overload
    def foreachRDD(self, func: Callable[[datetime.datetime, RDD[T_co]], None]) -> None: ...
    def pprint(self, num: int = ...) -> None: ...
    def mapValues(self: DStream[Tuple[K, V]], f: Callable[[V], U]) -> DStream[Tuple[K, U]]: ...
    def flatMapValues(
        self: DStream[Tuple[K, V]], f: Callable[[V], Iterable[U]]
    ) -> DStream[Tuple[K, U]]: ...
    def glom(self) -> DStream[List[T_co]]: ...
    def cache(self) -> DStream[T_co]: ...
    def persist(self, storageLevel: StorageLevel) -> DStream[T_co]: ...
    def checkpoint(self, interval: int) -> DStream[T_co]: ...
    def groupByKey(
        self: DStream[Tuple[K, V]], numPartitions: Optional[int] = ...
    ) -> DStream[Tuple[K, Iterable[V]]]: ...
    def countByValue(self) -> DStream[Tuple[T_co, int]]: ...
    def saveAsTextFiles(self, prefix: str, suffix: Optional[str] = ...) -> None: ...
    @overload
    def transform(self, func: Callable[[RDD[T_co]], RDD[U]]) -> TransformedDStream[U]: ...
    @overload
    def transform(
        self, func: Callable[[datetime.datetime, RDD[T_co]], RDD[U]]
    ) -> TransformedDStream[U]: ...
    @overload
    def transformWith(
        self,
        func: Callable[[RDD[T_co], RDD[U]], RDD[V]],
        other: RDD[U],
        keepSerializer: bool = ...,
    ) -> DStream[V]: ...
    @overload
    def transformWith(
        self,
        func: Callable[[datetime.datetime, RDD[T_co], RDD[U]], RDD[V]],
        other: RDD[U],
        keepSerializer: bool = ...,
    ) -> DStream[V]: ...
    def repartition(self, numPartitions: int) -> DStream[T_co]: ...
    def union(self, other: DStream[U]) -> DStream[Union[T_co, U]]: ...
    def cogroup(
        self: DStream[Tuple[K, V]],
        other: DStream[Tuple[K, U]],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Tuple[List[V], List[U]]]]: ...
    def join(
        self: DStream[Tuple[K, V]],
        other: DStream[Tuple[K, U]],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Tuple[V, U]]]: ...
    def leftOuterJoin(
        self: DStream[Tuple[K, V]],
        other: DStream[Tuple[K, U]],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Tuple[V, Optional[U]]]]: ...
    def rightOuterJoin(
        self: DStream[Tuple[K, V]],
        other: DStream[Tuple[K, U]],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Tuple[Optional[V], U]]]: ...
    def fullOuterJoin(
        self: DStream[Tuple[K, V]],
        other: DStream[Tuple[K, U]],
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Tuple[Optional[V], Optional[U]]]]: ...
    def slice(
        self, begin: Union[datetime.datetime, int], end: Union[datetime.datetime, int]
    ) -> List[RDD[T_co]]: ...
    def window(self, windowDuration: int, slideDuration: Optional[int] = ...) -> DStream[T_co]: ...
    def reduceByWindow(
        self,
        reduceFunc: Callable[[T_co, T_co], T_co],
        invReduceFunc: Optional[Callable[[T_co, T_co], T_co]],
        windowDuration: int,
        slideDuration: int,
    ) -> DStream[T_co]: ...
    def countByWindow(
        self, windowDuration: int, slideDuration: int
    ) -> DStream[Tuple[T_co, int]]: ...
    def countByValueAndWindow(
        self,
        windowDuration: int,
        slideDuration: int,
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[T_co, int]]: ...
    def groupByKeyAndWindow(
        self: DStream[Tuple[K, V]],
        windowDuration: int,
        slideDuration: int,
        numPartitions: Optional[int] = ...,
    ) -> DStream[Tuple[K, Iterable[V]]]: ...
    def reduceByKeyAndWindow(
        self: DStream[Tuple[K, V]],
        func: Callable[[V, V], V],
        invFunc: Optional[Callable[[V, V], V]],
        windowDuration: int,
        slideDuration: Optional[int] = ...,
        numPartitions: Optional[int] = ...,
        filterFunc: Optional[Callable[[Tuple[K, V]], bool]] = ...,
    ) -> DStream[Tuple[K, V]]: ...
    def updateStateByKey(
        self: DStream[Tuple[K, V]],
        updateFunc: Callable[[Iterable[V], Optional[S]], S],
        numPartitions: Optional[int] = ...,
        initialRDD: Optional[RDD[Tuple[K, S]]] = ...,
    ) -> DStream[Tuple[K, S]]: ...

class TransformedDStream(DStream[U]):
    is_cached: bool
    is_checkpointed: bool
    func: Callable
    prev: DStream
    @overload
    def __init__(self: DStream[U], prev: DStream[T], func: Callable[[RDD[T]], RDD[U]]) -> None: ...
    @overload
    def __init__(
        self: DStream[U],
        prev: DStream[T],
        func: Callable[[datetime.datetime, RDD[T]], RDD[U]],
    ) -> None: ...
