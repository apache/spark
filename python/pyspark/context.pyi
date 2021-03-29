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

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NoReturn,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from types import TracebackType

from py4j.java_gateway import JavaGateway, JavaObject  # type: ignore[import]

from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.conf import SparkConf
from pyspark.profiler import Profiler  # noqa: F401
from pyspark.resource.information import ResourceInformation
from pyspark.rdd import RDD
from pyspark.serializers import Serializer
from pyspark.status import StatusTracker

T = TypeVar("T")
U = TypeVar("U")

class SparkContext:
    master: str
    appName: str
    sparkHome: str
    PACKAGE_EXTENSIONS: Iterable[str]
    def __init__(
        self,
        master: Optional[str] = ...,
        appName: Optional[str] = ...,
        sparkHome: Optional[str] = ...,
        pyFiles: Optional[List[str]] = ...,
        environment: Optional[Dict[str, str]] = ...,
        batchSize: int = ...,
        serializer: Serializer = ...,
        conf: Optional[SparkConf] = ...,
        gateway: Optional[JavaGateway] = ...,
        jsc: Optional[JavaObject] = ...,
        profiler_cls: type = ...,
    ) -> None: ...
    def __getnewargs__(self) -> NoReturn: ...
    def __enter__(self) -> SparkContext: ...
    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        trace: Optional[TracebackType],
    ) -> None: ...
    @classmethod
    def getOrCreate(cls, conf: Optional[SparkConf] = ...) -> SparkContext: ...
    def setLogLevel(self, logLevel: str) -> None: ...
    @classmethod
    def setSystemProperty(cls, key: str, value: str) -> None: ...
    @property
    def version(self) -> str: ...
    @property
    def applicationId(self) -> str: ...
    @property
    def uiWebUrl(self) -> str: ...
    @property
    def startTime(self) -> int: ...
    @property
    def defaultParallelism(self) -> int: ...
    @property
    def defaultMinPartitions(self) -> int: ...
    def stop(self) -> None: ...
    def emptyRDD(self) -> RDD[Any]: ...
    def range(
        self,
        start: int,
        end: Optional[int] = ...,
        step: int = ...,
        numSlices: Optional[int] = ...,
    ) -> RDD[int]: ...
    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = ...) -> RDD[T]: ...
    def pickleFile(self, name: str, minPartitions: Optional[int] = ...) -> RDD[Any]: ...
    def textFile(
        self, name: str, minPartitions: Optional[int] = ..., use_unicode: bool = ...
    ) -> RDD[str]: ...
    def wholeTextFiles(
        self, path: str, minPartitions: Optional[int] = ..., use_unicode: bool = ...
    ) -> RDD[Tuple[str, str]]: ...
    def binaryFiles(
        self, path: str, minPartitions: Optional[int] = ...
    ) -> RDD[Tuple[str, bytes]]: ...
    def binaryRecords(self, path: str, recordLength: int) -> RDD[bytes]: ...
    def sequenceFile(
        self,
        path: str,
        keyClass: Optional[str] = ...,
        valueClass: Optional[str] = ...,
        keyConverter: Optional[str] = ...,
        valueConverter: Optional[str] = ...,
        minSplits: Optional[int] = ...,
        batchSize: int = ...,
    ) -> RDD[Tuple[T, U]]: ...
    def newAPIHadoopFile(
        self,
        path: str,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = ...,
        valueConverter: Optional[str] = ...,
        conf: Optional[Dict[str, str]] = ...,
        batchSize: int = ...,
    ) -> RDD[Tuple[T, U]]: ...
    def newAPIHadoopRDD(
        self,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = ...,
        valueConverter: Optional[str] = ...,
        conf: Optional[Dict[str, str]] = ...,
        batchSize: int = ...,
    ) -> RDD[Tuple[T, U]]: ...
    def hadoopFile(
        self,
        path: str,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = ...,
        valueConverter: Optional[str] = ...,
        conf: Optional[Dict[str, str]] = ...,
        batchSize: int = ...,
    ) -> RDD[Tuple[T, U]]: ...
    def hadoopRDD(
        self,
        inputFormatClass: str,
        keyClass: str,
        valueClass: str,
        keyConverter: Optional[str] = ...,
        valueConverter: Optional[str] = ...,
        conf: Optional[Dict[str, str]] = ...,
        batchSize: int = ...,
    ) -> RDD[Tuple[T, U]]: ...
    def union(self, rdds: Iterable[RDD[T]]) -> RDD[T]: ...
    def broadcast(self, value: T) -> Broadcast[T]: ...
    def accumulator(
        self, value: T, accum_param: Optional[AccumulatorParam[T]] = ...
    ) -> Accumulator[T]: ...
    def addFile(self, path: str, recursive: bool = ...) -> None: ...
    def addPyFile(self, path: str) -> None: ...
    def setCheckpointDir(self, dirName: str) -> None: ...
    def getCheckpointDir(self) -> Optional[str]: ...
    def setJobGroup(
        self, groupId: str, description: str, interruptOnCancel: bool = ...
    ) -> None: ...
    def setLocalProperty(self, key: str, value: str) -> None: ...
    def getLocalProperty(self, key: str) -> Optional[str]: ...
    def sparkUser(self) -> str: ...
    def setJobDescription(self, value: str) -> None: ...
    def cancelJobGroup(self, groupId: str) -> None: ...
    def cancelAllJobs(self) -> None: ...
    def statusTracker(self) -> StatusTracker: ...
    def runJob(
        self,
        rdd: RDD[T],
        partitionFunc: Callable[[Iterable[T]], Iterable[U]],
        partitions: Optional[List[int]] = ...,
        allowLocal: bool = ...,
    ) -> List[U]: ...
    def show_profiles(self) -> None: ...
    def dump_profiles(self, path: str) -> None: ...
    def getConf(self) -> SparkConf: ...
    @property
    def resources(self) -> Dict[str, ResourceInformation]: ...
