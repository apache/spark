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

from typing import Any, Callable, List, Optional, TypeVar

from py4j.java_gateway import JavaObject  # type: ignore[import]

from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.dstream import DStream
from pyspark.streaming.listener import StreamingListener

T = TypeVar("T")

class StreamingContext:
    def __init__(
        self,
        sparkContext: SparkContext,
        batchDuration: int = ...,
        jssc: Optional[JavaObject] = ...,
    ) -> None: ...
    @classmethod
    def getOrCreate(
        cls, checkpointPath: str, setupFunc: Callable[[], StreamingContext]
    ) -> StreamingContext: ...
    @classmethod
    def getActive(cls) -> StreamingContext: ...
    @classmethod
    def getActiveOrCreate(
        cls, checkpointPath: str, setupFunc: Callable[[], StreamingContext]
    ) -> StreamingContext: ...
    @property
    def sparkContext(self) -> SparkContext: ...
    def start(self) -> None: ...
    def awaitTermination(self, timeout: Optional[int] = ...) -> None: ...
    def awaitTerminationOrTimeout(self, timeout: int) -> None: ...
    def stop(self, stopSparkContext: bool = ..., stopGraceFully: bool = ...) -> None: ...
    def remember(self, duration: int) -> None: ...
    def checkpoint(self, directory: str) -> None: ...
    def socketTextStream(
        self, hostname: str, port: int, storageLevel: StorageLevel = ...
    ) -> DStream[str]: ...
    def textFileStream(self, directory: str) -> DStream[str]: ...
    def binaryRecordsStream(self, directory: str, recordLength: int) -> DStream[bytes]: ...
    def queueStream(
        self,
        rdds: List[RDD[T]],
        oneAtATime: bool = ...,
        default: Optional[RDD[T]] = ...,
    ) -> DStream[T]: ...
    def transform(
        self, dstreams: List[DStream[Any]], transformFunc: Callable[..., RDD[T]]
    ) -> DStream[T]: ...
    def union(self, *dstreams: DStream[T]) -> DStream[T]: ...
    def addStreamingListener(self, streamingListener: StreamingListener) -> None: ...
