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

from typing import Callable, Optional, TypeVar
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.dstream import DStream

T = TypeVar("T")

def utf8_decoder(s: Optional[bytes]) -> str: ...

class KinesisUtils:
    @staticmethod
    def createStream(
        ssc: StreamingContext,
        kinesisAppName: str,
        streamName: str,
        endpointUrl: str,
        regionName: str,
        initialPositionInStream: str,
        checkpointInterval: int,
        storageLevel: StorageLevel = ...,
        awsAccessKeyId: Optional[str] = ...,
        awsSecretKey: Optional[str] = ...,
        decoder: Callable[[Optional[bytes]], T] = ...,
        stsAssumeRoleArn: Optional[str] = ...,
        stsSessionName: Optional[str] = ...,
        stsExternalId: Optional[str] = ...,
    ) -> DStream[T]: ...

class InitialPositionInStream:
    LATEST: int
    TRIM_HORIZON: int
