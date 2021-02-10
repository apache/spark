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
from typing import List, Optional, Tuple

from py4j.java_gateway import JVMView, JavaObject  # type: ignore[import]

class SparkConf:
    def __init__(
        self,
        loadDefaults: bool = ...,
        _jvm: Optional[JVMView] = ...,
        _jconf: Optional[JavaObject] = ...,
    ) -> None: ...
    def set(self, key: str, value: str) -> SparkConf: ...
    def setIfMissing(self, key: str, value: str) -> SparkConf: ...
    def setMaster(self, value: str) -> SparkConf: ...
    def setAppName(self, value: str) -> SparkConf: ...
    def setSparkHome(self, value: str) -> SparkConf: ...
    @overload
    def setExecutorEnv(self, key: str, value: str) -> SparkConf: ...
    @overload
    def setExecutorEnv(self, *, pairs: List[Tuple[str, str]]) -> SparkConf: ...
    def setAll(self, pairs: List[Tuple[str, str]]) -> SparkConf: ...
    def get(self, key: str, defaultValue: Optional[str] = ...) -> str: ...
    def getAll(self) -> List[Tuple[str, str]]: ...
    def contains(self, key: str) -> bool: ...
    def toDebugString(self) -> str: ...
