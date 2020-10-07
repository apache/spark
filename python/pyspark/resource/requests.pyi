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

from typing import overload, Dict, Optional

from py4j.java_gateway import JVMView  # type: ignore[import]

class ExecutorResourceRequest:
    def __init__(
        self,
        resourceName: str,
        amount: int,
        discoveryScript: str = ...,
        vendor: str = ...,
    ) -> None: ...
    @property
    def resourceName(self) -> str: ...
    @property
    def amount(self) -> int: ...
    @property
    def discoveryScript(self) -> str: ...
    @property
    def vendor(self) -> str: ...

class ExecutorResourceRequests:
    @overload
    def __init__(self, _jvm: JVMView) -> None: ...
    @overload
    def __init__(
        self,
        _jvm: None = ...,
        _requests: Optional[Dict[str, ExecutorResourceRequest]] = ...,
    ) -> None: ...
    def memory(self, amount: str) -> ExecutorResourceRequests: ...
    def memoryOverhead(self, amount: str) -> ExecutorResourceRequests: ...
    def pysparkMemory(self, amount: str) -> ExecutorResourceRequests: ...
    def offheapMemory(self, amount: str) -> ExecutorResourceRequests: ...
    def cores(self, amount: int) -> ExecutorResourceRequests: ...
    def resource(
        self,
        resourceName: str,
        amount: int,
        discoveryScript: str = ...,
        vendor: str = ...,
    ) -> ExecutorResourceRequests: ...
    @property
    def requests(self) -> Dict[str, ExecutorResourceRequest]: ...

class TaskResourceRequest:
    def __init__(self, resourceName: str, amount: float) -> None: ...
    @property
    def resourceName(self) -> str: ...
    @property
    def amount(self) -> float: ...

class TaskResourceRequests:
    @overload
    def __init__(self, _jvm: JVMView) -> None: ...
    @overload
    def __init__(
        self,
        _jvm: None = ...,
        _requests: Optional[Dict[str, TaskResourceRequest]] = ...,
    ) -> None: ...
    def cpus(self, amount: int) -> TaskResourceRequests: ...
    def resource(self, resourceName: str, amount: float) -> TaskResourceRequests: ...
    @property
    def requests(self) -> Dict[str, TaskResourceRequest]: ...
