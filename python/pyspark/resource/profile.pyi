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

from pyspark.resource.requests import (  # noqa: F401
    ExecutorResourceRequest as ExecutorResourceRequest,
    ExecutorResourceRequests as ExecutorResourceRequests,
    TaskResourceRequest as TaskResourceRequest,
    TaskResourceRequests as TaskResourceRequests,
)
from typing import overload, Dict, Union, Optional
from py4j.java_gateway import JavaObject  # type: ignore[import]

class ResourceProfile:
    @overload
    def __init__(
        self,
        _java_resource_profile: JavaObject,
    ) -> None: ...
    @overload
    def __init__(
        self,
        _java_resource_profile: None = ...,
        _exec_req: Optional[Dict[str, ExecutorResourceRequest]] = ...,
        _task_req: Optional[Dict[str, TaskResourceRequest]] = ...,
    ) -> None: ...
    @property
    def id(self) -> int: ...
    @property
    def taskResources(self) -> Dict[str, TaskResourceRequest]: ...
    @property
    def executorResources(self) -> Dict[str, ExecutorResourceRequest]: ...

class ResourceProfileBuilder:
    def __init__(self) -> None: ...
    def require(
        self, resourceRequest: Union[ExecutorResourceRequest, TaskResourceRequests]
    ) -> ResourceProfileBuilder: ...
    def clearExecutorResourceRequests(self) -> None: ...
    def clearTaskResourceRequests(self) -> None: ...
    @property
    def taskResources(self) -> Dict[str, TaskResourceRequest]: ...
    @property
    def executorResources(self) -> Dict[str, ExecutorResourceRequest]: ...
    @property
    def build(self) -> ResourceProfile: ...
