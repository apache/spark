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

from typing import Dict, List
from typing_extensions import Literal
from pyspark.resource.information import ResourceInformation

class TaskContext:
    def __new__(cls) -> TaskContext: ...
    @classmethod
    def get(cls) -> TaskContext: ...
    def stageId(self) -> int: ...
    def partitionId(self) -> int: ...
    def attemptNumber(self) -> int: ...
    def taskAttemptId(self) -> int: ...
    def getLocalProperty(self, key: str) -> str: ...
    def resources(self) -> Dict[str, ResourceInformation]: ...

BARRIER_FUNCTION = Literal[1]

class BarrierTaskContext(TaskContext):
    @classmethod
    def get(cls) -> BarrierTaskContext: ...
    def barrier(self) -> None: ...
    def allGather(self, message: str = ...) -> List[str]: ...
    def getTaskInfos(self) -> List[BarrierTaskInfo]: ...

class BarrierTaskInfo:
    address: str
    def __init__(self, address: str) -> None: ...
