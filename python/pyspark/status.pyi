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

from typing import List, NamedTuple, Optional
from py4j.java_gateway import JavaArray, JavaObject  # type: ignore[import]

class SparkJobInfo(NamedTuple):
    jobId: int
    stageIds: JavaArray
    status: str

class SparkStageInfo(NamedTuple):
    stageId: int
    currentAttemptId: int
    name: str
    numTasks: int
    numActiveTasks: int
    numCompletedTasks: int
    numFailedTasks: int

class StatusTracker:
    def __init__(self, jtracker: JavaObject) -> None: ...
    def getJobIdsForGroup(self, jobGroup: Optional[str] = ...) -> List[int]: ...
    def getActiveStageIds(self) -> List[int]: ...
    def getActiveJobsIds(self) -> List[int]: ...
    def getJobInfo(self, jobId: int) -> SparkJobInfo: ...
    def getStageInfo(self, stageId: int) -> SparkStageInfo: ...
