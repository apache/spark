#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ["SparkJobInfo", "SparkStageInfo", "SparkExecutorInfo", "StatusTracker"]

from typing import List, NamedTuple, Optional

from py4j.java_collections import JavaArray
from py4j.java_gateway import JavaObject


class SparkJobInfo(NamedTuple):
    """
    Exposes information about Spark Jobs.
    """

    jobId: int
    stageIds: JavaArray
    status: str


class SparkStageInfo(NamedTuple):
    """
    Exposes information about Spark Stages.
    """

    stageId: int
    currentAttemptId: int
    name: str
    numTasks: int
    numActiveTasks: int
    numCompletedTasks: int
    numFailedTasks: int


class SparkExecutorInfo(NamedTuple):
    """
    Exposes information about Spark Executors.
    """

    host: str
    port: int
    cacheSize: int
    numRunningTasks: int
    usedOnHeapStorageMemory: int
    usedOffHeapStorageMemory: int
    totalOnHeapStorageMemory: int
    totalOffHeapStorageMemory: int


class StatusTracker:
    """
    Low-level status reporting APIs for monitoring job and stage progress.

    These APIs intentionally provide very weak consistency semantics;
    consumers of these APIs should be prepared to handle empty / missing
    information. For example, a job's stage ids may be known but the status
    API may not have any information about the details of those stages, so
    `getStageInfo` could potentially return `None` for a valid stage id.

    To limit memory usage, these APIs only provide information on recent
    jobs / stages.  These APIs will provide information for the last
    `spark.ui.retainedStages` stages and `spark.ui.retainedJobs` jobs.
    """

    def __init__(self, jtracker: JavaObject):
        self._jtracker = jtracker

    def getJobIdsForGroup(self, jobGroup: Optional[str] = None) -> List[int]:
        """
        Return a list of all known jobs in a particular job group.  If
        `jobGroup` is None, then returns all known jobs that are not
        associated with a job group.

        The returned list may contain running, failed, and completed jobs,
        and may vary across invocations of this method. This method does
        not guarantee the order of the elements in its result.
        """
        return list(self._jtracker.getJobIdsForGroup(jobGroup))

    def getActiveStageIds(self) -> List[int]:
        """
        Returns an array containing the ids of all active stages.
        """
        return sorted(list(self._jtracker.getActiveStageIds()))

    def getActiveJobsIds(self) -> List[int]:
        """
        Returns an array containing the ids of all active jobs.
        """
        return sorted((list(self._jtracker.getActiveJobIds())))

    def getJobInfo(self, jobId: int) -> Optional[SparkJobInfo]:
        """
        Returns a :class:`SparkJobInfo` object, or None if the job info
        could not be found or was garbage collected.
        """
        job = self._jtracker.getJobInfo(jobId)
        if job is not None:
            return SparkJobInfo(jobId, job.stageIds(), str(job.status()))
        return None

    def getStageInfo(self, stageId: int) -> Optional[SparkStageInfo]:
        """
        Returns a :class:`SparkStageInfo` object, or None if the stage
        info could not be found or was garbage collected.
        """
        stage = self._jtracker.getStageInfo(stageId)
        if stage is not None:
            # TODO: fetch them in batch for better performance
            attrs = [getattr(stage, f)() for f in SparkStageInfo._fields[1:]]
            return SparkStageInfo(stageId, *attrs)
        return None

    def getExecutorInfos(self) -> List[SparkExecutorInfo]:
        """
        Returns a list of :class:`SparkExecutorInfo`,
        contains information of all known executors, including host, port, cacheSize,
        numRunningTasks and memory metrics.
        Note this includes information for both the driver and executors.
        """
        executor_infos = self._jtracker.getExecutorInfos()
        return [
            SparkExecutorInfo(
                exec_info.host(),
                exec_info.port(),
                exec_info.cacheSize(),
                exec_info.numRunningTasks(),
                exec_info.usedOnHeapStorageMemory(),
                exec_info.usedOffHeapStorageMemory(),
                exec_info.totalOnHeapStorageMemory(),
                exec_info.totalOffHeapStorageMemory(),
            )
            for exec_info in executor_infos
        ]
