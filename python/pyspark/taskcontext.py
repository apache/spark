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

from __future__ import print_function


class TaskContext(object):

    """
    .. note:: Experimental

    Contextual information about a task which can be read or mutated during
    execution. To access the TaskContext for a running task, use:
    L{TaskContext.get()}.
    """

    _taskContext = None

    _attemptNumber = None
    _partitionId = None
    _stageId = None
    _taskAttemptId = None

    def __new__(cls):
        """Even if users construct TaskContext instead of using get, give them the singleton."""
        taskContext = cls._taskContext
        if taskContext is not None:
            return taskContext
        cls._taskContext = taskContext = object.__new__(cls)
        return taskContext

    def __init__(self):
        """Construct a TaskContext, use get instead"""
        pass

    @classmethod
    def _getOrCreate(cls):
        """Internal function to get or create global TaskContext."""
        if cls._taskContext is None:
            cls._taskContext = TaskContext()
        return cls._taskContext

    @classmethod
    def get(cls):
        """
        Return the currently active TaskContext. This can be called inside of
        user functions to access contextual information about running tasks.

        .. note:: Must be called on the worker, not the driver. Returns None if not initialized.
        """
        return cls._taskContext

    def stageId(self):
        """The ID of the stage that this task belong to."""
        return self._stageId

    def partitionId(self):
        """
        The ID of the RDD partition that is computed by this task.
        """
        return self._partitionId

    def attemptNumber(self):
        """"
        How many times this task has been attempted.  The first task attempt will be assigned
        attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
        """
        return self._attemptNumber

    def taskAttemptId(self):
        """
        An ID that is unique to this task attempt (within the same SparkContext, no two task
        attempts will share the same attempt ID).  This is roughly equivalent to Hadoop's
        TaskAttemptID.
        """
        return self._taskAttemptId
