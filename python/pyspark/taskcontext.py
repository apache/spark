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

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import write_int, UTF8Deserializer


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
    _localProperties = None

    def __new__(cls):
        """Even if users construct TaskContext instead of using get, give them the singleton."""
        taskContext = cls._taskContext
        if taskContext is not None:
            return taskContext
        cls._taskContext = taskContext = object.__new__(cls)
        return taskContext

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

    def getLocalProperty(self, key):
        """
        Get a local property set upstream in the driver, or None if it is missing.
        """
        return self._localProperties.get(key, None)


BARRIER_FUNCTION = 1


def _load_from_socket(port, auth_secret):
    """
    Load data from a given socket, this is a blocking method thus only return when the socket
    connection has been closed.
    """
    (sockfile, sock) = local_connect_and_auth(port, auth_secret)
    # The barrier() call may block forever, so no timeout
    sock.settimeout(None)
    # Make a barrier() function call.
    write_int(BARRIER_FUNCTION, sockfile)
    sockfile.flush()

    # Collect result.
    res = UTF8Deserializer().loads(sockfile)

    # Release resources.
    sockfile.close()
    sock.close()

    return res


class BarrierTaskContext(TaskContext):

    """
    .. note:: Experimental

    A :class:`TaskContext` with extra contextual info and tooling for tasks in a barrier stage.
    Use :func:`BarrierTaskContext.get` to obtain the barrier context for a running barrier task.

    .. versionadded:: 2.4.0
    """

    _port = None
    _secret = None

    @classmethod
    def _getOrCreate(cls):
        """
        Internal function to get or create global BarrierTaskContext. We need to make sure
        BarrierTaskContext is returned from here because it is needed in python worker reuse
        scenario, see SPARK-25921 for more details.
        """
        if not isinstance(cls._taskContext, BarrierTaskContext):
            cls._taskContext = object.__new__(cls)
        return cls._taskContext

    @classmethod
    def get(cls):
        """
        .. note:: Experimental

        Return the currently active :class:`BarrierTaskContext`.
        This can be called inside of user functions to access contextual information about
        running tasks.

        .. note:: Must be called on the worker, not the driver. Returns None if not initialized.
        """
        return cls._taskContext

    @classmethod
    def _initialize(cls, port, secret):
        """
        Initialize BarrierTaskContext, other methods within BarrierTaskContext can only be called
        after BarrierTaskContext is initialized.
        """
        cls._port = port
        cls._secret = secret

    def barrier(self):
        """
        .. note:: Experimental

        Sets a global barrier and waits until all tasks in this stage hit this barrier.
        Similar to `MPI_Barrier` function in MPI, this function blocks until all tasks
        in the same stage have reached this routine.

        .. warning:: In a barrier stage, each task much have the same number of `barrier()`
            calls, in all possible code branches.
            Otherwise, you may get the job hanging or a SparkException after timeout.

        .. versionadded:: 2.4.0
        """
        if self._port is None or self._secret is None:
            raise Exception("Not supported to call barrier() before initialize " +
                            "BarrierTaskContext.")
        else:
            _load_from_socket(self._port, self._secret)

    def getTaskInfos(self):
        """
        .. note:: Experimental

        Returns :class:`BarrierTaskInfo` for all tasks in this barrier stage,
        ordered by partition ID.

        .. versionadded:: 2.4.0
        """
        if self._port is None or self._secret is None:
            raise Exception("Not supported to call getTaskInfos() before initialize " +
                            "BarrierTaskContext.")
        else:
            addresses = self._localProperties.get("addresses", "")
            return [BarrierTaskInfo(h.strip()) for h in addresses.split(",")]


class BarrierTaskInfo(object):
    """
    .. note:: Experimental

    Carries all task infos of a barrier task.

    :var address: The IPv4 address (host:port) of the executor that the barrier task is running on

    .. versionadded:: 2.4.0
    """

    def __init__(self, address):
        self.address = address
