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
import socket

from pyspark.java_gateway import do_server_auth
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

    This is copied from context.py, while modified the message protocol.
    """
    sock = None
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo("localhost", port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        sock = socket.socket(af, socktype, proto)
        try:
            # Do not allow timeout for socket reading operation.
            sock.settimeout(None)
            sock.connect(sa)
        except socket.error:
            sock.close()
            sock = None
            continue
        break
    if not sock:
        raise Exception("could not open socket")

    # We don't really need a socket file here, it's just for convenience that we can reuse the
    # do_server_auth() function and data serialization methods.
    sockfile = sock.makefile("rwb", 65536)

    # Make a barrier() function call.
    write_int(BARRIER_FUNCTION, sockfile)
    sockfile.flush()

    # Do server auth.
    do_server_auth(sockfile, auth_secret)

    # Collect result.
    res = UTF8Deserializer().loads(sockfile)

    # Release resources.
    sockfile.close()
    sock.close()

    return res


class BarrierTaskContext(TaskContext):

    """
    .. note:: Experimental

    A TaskContext with extra info and tooling for a barrier stage. To access the BarrierTaskContext
    for a running task, use:
    L{BarrierTaskContext.get()}.

    .. versionadded:: 2.4.0
    """

    _port = None
    _secret = None

    def __init__(self):
        """Construct a BarrierTaskContext, use get instead"""
        pass

    @classmethod
    def _getOrCreate(cls):
        """Internal function to get or create global BarrierTaskContext."""
        if cls._taskContext is None:
            cls._taskContext = BarrierTaskContext()
        return cls._taskContext

    @classmethod
    def get(cls):
        """
        Return the currently active BarrierTaskContext. This can be called inside of user functions
        to access contextual information about running tasks.

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
        Note this method is only allowed for a BarrierTaskContext.

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

        Returns the all task infos in this barrier stage, the task infos are ordered by
        partitionId.
        Note this method is only allowed for a BarrierTaskContext.

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

    .. versionadded:: 2.4.0
    """

    def __init__(self, address):
        self.address = address
