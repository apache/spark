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
from typing import ClassVar, Type, Dict, List, Optional, Union, cast, TYPE_CHECKING

from pyspark.util import local_connect_and_auth
from pyspark.serializers import read_int, write_int, write_with_length, UTF8Deserializer
from pyspark.errors import PySparkRuntimeError

if TYPE_CHECKING:
    from pyspark.resource import ResourceInformation


class TaskContext:

    """
    Contextual information about a task which can be read or mutated during
    execution. To access the TaskContext for a running task, use:
    :meth:`TaskContext.get`.

    .. versionadded:: 2.2.0

    Examples
    --------
    >>> from pyspark import TaskContext

    Get a task context instance from :class:`RDD`.

    >>> spark.sparkContext.setLocalProperty("key1", "value")
    >>> taskcontext = spark.sparkContext.parallelize([1]).map(lambda _: TaskContext.get()).first()
    >>> isinstance(taskcontext.attemptNumber(), int)
    True
    >>> isinstance(taskcontext.partitionId(), int)
    True
    >>> isinstance(taskcontext.stageId(), int)
    True
    >>> isinstance(taskcontext.taskAttemptId(), int)
    True
    >>> taskcontext.getLocalProperty("key1")
    'value'
    >>> isinstance(taskcontext.cpus(), int)
    True

    Get a task context instance from a dataframe via Python UDF.

    >>> from pyspark.sql import Row
    >>> from pyspark.sql.functions import udf
    >>> @udf("STRUCT<anum: INT, partid: INT, stageid: INT, taskaid: INT, prop: STRING, cpus: INT>")
    ... def taskcontext_as_row():
    ...    taskcontext = TaskContext.get()
    ...    return Row(
    ...        anum=taskcontext.attemptNumber(),
    ...        partid=taskcontext.partitionId(),
    ...        stageid=taskcontext.stageId(),
    ...        taskaid=taskcontext.taskAttemptId(),
    ...        prop=taskcontext.getLocalProperty("key2"),
    ...        cpus=taskcontext.cpus())
    ...
    >>> spark.sparkContext.setLocalProperty("key2", "value")
    >>> [(anum, partid, stageid, taskaid, prop, cpus)] = (
    ...     spark.range(1).select(taskcontext_as_row()).first()
    ... )
    >>> isinstance(anum, int)
    True
    >>> isinstance(partid, int)
    True
    >>> isinstance(stageid, int)
    True
    >>> isinstance(taskaid, int)
    True
    >>> prop
    'value'
    >>> isinstance(cpus, int)
    True

    Get a task context instance from a dataframe via Pandas UDF.

    >>> import pandas as pd  # doctest: +SKIP
    >>> from pyspark.sql.functions import pandas_udf
    >>> @pandas_udf("STRUCT<"
    ...     "anum: INT, partid: INT, stageid: INT, taskaid: INT, prop: STRING, cpus: INT>")
    ... def taskcontext_as_row(_):
    ...    taskcontext = TaskContext.get()
    ...    return pd.DataFrame({
    ...        "anum": [taskcontext.attemptNumber()],
    ...        "partid": [taskcontext.partitionId()],
    ...        "stageid": [taskcontext.stageId()],
    ...        "taskaid": [taskcontext.taskAttemptId()],
    ...        "prop": [taskcontext.getLocalProperty("key3")],
    ...        "cpus": [taskcontext.cpus()]
    ...    })  # doctest: +SKIP
    ...
    >>> spark.sparkContext.setLocalProperty("key3", "value")  # doctest: +SKIP
    >>> [(anum, partid, stageid, taskaid, prop, cpus)] = (
    ...     spark.range(1).select(taskcontext_as_row("id")).first()
    ... )  # doctest: +SKIP
    >>> isinstance(anum, int)
    True
    >>> isinstance(partid, int)
    True
    >>> isinstance(stageid, int)
    True
    >>> isinstance(taskaid, int)
    True
    >>> prop
    'value'
    >>> isinstance(cpus, int)
    True
    """

    _taskContext: ClassVar[Optional["TaskContext"]] = None

    _attemptNumber: Optional[int] = None
    _partitionId: Optional[int] = None
    _stageId: Optional[int] = None
    _taskAttemptId: Optional[int] = None
    _localProperties: Optional[Dict[str, str]] = None
    _cpus: Optional[int] = None
    _resources: Optional[Dict[str, "ResourceInformation"]] = None

    def __new__(cls: Type["TaskContext"]) -> "TaskContext":
        """
        Even if users construct :class:`TaskContext` instead of using get, give them the singleton.
        """
        taskContext = cls._taskContext
        if taskContext is not None:
            return taskContext
        cls._taskContext = taskContext = object.__new__(cls)
        return taskContext

    @classmethod
    def _getOrCreate(cls: Type["TaskContext"]) -> "TaskContext":
        """Internal function to get or create global :class:`TaskContext`."""
        if cls._taskContext is None:
            cls._taskContext = TaskContext()
        return cls._taskContext

    @classmethod
    def _setTaskContext(cls: Type["TaskContext"], taskContext: "TaskContext") -> None:
        cls._taskContext = taskContext

    @classmethod
    def get(cls: Type["TaskContext"]) -> Optional["TaskContext"]:
        """
        Return the currently active :class:`TaskContext`. This can be called inside of
        user functions to access contextual information about running tasks.

        Returns
        -------
        :class:`TaskContext`, optional

        Notes
        -----
        Must be called on the worker, not the driver. Returns ``None`` if not initialized.
        """
        return cls._taskContext

    def stageId(self) -> int:
        """
        The ID of the stage that this task belong to.

        Returns
        -------
        int
            current stage id.
        """
        return cast(int, self._stageId)

    def partitionId(self) -> int:
        """
        The ID of the RDD partition that is computed by this task.

        Returns
        -------
        int
            current partition id.
        """
        return cast(int, self._partitionId)

    def attemptNumber(self) -> int:
        """
        How many times this task has been attempted.  The first task attempt will be assigned
        attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.

        Returns
        -------
        int
            current attempt number.
        """
        return cast(int, self._attemptNumber)

    def taskAttemptId(self) -> int:
        """
        An ID that is unique to this task attempt (within the same :class:`SparkContext`,
        no two task attempts will share the same attempt ID).  This is roughly equivalent
        to Hadoop's `TaskAttemptID`.

        Returns
        -------
        int
            current task attempt id.
        """
        return cast(int, self._taskAttemptId)

    def getLocalProperty(self, key: str) -> Optional[str]:
        """
        Get a local property set upstream in the driver, or None if it is missing.

        Parameters
        ----------
        key : str
            the key of the local property to get.

        Returns
        -------
        int
            the value of the local property.
        """
        return cast(Dict[str, str], self._localProperties).get(key, None)

    def cpus(self) -> int:
        """
        CPUs allocated to the task.

        Returns
        -------
        int
            the number of CPUs.
        """
        return cast(int, self._cpus)

    def resources(self) -> Dict[str, "ResourceInformation"]:
        """
        Resources allocated to the task. The key is the resource name and the value is information
        about the resource.

        Returns
        -------
        dict
            a dictionary of a string resource name, and :class:`ResourceInformation`.
        """
        from pyspark.resource import ResourceInformation

        return cast(Dict[str, "ResourceInformation"], self._resources)


BARRIER_FUNCTION = 1
ALL_GATHER_FUNCTION = 2


def _load_from_socket(
    port: Optional[Union[str, int]],
    auth_secret: str,
    function: int,
    all_gather_message: Optional[str] = None,
) -> List[str]:
    """
    Load data from a given socket, this is a blocking method thus only return when the socket
    connection has been closed.
    """
    (sockfile, sock) = local_connect_and_auth(port, auth_secret)

    # The call may block forever, so no timeout
    sock.settimeout(None)

    if function == BARRIER_FUNCTION:
        # Make a barrier() function call.
        write_int(function, sockfile)
    elif function == ALL_GATHER_FUNCTION:
        # Make a all_gather() function call.
        write_int(function, sockfile)
        write_with_length(cast(str, all_gather_message).encode("utf-8"), sockfile)
    else:
        raise ValueError("Unrecognized function type")
    sockfile.flush()

    # Collect result.
    len = read_int(sockfile)
    res = []
    for i in range(len):
        res.append(UTF8Deserializer().loads(sockfile))

    # Release resources.
    sockfile.close()
    sock.close()

    return res


class BarrierTaskContext(TaskContext):

    """
    A :class:`TaskContext` with extra contextual info and tooling for tasks in a barrier stage.
    Use :func:`BarrierTaskContext.get` to obtain the barrier context for a running barrier task.

    .. versionadded:: 2.4.0

    Notes
    -----
    This API is experimental

    Examples
    --------
    Set a barrier, and execute it with RDD.

    >>> from pyspark import BarrierTaskContext
    >>> def block_and_do_something(itr):
    ...     taskcontext = BarrierTaskContext.get()
    ...     # Do something.
    ...
    ...     # Wait until all tasks finished.
    ...     taskcontext.barrier()
    ...
    ...     return itr
    ...
    >>> rdd = spark.sparkContext.parallelize([1])
    >>> rdd.barrier().mapPartitions(block_and_do_something).collect()
    [1]
    """

    _port: ClassVar[Optional[Union[str, int]]] = None
    _secret: ClassVar[Optional[str]] = None

    @classmethod
    def _getOrCreate(cls: Type["BarrierTaskContext"]) -> "BarrierTaskContext":
        """
        Internal function to get or create global :class:`BarrierTaskContext`. We need to make sure
        :class:`BarrierTaskContext` is returned from here because it is needed in python worker
        reuse scenario, see SPARK-25921 for more details.
        """
        if not isinstance(cls._taskContext, BarrierTaskContext):
            cls._taskContext = object.__new__(cls)
        return cls._taskContext

    @classmethod
    def get(cls: Type["BarrierTaskContext"]) -> "BarrierTaskContext":
        """
        Return the currently active :class:`BarrierTaskContext`.
        This can be called inside of user functions to access contextual information about
        running tasks.

        Notes
        -----
        Must be called on the worker, not the driver. Returns ``None`` if not initialized.
        An Exception will raise if it is not in a barrier stage.

        This API is experimental
        """
        if not isinstance(cls._taskContext, BarrierTaskContext):
            raise PySparkRuntimeError(
                errorClass="NOT_IN_BARRIER_STAGE",
                messageParameters={},
            )
        return cls._taskContext

    @classmethod
    def _initialize(
        cls: Type["BarrierTaskContext"], port: Optional[Union[str, int]], secret: str
    ) -> None:
        """
        Initialize :class:`BarrierTaskContext`, other methods within :class:`BarrierTaskContext`
        can only be called after BarrierTaskContext is initialized.
        """
        cls._port = port
        cls._secret = secret

    def barrier(self) -> None:
        """
        Sets a global barrier and waits until all tasks in this stage hit this barrier.
        Similar to `MPI_Barrier` function in MPI, this function blocks until all tasks
        in the same stage have reached this routine.

        .. versionadded:: 2.4.0

        Notes
        -----
        This API is experimental

        In a barrier stage, each task much have the same number of `barrier()`
        calls, in all possible code branches. Otherwise, you may get the job hanging
        or a `SparkException` after timeout.
        """
        if self._port is None or self._secret is None:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "barrier",
                    "object": "BarrierTaskContext",
                },
            )
        else:
            _load_from_socket(self._port, self._secret, BARRIER_FUNCTION)

    def allGather(self, message: str = "") -> List[str]:
        """
        This function blocks until all tasks in the same stage have reached this routine.
        Each task passes in a message and returns with a list of all the messages passed in
        by each of those tasks.

        .. versionadded:: 3.0.0

        Notes
        -----
        This API is experimental

        In a barrier stage, each task much have the same number of `barrier()`
        calls, in all possible code branches. Otherwise, you may get the job hanging
        or a `SparkException` after timeout.
        """
        if not isinstance(message, str):
            raise TypeError("Argument `message` must be of type `str`")
        elif self._port is None or self._secret is None:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "allGather",
                    "object": "BarrierTaskContext",
                },
            )
        else:
            return _load_from_socket(self._port, self._secret, ALL_GATHER_FUNCTION, message)

    def getTaskInfos(self) -> List["BarrierTaskInfo"]:
        """
        Returns :class:`BarrierTaskInfo` for all tasks in this barrier stage,
        ordered by partition ID.

        .. versionadded:: 2.4.0

        Notes
        -----
        This API is experimental

        Examples
        --------
        >>> from pyspark import BarrierTaskContext
        >>> rdd = spark.sparkContext.parallelize([1])
        >>> barrier_info = rdd.barrier().mapPartitions(
        ...     lambda _: [BarrierTaskContext.get().getTaskInfos()]).collect()[0][0]
        >>> barrier_info.address
        '...:...'
        """
        if self._port is None or self._secret is None:
            raise PySparkRuntimeError(
                errorClass="CALL_BEFORE_INITIALIZE",
                messageParameters={
                    "func_name": "getTaskInfos",
                    "object": "BarrierTaskContext",
                },
            )
        else:
            addresses = cast(Dict[str, str], self._localProperties).get("addresses", "")
            return [BarrierTaskInfo(h.strip()) for h in addresses.split(",")]


class BarrierTaskInfo:
    """
    Carries all task infos of a barrier task.

    .. versionadded:: 2.4.0

    Attributes
    ----------
    address : str
        The IPv4 address (host:port) of the executor that the barrier task is running on

    Notes
    -----
    This API is experimental
    """

    def __init__(self, address: str) -> None:
        self.address = address


def _test() -> None:
    import doctest
    import sys
    from pyspark.sql import SparkSession

    globs = globals().copy()
    globs["spark"] = (
        SparkSession.builder.master("local[2]").appName("taskcontext tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
