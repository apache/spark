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
from threading import RLock
from typing import overload, Dict, Union, Optional, TYPE_CHECKING

from pyspark.resource.requests import (
    TaskResourceRequest,
    TaskResourceRequests,
    ExecutorResourceRequests,
    ExecutorResourceRequest,
)

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject


class ResourceProfile:

    """
    Resource profile to associate with an RDD. A :class:`pyspark.resource.ResourceProfile`
    allows the user to specify executor and task requirements for an RDD that will get
    applied during a stage. This allows the user to change the resource requirements between
    stages. This is meant to be immutable so user cannot change it after building.

    .. versionadded:: 3.1.0

    .. versionchanged:: 4.0.0
        Supports Spark Connect.

    Notes
    -----
    This API is evolving.

    Examples
    --------
    Create Executor resource requests.

    >>> executor_requests = (
    ...     ExecutorResourceRequests()
    ...     .cores(2)
    ...     .memory("6g")
    ...     .memoryOverhead("1g")
    ...     .pysparkMemory("2g")
    ...     .offheapMemory("3g")
    ...     .resource("gpu", 2, "testGpus", "nvidia.com")
    ... )

    Create task resource requasts.

    >>> task_requests = TaskResourceRequests().cpus(2).resource("gpu", 2)

    Create a resource profile.

    >>> builder = ResourceProfileBuilder()
    >>> resource_profile = builder.require(executor_requests).require(task_requests).build

    Create an RDD with the resource profile.

    >>> rdd = sc.parallelize(range(10)).withResources(resource_profile)
    >>> rdd.getResourceProfile()
    <pyspark.resource.profile.ResourceProfile object ...>
    >>> rdd.getResourceProfile().taskResources
    {'cpus': <...TaskResourceRequest...>, 'gpu': <...TaskResourceRequest...>}
    >>> rdd.getResourceProfile().executorResources
    {'gpu': <...ExecutorResourceRequest...>,
     'cores': <...ExecutorResourceRequest...>,
     'offHeap': <...ExecutorResourceRequest...>,
     'memoryOverhead': <...ExecutorResourceRequest...>,
     'pyspark.memory': <...ExecutorResourceRequest...>,
     'memory': <...ExecutorResourceRequest...>}
    """

    @overload
    def __init__(self, _java_resource_profile: "JavaObject"):
        ...

    @overload
    def __init__(
        self,
        _java_resource_profile: None = ...,
        _exec_req: Optional[Dict[str, ExecutorResourceRequest]] = ...,
        _task_req: Optional[Dict[str, TaskResourceRequest]] = ...,
    ):
        ...

    def __init__(
        self,
        _java_resource_profile: Optional["JavaObject"] = None,
        _exec_req: Optional[Dict[str, ExecutorResourceRequest]] = None,
        _task_req: Optional[Dict[str, TaskResourceRequest]] = None,
    ):
        # profile id
        self._id: Optional[int] = None
        # lock to protect _id
        self._lock = RLock()

        if _java_resource_profile is not None:
            self._java_resource_profile = _java_resource_profile
        else:
            self._java_resource_profile = None
            self._executor_resource_requests = _exec_req or {}
            self._task_resource_requests = _task_req or {}

    @property
    def id(self) -> int:
        """
        Returns
        -------
        int
            A unique id of this :class:`ResourceProfile`
        """
        with self._lock:
            if self._id is None:
                if self._java_resource_profile is not None:
                    self._id = self._java_resource_profile.id()
                else:
                    from pyspark.sql import is_remote

                    if is_remote():
                        from pyspark.sql.connect.resource.profile import ResourceProfile

                        # Utilize the connect ResourceProfile to create Spark ResourceProfile
                        # on the server and get the profile ID.
                        rp = ResourceProfile(
                            self._executor_resource_requests, self._task_resource_requests
                        )
                        self._id = rp.id
                    else:
                        raise RuntimeError("SparkContext must be created to get the profile id.")
            return self._id

    @property
    def taskResources(self) -> Dict[str, TaskResourceRequest]:
        """
        Returns
        -------
        dict
            a dictionary of resources to :class:`TaskResourceRequest`
        """

        if self._java_resource_profile is not None:
            taskRes = self._java_resource_profile.taskResourcesJMap()
            result = {}
            for k, v in taskRes.items():
                result[k] = TaskResourceRequest(v.resourceName(), v.amount())
            return result
        else:
            return self._task_resource_requests

    @property
    def executorResources(self) -> Dict[str, ExecutorResourceRequest]:
        """
        Returns
        -------
        dict
            a dictionary of resources to :class:`ExecutorResourceRequest`
        """
        if self._java_resource_profile is not None:
            execRes = self._java_resource_profile.executorResourcesJMap()
            result = {}
            for k, v in execRes.items():
                result[k] = ExecutorResourceRequest(
                    v.resourceName(), v.amount(), v.discoveryScript(), v.vendor()
                )
            return result
        else:
            return self._executor_resource_requests


class ResourceProfileBuilder:

    """
    Resource profile Builder to build a resource profile to associate with an RDD.
    A ResourceProfile allows the user to specify executor and task requirements for
    an RDD that will get applied during a stage. This allows the user to change the
    resource requirements between stages.

    .. versionadded:: 3.1.0

    See Also
    --------
    :class:`pyspark.resource.ResourceProfile`

    Notes
    -----
    This API is evolving.
    """

    def __init__(self) -> None:
        from pyspark.sql import is_remote

        _jvm = None
        if not is_remote():
            from pyspark.core.context import SparkContext

            _jvm = SparkContext._jvm

        if _jvm is not None:
            self._jvm = _jvm
            self._java_resource_profile_builder = getattr(
                _jvm, "org.apache.spark.resource.ResourceProfileBuilder"
            )()
        else:
            self._jvm = None
            self._java_resource_profile_builder = None
            self._executor_resource_requests: Dict[str, ExecutorResourceRequest] = {}
            self._task_resource_requests: Dict[str, TaskResourceRequest] = {}

    def require(
        self, resourceRequest: Union[ExecutorResourceRequests, TaskResourceRequests]
    ) -> "ResourceProfileBuilder":
        """
        Add executor resource requests

        Parameters
        ----------
        resourceRequest : :class:`ExecutorResourceRequests` or :class:`TaskResourceRequests`
            The detailed executor resource requests, see :class:`ExecutorResourceRequests`

        Returns
        -------
        dict
            a dictionary of resources to :class:`ExecutorResourceRequest`
        """

        if isinstance(resourceRequest, TaskResourceRequests):
            if self._java_resource_profile_builder is not None:
                if resourceRequest._java_task_resource_requests is not None:
                    self._java_resource_profile_builder.require(
                        resourceRequest._java_task_resource_requests
                    )
                else:
                    taskReqs = TaskResourceRequests(self._jvm, resourceRequest.requests)
                    self._java_resource_profile_builder.require(
                        taskReqs._java_task_resource_requests
                    )
            else:
                self._task_resource_requests.update(resourceRequest.requests)
        else:
            if self._java_resource_profile_builder is not None:
                r = resourceRequest._java_executor_resource_requests
                if r is not None:
                    self._java_resource_profile_builder.require(r)
                else:
                    execReqs = ExecutorResourceRequests(self._jvm, resourceRequest.requests)
                    self._java_resource_profile_builder.require(
                        execReqs._java_executor_resource_requests
                    )
            else:
                self._executor_resource_requests.update(resourceRequest.requests)
        return self

    def clearExecutorResourceRequests(self) -> None:
        if self._java_resource_profile_builder is not None:
            self._java_resource_profile_builder.clearExecutorResourceRequests()
        else:
            self._executor_resource_requests = {}

    def clearTaskResourceRequests(self) -> None:
        if self._java_resource_profile_builder is not None:
            self._java_resource_profile_builder.clearTaskResourceRequests()
        else:
            self._task_resource_requests = {}

    @property
    def taskResources(self) -> Dict[str, TaskResourceRequest]:
        """
        Returns
        -------
        dict
            a dictionary of resources to :class:`TaskResourceRequest`
        """
        if self._java_resource_profile_builder is not None:
            taskRes = self._java_resource_profile_builder.taskResourcesJMap()
            result = {}
            for k, v in taskRes.items():
                result[k] = TaskResourceRequest(v.resourceName(), v.amount())
            return result
        else:
            return self._task_resource_requests

    @property
    def executorResources(self) -> Dict[str, ExecutorResourceRequest]:
        """
        Returns
        -------
        dict
            a dictionary of resources to :class:`ExecutorResourceRequest`
        """
        if self._java_resource_profile_builder is not None:
            result = {}
            execRes = self._java_resource_profile_builder.executorResourcesJMap()
            for k, v in execRes.items():
                result[k] = ExecutorResourceRequest(
                    v.resourceName(), v.amount(), v.discoveryScript(), v.vendor()
                )
            return result
        else:
            return self._executor_resource_requests

    @property
    def build(self) -> ResourceProfile:
        if self._java_resource_profile_builder is not None:
            jresourceProfile = self._java_resource_profile_builder.build()
            return ResourceProfile(_java_resource_profile=jresourceProfile)
        else:
            return ResourceProfile(
                _exec_req=self._executor_resource_requests, _task_req=self._task_resource_requests
            )


def _test() -> None:
    import doctest
    import sys
    from pyspark import SparkContext

    globs = globals().copy()
    globs["sc"] = SparkContext("local[4]", "profile tests")
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
