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
from typing import overload, Optional, Dict, TYPE_CHECKING

from pyspark.util import _parse_memory

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject, JVMView


class ExecutorResourceRequest:
    """
    An Executor resource request. This is used in conjunction with the ResourceProfile to
    programmatically specify the resources needed for an RDD that will be applied at the
    stage level.

    This is used to specify what the resource requirements are for an Executor and how
    Spark can find out specific details about those resources. Not all the parameters are
    required for every resource type. Resources like GPUs are supported and have same limitations
    as using the global spark configs spark.executor.resource.gpu.*. The amount, discoveryScript,
    and vendor parameters for resources are all the same parameters a user would specify through the
    configs: spark.executor.resource.{resourceName}.{amount, discoveryScript, vendor}.

    For instance, a user wants to allocate an Executor with GPU resources on YARN. The user has
    to specify the resource name (gpu), the amount or number of GPUs per Executor,
    the discovery script would be specified so that when the Executor starts up it can
    discovery what GPU addresses are available for it to use because YARN doesn't tell
    Spark that, then vendor would not be used because its specific for Kubernetes.

    See the configuration and cluster specific docs for more details.

    Use :class:`pyspark.ExecutorResourceRequests` class as a convenience API.

    .. versionadded:: 3.1.0

    Parameters
    ----------
    resourceName : str
        Name of the resource
    amount : str
        Amount requesting
    discoveryScript : str, optional
        Optional script used to discover the resources. This is required on some
        cluster managers that don't tell Spark the addresses of the resources
        allocated. The script runs on Executors startup to discover the addresses
        of the resources available.
    vendor : str, optional
        Vendor, required for some cluster managers

    See Also
    --------
    :class:`pyspark.resource.ResourceProfile`

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        resourceName: str,
        amount: int,
        discoveryScript: str = "",
        vendor: str = "",
    ):
        self._name = resourceName
        self._amount = amount
        self._discovery_script = discoveryScript
        self._vendor = vendor

    @property
    def resourceName(self) -> str:
        """
        Returns
        -------
        str
            Name of the resource
        """
        return self._name

    @property
    def amount(self) -> int:
        """
        Returns
        -------
        str
            Amount requesting
        """
        return self._amount

    @property
    def discoveryScript(self) -> str:
        """
        Returns
        -------
        str
            Amount requesting
        """
        return self._discovery_script

    @property
    def vendor(self) -> str:
        """
        Returns
        -------
        str
            Vendor, required for some cluster managers
        """
        return self._vendor


class ExecutorResourceRequests:

    """
    A set of Executor resource requests. This is used in conjunction with the
    :class:`pyspark.resource.ResourceProfileBuilder` to programmatically specify the
    resources needed for an RDD that will be applied at the stage level.

    .. versionadded:: 3.1.0

    See Also
    --------
    :class:`pyspark.resource.ResourceProfile`

    Notes
    -----
    This API is evolving.
    """

    _CORES = "cores"
    _MEMORY = "memory"
    _OVERHEAD_MEM = "memoryOverhead"
    _PYSPARK_MEM = "pyspark.memory"
    _OFFHEAP_MEM = "offHeap"

    @overload
    def __init__(self, _jvm: "JVMView"):
        ...

    @overload
    def __init__(
        self,
        _jvm: None = ...,
        _requests: Optional[Dict[str, ExecutorResourceRequest]] = ...,
    ):
        ...

    def __init__(
        self,
        _jvm: Optional["JVMView"] = None,
        _requests: Optional[Dict[str, ExecutorResourceRequest]] = None,
    ):
        from pyspark.sql import is_remote

        jvm = None
        if not is_remote():
            from pyspark.core.context import SparkContext

            jvm = _jvm or SparkContext._jvm

        if jvm is not None:
            self._java_executor_resource_requests = getattr(
                jvm, "org.apache.spark.resource.ExecutorResourceRequests"
            )()
            if _requests is not None:
                for k, v in _requests.items():
                    if k == self._MEMORY:
                        self._java_executor_resource_requests.memory(str(v.amount))
                    elif k == self._OVERHEAD_MEM:
                        self._java_executor_resource_requests.memoryOverhead(str(v.amount))
                    elif k == self._PYSPARK_MEM:
                        self._java_executor_resource_requests.pysparkMemory(str(v.amount))
                    elif k == self._CORES:
                        self._java_executor_resource_requests.cores(v.amount)
                    else:
                        self._java_executor_resource_requests.resource(
                            v.resourceName, v.amount, v.discoveryScript, v.vendor
                        )
        else:
            self._java_executor_resource_requests = None
            self._executor_resources: Dict[str, ExecutorResourceRequest] = {}

    def memory(self, amount: str) -> "ExecutorResourceRequests":
        """
        Specify heap memory. The value specified will be converted to MiB.
        This is a convenient API to add :class:`ExecutorResourceRequest` for "memory" resource.

        Parameters
        ----------
        amount : str
            Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
            Default unit is MiB if not specified.

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.memory(amount)
        else:
            self._executor_resources[self._MEMORY] = ExecutorResourceRequest(
                self._MEMORY, _parse_memory(amount)
            )
        return self

    def memoryOverhead(self, amount: str) -> "ExecutorResourceRequests":
        """
        Specify overhead memory. The value specified will be converted to MiB.
        This is a convenient API to add :class:`ExecutorResourceRequest` for "memoryOverhead"
        resource.

        Parameters
        ----------
        amount : str
            Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
            Default unit is MiB if not specified.

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.memoryOverhead(amount)
        else:
            self._executor_resources[self._OVERHEAD_MEM] = ExecutorResourceRequest(
                self._OVERHEAD_MEM, _parse_memory(amount)
            )
        return self

    def pysparkMemory(self, amount: str) -> "ExecutorResourceRequests":
        """
        Specify pyspark memory. The value specified will be converted to MiB.
        This is a convenient API to add :class:`ExecutorResourceRequest` for "pyspark.memory"
        resource.

        Parameters
        ----------
        amount : str
            Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
            Default unit is MiB if not specified.

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.pysparkMemory(amount)
        else:
            self._executor_resources[self._PYSPARK_MEM] = ExecutorResourceRequest(
                self._PYSPARK_MEM, _parse_memory(amount)
            )
        return self

    def offheapMemory(self, amount: str) -> "ExecutorResourceRequests":
        """
        Specify off heap memory. The value specified will be converted to MiB.
        This value only take effect when MEMORY_OFFHEAP_ENABLED is true.
        This is a convenient API to add :class:`ExecutorResourceRequest` for "offHeap"
        resource.

        Parameters
        ----------
        amount : str
            Amount of memory. In the same format as JVM memory strings (e.g. 512m, 2g).
            Default unit is MiB if not specified.

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.offHeapMemory(amount)
        else:
            self._executor_resources[self._OFFHEAP_MEM] = ExecutorResourceRequest(
                self._OFFHEAP_MEM, _parse_memory(amount)
            )
        return self

    def cores(self, amount: int) -> "ExecutorResourceRequests":
        """
        Specify number of cores per Executor.
        This is a convenient API to add :class:`ExecutorResourceRequest` for "cores" resource.

        Parameters
        ----------
        amount : int
            Number of cores to allocate per Executor.

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.cores(amount)
        else:
            self._executor_resources[self._CORES] = ExecutorResourceRequest(self._CORES, amount)
        return self

    def resource(
        self,
        resourceName: str,
        amount: int,
        discoveryScript: str = "",
        vendor: str = "",
    ) -> "ExecutorResourceRequests":
        """
        Amount of a particular custom resource(GPU, FPGA, etc) to use. The resource names supported
        correspond to the regular Spark configs with the prefix removed. For instance, resources
        like GPUs are gpu (spark configs `spark.executor.resource.gpu.*`). If you pass in a resource
        that the cluster manager doesn't support the result is undefined, it may error or may just
        be ignored.
        This is a convenient API to add :class:`ExecutorResourceRequest` for custom resources.

        Parameters
        ----------
        resourceName : str
            Name of the resource.
        amount : str
            amount of that resource per executor to use.
        discoveryScript : str, optional
            Optional script used to discover the resources. This is required on
            some cluster managers that don't tell Spark the addresses of
            the resources allocated. The script runs on Executors startup to
            of the resources available.
        vendor : str
            Optional vendor, required for some cluster managers

        Returns
        -------
        :class:`ExecutorResourceRequests`
        """
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.resource(
                resourceName, amount, discoveryScript, vendor
            )
        else:
            self._executor_resources[resourceName] = ExecutorResourceRequest(
                resourceName, amount, discoveryScript, vendor
            )
        return self

    @property
    def requests(self) -> Dict[str, ExecutorResourceRequest]:
        """
        Returns
        -------
        dict
            Returns all the resource requests for the executor.
        """
        if self._java_executor_resource_requests is not None:
            result = {}
            execRes = self._java_executor_resource_requests.requestsJMap()
            for k, v in execRes.items():
                result[k] = ExecutorResourceRequest(
                    v.resourceName(), v.amount(), v.discoveryScript(), v.vendor()
                )
            return result
        else:
            return self._executor_resources


class TaskResourceRequest:
    """
    A task resource request. This is used in conjunction with the
    :class:`pyspark.resource.ResourceProfile` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level. The amount is specified
    as a float to allow for saying you want more than 1 task per resource. Valid values
    are less than or equal to 0.5 or whole numbers.
    Use :class:`pyspark.resource.TaskResourceRequests` class as a convenience API.

    Parameters
    ----------
    resourceName : str
        Name of the resource
    amount : float
        Amount requesting as a float to support fractional resource requests.
        Valid values are less than or equal to 0.5 or whole numbers. This essentially
        lets you configure X number of tasks to run on a single resource,
        ie amount equals 0.5 translates into 2 tasks per resource address.

    .. versionadded:: 3.1.0

    See Also
    --------
    :class:`pyspark.resource.ResourceProfile`

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, resourceName: str, amount: float):
        self._name = resourceName
        self._amount = float(amount)

    @property
    def resourceName(self) -> str:
        """
        Returns
        -------
        str
            Name of the resource.
        """
        return self._name

    @property
    def amount(self) -> float:
        """
        Returns
        -------
        str
            Amount requesting as a float to support fractional resource requests.
        """
        return self._amount


class TaskResourceRequests:

    """
    A set of task resource requests. This is used in conjunction with the
    :class:`pyspark.resource.ResourceProfileBuilder` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level.

    .. versionadded:: 3.1.0

    See Also
    --------
    :class:`pyspark.resource.ResourceProfile`

    Notes
    -----
    This API is evolving.
    """

    _CPUS = "cpus"

    @overload
    def __init__(self, _jvm: "JVMView"):
        ...

    @overload
    def __init__(
        self,
        _jvm: None = ...,
        _requests: Optional[Dict[str, TaskResourceRequest]] = ...,
    ):
        ...

    def __init__(
        self,
        _jvm: Optional["JVMView"] = None,
        _requests: Optional[Dict[str, TaskResourceRequest]] = None,
    ):
        from pyspark.sql import is_remote

        jvm = None
        if not is_remote():
            from pyspark.core.context import SparkContext

            jvm = _jvm or SparkContext._jvm

        if jvm is not None:
            self._java_task_resource_requests: Optional["JavaObject"] = getattr(
                jvm, "org.apache.spark.resource.TaskResourceRequests"
            )()
            if _requests is not None:
                for k, v in _requests.items():
                    if k == self._CPUS:
                        self._java_task_resource_requests.cpus(int(v.amount))
                    else:
                        self._java_task_resource_requests.resource(v.resourceName, v.amount)
        else:
            self._java_task_resource_requests = None
            self._task_resources: Dict[str, TaskResourceRequest] = {}

    def cpus(self, amount: int) -> "TaskResourceRequests":
        """
        Specify number of cpus per Task.
        This is a convenient API to add :class:`TaskResourceRequest` for cpus.

        Parameters
        ----------
        amount : int
            Number of cpus to allocate per Task.

        Returns
        -------
        :class:`TaskResourceRequests`
        """
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.cpus(amount)
        else:
            self._task_resources[self._CPUS] = TaskResourceRequest(self._CPUS, amount)
        return self

    def resource(self, resourceName: str, amount: float) -> "TaskResourceRequests":
        """
        Amount of a particular custom resource(GPU, FPGA, etc) to use.
        This is a convenient API to add :class:`TaskResourceRequest` for custom resources.

        Parameters
        ----------
        resourceName : str
            Name of the resource.
        amount : float
            Amount requesting as a float to support fractional resource requests.
            Valid values are less than or equal to 0.5 or whole numbers. This essentially
            lets you configure X number of tasks to run on a single resource,
            ie amount equals 0.5 translates into 2 tasks per resource address.

        Returns
        -------
        :class:`TaskResourceRequests`
        """
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.resource(resourceName, float(amount))
        else:
            self._task_resources[resourceName] = TaskResourceRequest(resourceName, amount)
        return self

    @property
    def requests(self) -> Dict[str, TaskResourceRequest]:
        """
        Returns
        -------
        dict
            Returns all the resource requests for the task.
        """
        if self._java_task_resource_requests is not None:
            result = {}
            taskRes = self._java_task_resource_requests.requestsJMap()
            for k, v in taskRes.items():
                result[k] = TaskResourceRequest(v.resourceName(), v.amount())
            return result
        else:
            return self._task_resources
