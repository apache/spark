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


class TaskResourceRequest(object):
    """
    .. note:: Evolving

    A task resource request. This is used in conjuntion with the
    :class:`pyspark.resource.ResourceProfile` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level. The amount is specified
    as a Double to allow for saying you want more then 1 task per resource. Valid values
    are less than or equal to 0.5 or whole numbers.
    Use :class:`pyspark.resource.TaskResourceRequests` class as a convenience API.

    :param resourceName: Name of the resource
    :param amount: Amount requesting as a Double to support fractional resource requests.
        Valid values are less than or equal to 0.5 or whole numbers.

    .. versionadded:: 3.1.0
    """
    def __init__(self, resourceName, amount):
        self._name = resourceName
        self._amount = float(amount)

    @property
    def resourceName(self):
        return self._name

    @property
    def amount(self):
        return self._amount


class TaskResourceRequests(object):

    """
    .. note:: Evolving

    A set of task resource requests. This is used in conjuntion with the
    :class:`pyspark.resource.ResourceProfileBuilder` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level.

    .. versionadded:: 3.1.0
    """

    _CPUS = "cpus"

    def __init__(self, _jvm=None, _requests=None):
        from pyspark import SparkContext
        _jvm = _jvm or SparkContext._jvm
        if _jvm is not None:
            self._java_task_resource_requests = \
                SparkContext._jvm.org.apache.spark.resource.TaskResourceRequests()
            if _requests is not None:
                for k, v in _requests.items():
                    if k == self._CPUS:
                        self._java_task_resource_requests.cpus(int(v.amount))
                    else:
                        self._java_task_resource_requests.resource(v.resourceName, v.amount)
        else:
            self._java_task_resource_requests = None
            self._task_resources = {}

    def cpus(self, amount):
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.cpus(amount)
        else:
            self._task_resources[self._CPUS] = TaskResourceRequest(self._CPUS, amount)
        return self

    def resource(self, resourceName, amount):
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.resource(resourceName, float(amount))
        else:
            self._task_resources[resourceName] = TaskResourceRequest(resourceName, amount)
        return self

    @property
    def requests(self):
        if self._java_task_resource_requests is not None:
            result = {}
            taskRes = self._java_task_resource_requests.requestsJMap()
            for k, v in taskRes.items():
                result[k] = TaskResourceRequest(v.resourceName(), v.amount())
            return result
        else:
            return self._task_resources
