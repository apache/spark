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

from pyspark.resource.taskresourcerequest import TaskResourceRequest


class TaskResourceRequests(object):

    """
    .. note:: Evolving

    A set of task resource requests. This is used in conjuntion with the
    :class:`pyspark.resource.ResourceProfileBuilder` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level.

    .. versionadded:: 3.1.0
    """

    def __init__(self, _jvm=None, _requests=None):
        from pyspark import SparkContext
        _jvm = _jvm or SparkContext._jvm
        if _jvm is not None:
            self._java_task_resource_requests = \
                SparkContext._jvm.org.apache.spark.resource.TaskResourceRequests()
            if _requests is not None:
                if _requests._cpus is not None:
                    self._java_task_resource_requests.cpus(_requests._cpus)
                for r in _requests._custom_resources:
                    self._java_task_resource_requests.resource(r.resourceName, r.amount)
        else:
            self._java_task_resource_requests = None
            self._custom_resources = []
            self._cpus = None

    def cpus(self, amount):
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.cpus(amount)
        else:
            self._cpus = amount
        return self

    def resource(self, resourceName, amount):
        if self._java_task_resource_requests is not None:
            self._java_task_resource_requests.resource(resourceName, float(amount))
        else:
            self._custom_resources.append(TaskResourceRequest(resourceName, amount))
        return self

    @property
    def requests(self):
        result = {}
        if self._java_task_resource_requests is not None:
            taskRes = self._java_task_resource_requests.requestsJMap()
            for k, v in taskRes.items():
                result[k] = TaskResourceRequest(v.resourceName(), v.amount())
        else:
            if self._cpus is not None:
                result["cpus"] = TaskResourceRequest("cpus", self._cpus)
            for t in self._custom_resources:
                result[t.resourceName] = TaskResourceRequest(t.resourceName, t.amount)
        return result
