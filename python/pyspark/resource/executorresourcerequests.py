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

from pyspark.resource.executorresourcerequest import ExecutorResourceRequest


class ExecutorResourceRequests(object):

    """
    .. note:: Evolving

    A set of Executor resource requests. This is used in conjunction with the
    ResourceProfileBuilder to programmatically specify the resources needed for an RDD
    that will be applied at the stage level.

    .. versionadded:: 3.1.0
    """

    def __init__(self, _jvm = None, _requests = None):
        """
        Create a new :class:`pyspark.resource.ExecutorResourceRequests` that wraps the
        underlying JVM object.
        """
        from pyspark import SparkContext
        _jvm = _jvm or SparkContext._jvm
        if _jvm is not None:
            self._java_executor_resource_requests = \
                _jvm.org.apache.spark.resource.ExecutorResourceRequests()
            if _requests is not None:
                self._java_executor_resource_requests.memory(_requests._memory)
                self._java_executor_resource_requests.cores(_requests._cores)
        else:
            self._java_executor_resource_requests = None
            self._custom_resources = []

    def memory(self, amount):
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.memory(amount)
        else:
            self._memory = amount
        return self
            

    def memoryOverhead(self, amount):
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.memoryOverhead(amount)
        else:
            self._overhead_memory = amount
        return self

    def pysparkMemory(self, amount):
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.pysparkMemory(amount)
        else:
            self._pyspark_memory = amount
        return self

    def cores(self, amount):
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.cores(amount)
        else:
            self._cores = amount
        return self

    def resource(self, resourceName, amount, discoveryScript="", vendor=""):
        if self._java_executor_resource_requests is not None:
            self._java_executor_resource_requests.resource(resourceName, amount, discoveryScript, vendor)
        else:
            self._custom_resources.append(ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor))
        return self

    @property
    def requests(self):
        if self._java_executor_resource_requests is not None:
            execRes = self._java_executor_resource_requests.requestsJMap()
        else:
            # TODO
            execRes = self._custom_resources
        result = {}
        # convert back to python ExecutorResourceRequest
        for k, v in execRes.items():
            result[k] = ExecutorResourceRequest(v.resourceName(), v.amount(),
                                                v.discoveryScript(), v.vendor())
        return result
