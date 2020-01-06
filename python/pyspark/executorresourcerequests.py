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

from pyspark.executorresourcerequest import ExecutorResourceRequest


class ExecutorResourceRequests(object):

    """
    .. note:: Evolving

    A set of Executor resource requests. This is used in conjunction with the
    ResourceProfileBuilder to programmatically specify the resources needed for an RDD
    that will be applied at the stage level.
    """

    def __init__(self):
        """Create a new ExecutorResourceRequests that wraps the underlying JVM object."""
        from pyspark import SparkContext
        self._javaExecutorResourceRequests \
            = SparkContext._jvm.org.apache.spark.resource.ExecutorResourceRequests()

    def memory(self, amount):
        self._javaExecutorResourceRequests.memory(amount)
        return self

    def memoryOverhead(self, amount):
        self._javaExecutorResourceRequests.memoryOverhead(amount)
        return self

    def pysparkMemory(self, amount):
        self._javaExecutorResourceRequests.pysparkMemory(amount)
        return self

    def cores(self, amount):
        self._javaExecutorResourceRequests.cores(amount)
        return self

    def resource(self, resourceName, amount, discoveryScript="", vendor=""):
        self._javaExecutorResourceRequests.resource(resourceName, amount, discoveryScript, vendor)
        return self

    @property
    def requests(self):
        execRes = self._javaExecutorResourceRequests.requestsJMap()
        result = {}
        # convert back to python ExecutorResourceRequest
        for k, v in execRes.items():
            result[k] = ExecutorResourceRequest(v.resourceName(), v.amount(),
                                                v.discoveryScript(), v.vendor())
        return result
