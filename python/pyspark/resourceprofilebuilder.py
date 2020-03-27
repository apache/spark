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
from pyspark.resourceprofile import ResourceProfile
from pyspark.taskresourcerequest import TaskResourceRequest
from pyspark.taskresourcerequests import TaskResourceRequests


class ResourceProfileBuilder(object):

    """
    .. note:: Evolving

    Resource profile Builder to build a resource profile to associate with an RDD.
    A ResourceProfile allows the user to specify executor and task requirements for
    an RDD that will get applied during a stage. This allows the user to change the
    resource requirements between stages.
    """

    def __init__(self, ):
        """Create a new ResourceProfileBuilder that wraps the underlying JVM object."""
        from pyspark.context import SparkContext
        self._jResourceProfileBuilder \
            = SparkContext._jvm.org.apache.spark.resource.ResourceProfileBuilder()

    def require(self, resourceRequest):
        if isinstance(resourceRequest, TaskResourceRequests):
            self._jResourceProfileBuilder.require(resourceRequest._javaTaskResourceRequests)
        else:
            self._jResourceProfileBuilder.require(resourceRequest._javaExecutorResourceRequests)
        return self

    def clearExecutorResourceRequests(self):
        self._jResourceProfileBuilder.clearExecutorResourceRequests()

    def clearTaskResourceRequests(self):
        self._jResourceProfileBuilder.clearTaskResourceRequests()

    @property
    def taskResources(self):
        taskRes = self._jResourceProfileBuilder.taskResourcesJMap()
        result = {}
        # convert back to python TaskResourceRequest
        for k, v in taskRes.items():
            result[k] = TaskResourceRequest(v.resourceName(), v.amount())
        return result

    @property
    def executorResources(self):
        execRes = self._jResourceProfileBuilder.executorResourcesJMap()
        result = {}
        # convert back to python ExecutorResourceRequest
        for k, v in execRes.items():
            result[k] = ExecutorResourceRequest(v.resourceName(), v.amount(),
                                                v.discoveryScript(), v.vendor())
        return result

    @property
    def build(self):
        jresourceProfile = self._jResourceProfileBuilder.build()
        return ResourceProfile(jresourceProfile)
