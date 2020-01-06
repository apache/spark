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

from pyspark.taskresourcerequest import TaskResourceRequest


class TaskResourceRequests(object):

    """
    .. note:: Evolving

    A set of task resource requests. This is used in conjuntion with the
    ResourceProfileBuilder to programmatically specify the resources needed for
    an RDD that will be applied at the stage level.
    """

    def __init__(self):
        """Create a new TaskResourceRequests that wraps the underlying JVM object."""
        from pyspark import SparkContext
        self._javaTaskResourceRequests \
            = SparkContext._jvm.org.apache.spark.resource.TaskResourceRequests()

    def cpus(self, amount):
        self._javaTaskResourceRequests.cpus(amount)
        return self

    def resource(self, resourceName, amount):
        self._javaTaskResourceRequests.resource(resourceName, float(amount))
        return self

    @property
    def requests(self):
        taskRes = self._javaTaskResourceRequests.requestsJMap()
        result = {}
        # convert back to python TaskResourceRequest
        for k, v in taskRes.items():
            result[k] = TaskResourceRequest(v.resourceName(), v.amount())
        return result
