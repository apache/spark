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

"""
Python bindings for GraphX.
"""

__all__ = ["VertexRDD"]

from pyspark import RDD


class VertexRDD(RDD):
    """
    VertexRDD class is used to enter the vertex class for GraphX
    """

    def __init__(self, otherRDD):
        self.__init__(otherRDD._jrdd, otherRDD._ctx, otherRDD._serializer)
        self.setName("VertexRDD")

    def __init__(self, jrdd, ctx, serializer):
        super(jrdd, ctx, serializer)
        self.setName("VertexRDD")

    def filter(self, (vertexId, VertexProperty)):
        return

    def mapValues(self, func):
        self._jrdd._jvm.org.apache.spark.PythonVertexRDD.mapValues()
        return

    def diff(self, other):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.diff()

    def leftJoin(self, other):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.leftJoin()

    def innerJoin(self, other, func):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.innerJoin()

    def aggregateUsingIndex(self, other, reduceFunc):
        return self._jrdd._jvm.org.apache.spark.PythonVertexRDD.aggregateUsingIndex()


class VertexProperty(object):

    def __init__(self, property_name, propertyValue):
        self.name = property_name
        self.value = propertyValue

    def getKey(self):
        return self.name

    @property
    def getValue(self):
        return self.value