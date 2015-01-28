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

import unittest

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.graphx.vertex import VertexRDD


class PyVertexRDDTestCase(unittest.TestCase):
    """
    Test collect, take, count, mapValues, diff,
    filter, mapVertexPartitions, innerJoin and leftJoin
    for VertexRDD
    """

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        self.sc = SparkContext(appName=class_name, conf=conf)
        self.sc.setCheckpointDir("/tmp")

    def tearDown(self):
        self.sc.stop()

    def collect(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.take(1)
        self.assertEqual(results, [(3, ("rxin", "student"))])

    def take(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, [(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])

    def count(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.count()
        self.assertEqual(results, 2)

    def mapValues(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.mapValues(lambda x: x + ":" + x)
        self.assertEqual(results, [(3, ("rxin:rxin", "student:student")),
                                   (7, ("jgonzal:jgonzal", "postdoc:postdoc"))])

    def innerJoin(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.innerJoin(vertices1).collect()
        self.assertEqual(results, [])

    def leftJoin(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.diff(vertices1)
        self.assertEqual(results, 2)


class PyEdgeRDDTestCase(unittest.TestCase):
    """
    Test collect, take, count, mapValues,
    filter and innerJoin for EdgeRDD
    """

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        self.sc = SparkContext(appName=class_name, conf=conf)
        self.sc.setCheckpointDir("/tmp")

    def tearDown(self):
        self.sc.stop()

    # TODO
    def collect(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, [(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])

    # TODO
    def take(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, [(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])

    # TODO
    def count(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, 2)

    # TODO
    def mapValues(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, 2)

    # TODO
    def filter(self):
        return

    # TODO
    def innerJoin(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.diff(vertices1)
        self.assertEqual(results, 2)


class PyGraphXTestCase(unittest.TestCase):
    """
    Test vertices, edges, partitionBy, numEdges, numVertices,
    inDegrees, outDegrees, degrees, triplets, mapVertices,
    mapEdges, mapTriplets, reverse, subgraph, groupEdges,
    joinVertices, outerJoinVertices, collectNeighborIds,
    collectNeighbors, mapReduceTriplets, triangleCount for Graph
    """

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        self.sc = SparkContext(appName=class_name, conf=conf)
        self.sc.setCheckpointDir("/tmp")

    def tearDown(self):
        self.sc.stop()

    def collect(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, [(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])

    def take(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, [(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])

    def count(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, 2)

    def mapValues(self):
        vertexData = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertices = VertexRDD(vertexData)
        results = vertices.collect()
        self.assertEqual(results, 2)

    def diff(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.diff(vertices1)
        self.assertEqual(results, 2)

    def innerJoin(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.diff(vertices1)
        self.assertEqual(results, 2)

    def leftJoin(self):
        vertexData0 = self.sc.parallelize([(3, ("rxin", "student")), (7, ("jgonzal", "postdoc"))])
        vertexData1 = self.sc.parallelize([(1, ("rxin", "student")), (2, ("jgonzal", "postdoc"))])
        vertices0 = VertexRDD(vertexData0)
        vertices1 = VertexRDD(vertexData1)
        results = vertices0.diff(vertices1)
        self.assertEqual(results, 2)
