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

import sys
from random import Random
from typing import Set, Tuple

from pyspark.sql import SparkSession

numEdges = 200
numVertices = 100
rand = Random(42)


def generateGraph() -> Set[Tuple[int, int]]:
    edges: Set[Tuple[int, int]] = set()
    while len(edges) < numEdges:
        src = rand.randrange(0, numVertices)
        dst = rand.randrange(0, numVertices)
        if src != dst:
            edges.add((src, dst))
    return edges


if __name__ == "__main__":
    """
    Usage: transitive_closure [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonTransitiveClosure")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    tc = spark.sparkContext.parallelize(generateGraph(), partitions).cache()

    # Linear transitive closure: each round grows paths by one edge,
    # by joining the graph's edges with the already-discovered paths.
    # e.g. join the path (y, z) from the TC with the edge (x, y) from
    # the graph to obtain the path (x, z).

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = tc.map(lambda x_y: (x_y[1], x_y[0]))

    oldCount = 0
    nextCount = tc.count()
    while True:
        oldCount = nextCount
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = tc.join(edges).map(lambda __a_b: (__a_b[1][1], __a_b[1][0]))
        tc = tc.union(new_edges).distinct().cache()
        nextCount = tc.count()
        if nextCount == oldCount:
            break

    print("TC has %i edges" % tc.count())

    spark.stop()
