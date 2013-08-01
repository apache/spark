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

from pyspark import SparkContext

numEdges = 200
numVertices = 100
rand = Random(42)


def generateGraph():
    edges = set()
    while len(edges) < numEdges:
        src = rand.randrange(0, numEdges)
        dst = rand.randrange(0, numEdges)
        if src != dst:
            edges.add((src, dst))
    return edges


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print >> sys.stderr, "Usage: transitive_closure <master> [<slices>]"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonTransitiveClosure")
    slices = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    tc = sc.parallelize(generateGraph(), slices).cache()

    # Linear transitive closure: each round grows paths by one edge,
    # by joining the graph's edges with the already-discovered paths.
    # e.g. join the path (y, z) from the TC with the edge (x, y) from
    # the graph to obtain the path (x, z).

    # Because join() joins on keys, the edges are stored in reversed order.
    edges = tc.map(lambda (x, y): (y, x))

    oldCount = 0L
    nextCount = tc.count()
    while True:
        oldCount = nextCount
        # Perform the join, obtaining an RDD of (y, (z, x)) pairs,
        # then project the result to obtain the new (x, z) paths.
        new_edges = tc.join(edges).map(lambda (_, (a, b)): (b, a))
        tc = tc.union(new_edges).distinct().cache()
        nextCount = tc.count()
        if nextCount == oldCount:
            break

    print "TC has %i edges" % tc.count()
