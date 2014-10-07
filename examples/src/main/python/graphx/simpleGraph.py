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
Correlations using MLlib.
"""

import sys

from pyspark import SparkContext
from pyspark.graphx import GraphLoader
from pyspark.graphx import Vertex
from pyspark.graphx import Edge

if __name__ == "__main__":

    """
    Usage: simpleGraph filename [partitions]"
    """

    sc = SparkContext(appName="PythonSimpleGraphExample")
    graphFile = int(sys.argv[1]) if len(sys.argv) > 1 else "simplegraph.edges"
    partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 2

    print "Running SimpleGraph example with filename=%s partitions=%d\n" % (graphFile, partitions)

    graph = GraphLoader.edgeListFile(sc, graphFile, partitions)
    vertices = graph.vertices()
    edges = graph.edges





