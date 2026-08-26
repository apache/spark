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

from pyspark.graphframes import GraphFrame
from pyspark.sql import Row
from pyspark.testing.connectutils import ReusedConnectTestCase


class GraphFrameParityTests(ReusedConnectTestCase):
    def test_dataframe_operations(self) -> None:
        vertices = self.spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["id", "name"],
        )
        edges = self.spark.createDataFrame(
            [(1, 2, "friend"), (2, 3, "follow"), (2, 1, "friend")],
            ["src", "dst", "relationship"],
        )
        graph = GraphFrame(vertices, edges)

        self.assertEqual(
            sorted(graph.outDegrees.collect()),
            [Row(id=1, outDegree=1), Row(id=2, outDegree=2)],
        )
        self.assertEqual(graph.triplets.count(), 3)
        self.assertEqual(graph.filterVertices("id <= 2").edges.count(), 2)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
