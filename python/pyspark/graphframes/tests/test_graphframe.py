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
from pyspark.sql import functions as F
from pyspark.testing.sqlutils import ReusedSQLTestCase


class GraphFrameTests(ReusedSQLTestCase):
    def setUp(self) -> None:
        super().setUp()
        vertices = self.spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c"), (4, "isolated")],
            ["id", "name"],
        )
        edges = self.spark.createDataFrame(
            [(1, 2, "friend"), (2, 3, "follow"), (2, 1, "friend")],
            ["src", "dst", "relationship"],
        )
        self.graph = GraphFrame(vertices, edges)

    def test_degrees(self) -> None:
        self.assertEqual(
            sorted(self.graph.outDegrees.collect()),
            [Row(id=1, outDegree=1), Row(id=2, outDegree=2)],
        )
        self.assertEqual(
            sorted(self.graph.inDegrees.collect()),
            [Row(id=1, inDegree=1), Row(id=2, inDegree=1), Row(id=3, inDegree=1)],
        )
        self.assertEqual(
            sorted(self.graph.degrees.collect()),
            [Row(id=1, degree=2), Row(id=2, degree=3), Row(id=3, degree=1)],
        )

    def test_triplets(self) -> None:
        rows = self.graph.triplets.select(
            F.col("src.id").alias("src_id"),
            F.col("src.name").alias("src_name"),
            F.col("edge.relationship").alias("relationship"),
            F.col("dst.id").alias("dst_id"),
            F.col("dst.name").alias("dst_name"),
        ).collect()
        self.assertEqual(
            sorted(rows),
            [
                Row(
                    src_id=1,
                    src_name="a",
                    relationship="friend",
                    dst_id=2,
                    dst_name="b",
                ),
                Row(
                    src_id=2,
                    src_name="b",
                    relationship="follow",
                    dst_id=3,
                    dst_name="c",
                ),
                Row(
                    src_id=2,
                    src_name="b",
                    relationship="friend",
                    dst_id=1,
                    dst_name="a",
                ),
            ],
        )

    def test_relational_transforms(self) -> None:
        filtered = self.graph.filterVertices(F.col("id") <= 2)
        self.assertEqual(
            sorted(filtered.vertices.collect()),
            [Row(id=1, name="a"), Row(id=2, name="b")],
        )
        self.assertEqual(
            sorted(filtered.edges.collect()),
            [
                Row(src=1, dst=2, relationship="friend"),
                Row(src=2, dst=1, relationship="friend"),
            ],
        )
        self.assertEqual(
            sorted(self.graph.dropIsolatedVertices().vertices.select("id").collect()),
            [Row(id=1), Row(id=2), Row(id=3)],
        )

    def test_validate(self) -> None:
        self.graph.validate()

        vertices = self.spark.createDataFrame([(1,), (1,)], ["id"])
        edges = self.spark.createDataFrame([], "src long, dst long")
        with self.assertRaisesRegex(ValueError, "duplicate vertices"):
            GraphFrame(vertices, edges).validate()


if __name__ == "__main__":
    from pyspark.graphframes.tests.test_graphframe import *  # noqa: F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    import unittest

    unittest.main(testRunner=testRunner, verbosity=2)
