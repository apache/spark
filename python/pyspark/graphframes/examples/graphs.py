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

import itertools

from pyspark.graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlfunctions

__all__ = ["Graphs"]


class Graphs:
    """Example GraphFrames for testing the API

    :param spark: SparkSession
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def friends(self) -> GraphFrame:
        """A GraphFrame of friends in a (fake) social network."""
        # Vertex DataFrame
        v = self._spark.createDataFrame(
            [
                ("a", "Alice", 34),
                ("b", "Bob", 36),
                ("c", "Charlie", 30),
                ("d", "David", 29),
                ("e", "Esther", 32),
                ("f", "Fanny", 36),
            ],
            ["id", "name", "age"],
        )
        # Edge DataFrame
        e = self._spark.createDataFrame(
            [
                ("a", "b", "friend"),
                ("b", "c", "follow"),
                ("c", "b", "follow"),
                ("f", "c", "follow"),
                ("e", "f", "follow"),
                ("e", "d", "friend"),
                ("d", "a", "friend"),
            ],
            ["src", "dst", "relationship"],
        )
        # Create a GraphFrame
        return GraphFrame(v, e)

    def gridIsingModel(self, n: int, vStd: float = 1.0, eStd: float = 1.0) -> GraphFrame:
        r"""Grid Ising model with random parameters.

        Ising models are probabilistic graphical models over binary variables x\ :sub:`i`.
        Each binary variable x\ :sub:`i` corresponds to one vertex, and it may take values -1 or +1.
        The probability distribution P(X) (over all x\ :sub:`i`) is parameterized by vertex factors
        a\ :sub:`i` and edge factors b\ :sub:`ij`:

           P(X) = (1/Z) * exp[ \sum_i a_i x_i + \sum_{ij} b_{ij} x_i x_j ]

        where Z is the normalization constant (partition function). See `Wikipedia
        <https://en.wikipedia.org/wiki/Ising_model>`__ for more information on Ising models.

        Each vertex is parameterized by a single scalar a\ :sub:`i`.
        Each edge is parameterized by a single scalar b\ :sub:`ij`.

        :param n: Length of one side of the grid.  The grid will be of size n x n.
        :param vStd: Standard deviation of normal distribution used to generate vertex factors "a".
                     Default of 1.0.
        :param eStd: Standard deviation of normal distribution used to generate edge factors "b".
                     Default of 1.0.
        :return: GraphFrame. Vertices have columns "id" and "a". Edges have columns "src", "dst",
            and "b".  Edges are directed, but they should be treated as undirected in any algorithms
            run on this model. Vertex IDs are of the form "i,j".  E.g., vertex "1,3" is in the
            second row and fourth column of the grid.
        """
        # check param n
        if n < 1:
            raise ValueError(
                "Grid graph must have size >= 1, but was given invalid value n = {}".format(n)
            )

        # create coordinates grid
        coordinates = self._spark.createDataFrame(
            itertools.product(range(n), range(n)), schema=("i", "j")
        )

        # create SQL expression for converting coordinates (i,j) to a string ID "i,j"
        # avoid Cartesian join due to SPARK-15425: use generator since n should be small
        toIDudf = sqlfunctions.udf(lambda i, j: "{},{}".format(i, j))

        # create the vertex DataFrame
        # create SQL expression for converting coordinates (i,j) to a string ID "i,j"
        vIDcol = toIDudf(sqlfunctions.col("i"), sqlfunctions.col("j"))
        # add random parameters generated from a normal distribution
        seed = 12345
        vertices = coordinates.withColumn("id", vIDcol).withColumn(
            "a", sqlfunctions.randn(seed) * vStd
        )

        # create the edge DataFrame
        # create SQL expression for converting coordinates (i,j+1) and (i+1,j) to string IDs
        rightIDcol = toIDudf(sqlfunctions.col("i"), sqlfunctions.col("j") + 1)
        downIDcol = toIDudf(sqlfunctions.col("i") + 1, sqlfunctions.col("j"))
        horizontalEdges = coordinates.filter(sqlfunctions.col("j") != n - 1).select(
            vIDcol.alias("src"), rightIDcol.alias("dst")
        )
        verticalEdges = coordinates.filter(sqlfunctions.col("i") != n - 1).select(
            vIDcol.alias("src"), downIDcol.alias("dst")
        )
        allEdges = horizontalEdges.unionAll(verticalEdges)
        # add random parameters from a normal distribution
        edges = allEdges.withColumn("b", sqlfunctions.randn(seed + 1) * eStd)

        # create the GraphFrame
        g = GraphFrame(vertices, edges)

        # materialize graph as workaround for SPARK-13333
        g.vertices.cache().count()
        g.edges.cache().count()

        return g
