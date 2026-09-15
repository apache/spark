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

import math
from typing import Union

# Import subpackage examples here explicitly so that
# this module can be run directly with spark-submit.
import pyspark.graphframes.examples
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as sqlfunctions

__all__ = ["BeliefPropagation"]


class BeliefPropagation:
    r"""Example code for Belief Propagation (BP)

    This provides a template for building customized BP algorithms for different types of graphical
    models.

    This example:

    * Ising model on a grid
    * Parallel Belief Propagation using colored fields

    Ising models are probabilistic graphical models over binary variables
    (see :meth:`Graphs.gridIsingModel()`).

    Belief Propagation (BP) provides marginal probabilities of the values of the variables
    x\ :sub:`i` i.e., P(x\ :sub:`i`) for each i.  This allows a user to understand likely values of
    variables. See `Wikipedia <https://en.wikipedia.org/wiki/Belief_propagation>`__ for more
    information on BP.

    We use a batch synchronous BP algorithm, where batches of vertices are updated synchronously.
    We follow the mean field update algorithm in Slide 13 of the
    `talk slides <http://www.eecs.berkeley.edu/~wainwrig/Talks/A_GraphModel_Tutorial>`__ from:
    Wainwright. "Graphical models, message-passing algorithms, and convex optimization."

    The batches are chosen according to a coloring. For background on graph colorings for
    inference, see for example: Gonzalez et al. "Parallel Gibbs Sampling: From Colored Fields to
    Thin Junction Trees." AISTATS, 2011.

    The BP algorithm works by:

    * Coloring the graph by assigning a color to each vertex such that no neighboring vertices
      share the same color.
    * In each step of BP, update all vertices of a single color.  Alternate colors.
    """

    @classmethod
    def runBPwithGraphFrames(cls, g: GraphFrame, numIter: int) -> GraphFrame:
        """Run Belief Propagation using GraphFrame.

        This implementation of BP shows how to use GraphFrame's aggregateMessages method.
        """
        # choose colors for vertices for BP scheduling
        colorG = cls._colorGraph(g)
        numColors = colorG.vertices.select("color").distinct().count()

        # TODO: handle vertices without any edges

        # initialize vertex beliefs at 0.0
        gx = GraphFrame(colorG.vertices.withColumn("belief", sqlfunctions.lit(0.0)), colorG.edges)

        # run BP for numIter iterations
        for iter_ in range(numIter):
            # for each color, have that color receive messages from neighbors
            for color in range(numColors):
                # Send messages to vertices of the current color.
                # We may send to source or destination since edges are treated as undirected.
                msgForSrc = sqlfunctions.when(
                    AM.src["color"] == color, AM.edge["b"] * AM.dst["belief"]
                )
                msgForDst = sqlfunctions.when(
                    AM.dst["color"] == color, AM.edge["b"] * AM.src["belief"]
                )
                # numerically stable sigmoid
                logistic = sqlfunctions.udf(cls._sigmoid, returnType=types.DoubleType())
                aggregates = gx.aggregateMessages(
                    sqlfunctions.sum(AM.msg).alias("aggMess"),
                    sendToSrc=msgForSrc,
                    sendToDst=msgForDst,
                )
                v = gx.vertices
                # receive messages and update beliefs for vertices of the current color
                newBeliefCol = sqlfunctions.when(
                    (v["color"] == color) & (aggregates["aggMess"].isNotNull()),
                    logistic(aggregates["aggMess"] + v["a"]),
                ).otherwise(v["belief"])  # keep old beliefs for other colors
                newVertices = (
                    v.join(aggregates, on=(v["id"] == aggregates["id"]), how="left_outer")
                    .drop(aggregates["id"])  # drop duplicate ID column (from outer join)
                    .withColumn("newBelief", newBeliefCol)  # compute new beliefs
                    .drop("aggMess")  # drop messages
                    .drop("belief")  # drop old beliefs
                    .withColumnRenamed("newBelief", "belief")
                )
                # cache new vertices using workaround for SPARK-1334
                cachedNewVertices = newVertices.localCheckpoint()
                gx = GraphFrame(cachedNewVertices, gx.edges)

        # Drop the "color" column from vertices
        return GraphFrame(gx.vertices.drop("color"), gx.edges)

    @staticmethod
    def _colorGraph(g: GraphFrame) -> GraphFrame:
        """Given a GraphFrame, choose colors for each vertex.

        No neighboring vertices will share the same color. The number of colors is minimized.

        This is written specifically for grid graphs. For non-grid graphs, it should be generalized,
        such as by using a greedy coloring scheme.

        :param g: Grid graph generated by :meth:`Graphs.gridIsingModel()`
        :return: Same graph, but with a new vertex column "color" of type Int (0 or 1)

        """

        colorUDF = sqlfunctions.udf(lambda i, j: (i + j) % 2, returnType=types.IntegerType())
        v = g.vertices.withColumn("color", colorUDF(sqlfunctions.col("i"), sqlfunctions.col("j")))
        return GraphFrame(v, g.edges)

    @staticmethod
    def _sigmoid(x: Union[int, float, None]) -> Union[float, None]:
        """Numerically stable sigmoid function 1 / (1 + exp(-x))"""
        if not x:
            return None
        if x >= 0:
            z = math.exp(-x)
            return 1 / (1 + z)
        else:
            z = math.exp(x)
            return z / (1 + z)


def main() -> None:
    """Run the belief propagation algorithm for an example problem."""
    # setup spark session
    spark = SparkSession.builder.appName("BeliefPropagation example").getOrCreate()

    # create graphical model g of size 3 x 3
    g = pyspark.graphframes.examples.Graphs(spark).gridIsingModel(3)
    print("Original Ising model:")
    g.vertices.show()
    g.edges.show()

    # run BP for 5 iterations
    numIter = 5
    results = BeliefPropagation.runBPwithGraphFrames(g, numIter)

    # display beliefs
    beliefs = results.vertices.select("id", "belief")
    print("Done with BP. Final beliefs after {} iterations:".format(numIter))
    beliefs.show()

    spark.stop()


if __name__ == "__main__":
    main()
