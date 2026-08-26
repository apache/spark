/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphframes.lib
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils
import org.apache.spark.graphframes.examples.Graphs

class ParallelPersonalizedPageRankSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  val n = 100L

  test("Illegal function call argument setting") {
    val g = Graphs.star(n)
    val vertexIds: Array[Any] = Array(1L, 2L, 3L)

    // Not providing number of iterations
    intercept[IllegalArgumentException] {
      g.parallelPersonalizedPageRank.sourceIds(vertexIds).run()
    }

    // Not providing sourceIds
    intercept[IllegalArgumentException] {
      g.parallelPersonalizedPageRank.maxIter(15).run()
    }

    // Provided empty sourceIds
    intercept[IllegalArgumentException] {
      g.parallelPersonalizedPageRank.maxIter(15).sourceIds(Array()).run()
    }
  }

  test("Star example parallel personalized PageRank") {
    val g = Graphs.star(n)
    val resetProb = 0.15
    val maxIter = 10
    val vertexIds: Array[Any] = Array(1L, 2L, 3L)

    lazy val prc = g.parallelPersonalizedPageRank
      .maxIter(maxIter)
      .sourceIds(vertexIds)
      .resetProbability(resetProb)

    val pr = prc.run()
    TestUtils.testSchemaInvariants(g, pr)
    TestUtils.checkColumnType(pr.vertices.schema, "pageranks", SQLDataTypes.VectorType)
    TestUtils.checkColumnType(pr.edges.schema, "weight", DataTypes.DoubleType)
    pr.unpersist()
  }

  test("friends graph with parallel personalized PageRank") {
    val g = Graphs.friends
    val resetProb = 0.15
    val maxIter = 10
    val vertexIds: Array[Any] = Array("a")
    lazy val prc = g.parallelPersonalizedPageRank
      .maxIter(maxIter)
      .sourceIds(vertexIds)
      .resetProbability(resetProb)

    val pr = prc.run()
    val prInvalid = pr.vertices
      .select("pageranks")
      .collect()
      .filter { (row: Row) =>
        vertexIds.size != row.getAs[SparseVector](0).size
      }
    assert(
      prInvalid.size === 0,
      s"found ${prInvalid.size} entries with invalid number of returned personalized pagerank vector")

    val gRank = pr.vertices
      .filter(col("id") === "g")
      .select("pageranks")
      .first()
      .getAs[SparseVector](0)
    assert(
      gRank.numNonzeros === 0,
      s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got ${gRank.numNonzeros}.")
    pr.unpersist()
  }
}
