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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils
import org.apache.spark.graphframes.examples.Graphs

class PageRankSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  val n = 100L

  test("Star example") {
    val g = Graphs.star(n)
    val resetProb = 0.15
    val errorTol = 1.0e-5
    val pr = g.pageRank
      .resetProbability(resetProb)
      .tol(errorTol)
      .run()
    TestUtils.testSchemaInvariants(g, pr)
    TestUtils.checkColumnType(pr.vertices.schema, "pagerank", DataTypes.DoubleType)
    TestUtils.checkColumnType(pr.edges.schema, "weight", DataTypes.DoubleType)
    pr.unpersist()
  }

  test("friends graph with personalized PageRank") {
    val results = Graphs.friends.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

    val gRank = results.vertices.filter(col("id") === "g").select("pagerank").first().getDouble(0)
    assert(
      gRank === 0.0,
      s"User g (Gabby) doesn't connect with a. So its pagerank should be 0 but we got $gRank.")
    results.unpersist()
  }

  test("graph with three disconnected components") {
    import sqlImplicits._

    val v = Seq((0L, "a"), (1L, "b"), (2L, "c"), (3L, "d"), (4L, "e"), (5L, "f"), (6L, "g"))
      .toDF("id", "name")

    val e = Seq(
      (0L, 1L, "friend"), // First component: a->b->c
      (1L, 2L, "friend"),
      (3L, 4L, "friend"), // Second component: d->e->f
      (4L, 5L, "friend")
      // Third component: isolated vertex g (6)
    ).toDF("src", "dst", "relationship")

    val g = org.apache.spark.graphframes.GraphFrame(v, e)
    val results = g.pageRank.resetProbability(0.15).maxIter(10).run()

    // Verify that all original vertices are present in the result
    assert(
      results.vertices.count() === v.count(),
      "PageRank results should contain all original vertices")

    val originalIds = v.select("id").collect().map(_.getLong(0)).toSet
    val resultIds = results.vertices.select("id").collect().map(_.getLong(0)).toSet
    assert(originalIds === resultIds, "PageRank results should preserve all vertex IDs")
    results.unpersist()
  }
}
