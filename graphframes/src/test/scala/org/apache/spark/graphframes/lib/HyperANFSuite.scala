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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.hll_sketch_estimate
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils

class HyperANFSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  private def diamondCycleGraph: GraphFrame = {
    val vertices =
      spark.createDataFrame((1L to 5L).map(id => (id, s"v$id"))).toDF("id", "name")
    val edges = spark
      .createDataFrame(Seq((1L, 2L), (1L, 3L), (2L, 4L), (3L, 4L), (4L, 5L), (5L, 1L)))
      .toDF("src", "dst")
    GraphFrame(vertices, edges)
  }

  private def estimateHopCounts(result: DataFrame, nHops: Int): Map[Long, Seq[Long]] = {
    val estimateColumns = (0 to nHops).map { hop =>
      hll_sketch_estimate(col(s"hop_$hop")).alias(s"hop_${hop}_estimate")
    }

    result
      .select((Seq(col("id")) ++ estimateColumns): _*)
      .collect()
      .map { row =>
        row.getAs[Long]("id") -> (0 to nHops).map { hop =>
          row.getAs[Long](s"hop_${hop}_estimate")
        }
      }
      .toMap
  }

  test("HyperANF returns exact 0-hop through 2-hop reachable cardinalities") {
    val graph = diamondCycleGraph
    val result = new HyperANF(graph)
      .setNHops(2)
      .setLgNomEntries(12)
      .run()

    TestUtils.checkColumnType(result.schema, "hop_0", DataTypes.BinaryType)
    TestUtils.checkColumnType(result.schema, "hop_1", DataTypes.BinaryType)
    TestUtils.checkColumnType(result.schema, "hop_2", DataTypes.BinaryType)

    val expected = Map(
      1L -> Seq(1L, 2L, 1L),
      2L -> Seq(1L, 1L, 1L),
      3L -> Seq(1L, 1L, 1L),
      4L -> Seq(1L, 1L, 1L),
      5L -> Seq(1L, 1L, 2L))

    assert(estimateHopCounts(result, 2) === expected)
    result.unpersist()
  }

  test("HyperANF returns exact 0-hop through 3-hop reachable cardinalities") {
    val graph = diamondCycleGraph
    val result = new HyperANF(graph)
      .setNHops(3)
      .setLgNomEntries(12)
      .run()

    TestUtils.checkColumnType(result.schema, "hop_0", DataTypes.BinaryType)
    TestUtils.checkColumnType(result.schema, "hop_1", DataTypes.BinaryType)
    TestUtils.checkColumnType(result.schema, "hop_2", DataTypes.BinaryType)
    TestUtils.checkColumnType(result.schema, "hop_3", DataTypes.BinaryType)

    val expected = Map(
      1L -> Seq(1L, 2L, 1L, 1L),
      2L -> Seq(1L, 1L, 1L, 1L),
      3L -> Seq(1L, 1L, 1L, 1L),
      4L -> Seq(1L, 1L, 1L, 2L),
      5L -> Seq(1L, 1L, 2L, 1L))

    assert(estimateHopCounts(result, 3) === expected)
    result.unpersist()
  }

  test(
    "HyperANF starting vertices expression limits output to selected vertices with outgoing edges") {
    val graph = diamondCycleGraph
    val result = new HyperANF(graph)
      .setEdgesFilterExpression(expr("src IN (1, 3, 42)"))
      .setNHops(2)
      .setLgNomEntries(12)
      .run()

    val ids = result.select("id").collect().map(_.getAs[Long]("id")).toSet

    assert(ids === Set(1L, 3L))
    result.unpersist()
  }

  test("HyperANF does not fail on dead-ends") {
    // Graph: 1 -> 2 -> 3, vertex 4 is isolated (no edges at all)
    // Dead-ends: vertex 3 (no outgoing edges), vertex 4 (no edges at all)
    val vertices = spark
      .createDataFrame((1L to 4L).map(id => (id, s"v$id")))
      .toDF("id", "name")
    val edges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 3L)))
      .toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)

    val nHops = 3
    val result = new HyperANF(graph)
      .setNHops(nHops)
      .setLgNomEntries(12)
      .run()

    val ids = result.select("id").collect().map(_.getAs[Long]("id")).toSet

    // 1. Dead-end vertices are not present in the output
    assert(!ids.contains(3L), "Dead-end vertex 3 (no outgoing edges) should not be in output")
    assert(!ids.contains(4L), "Isolated vertex 4 (no edges at all) should not be in output")
    assert(ids === Set(1L, 2L))

    // 2. hop_2 for vertex 2 reaches dead-end 3, producing an empty sketch (estimate 0, not null)
    val estimates = estimateHopCounts(result, nHops)
    assert(estimates(2L)(2) === 0L, "hop_2 estimate for vertex 2 should be 0")

    val row2 = result.filter(col("id") === 2L).collect()(0)
    assert(
      row2.getAs[Array[Byte]]("hop_2") !== null,
      "hop_2 sketch for vertex 2 should not be null")

    result.unpersist()
  }
}
