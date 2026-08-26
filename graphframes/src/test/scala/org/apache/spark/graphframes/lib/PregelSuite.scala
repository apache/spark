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

import org.apache.spark.sql.functions._
import org.apache.spark.graphframes._
import org.scalactic.Tolerance._

class PregelSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits._

  Seq(true, false).foreach(useLocalCheckpoint => {
    test(s"page rank${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val edges = Seq(
        (0L, 1L),
        (1L, 2L),
        (2L, 4L),
        (2L, 0L),
        (3L, 4L), // 3 has no in-links
        (4L, 0L),
        (4L, 2L)).toDF("src", "dst").cache()
      val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
      val numVertices = vertices.count()
      val graph = GraphFrame(vertices, edges)

      val alpha = 0.15
      // NOTE: This version doesn't handle nodes with no out-links.
      val ranks = graph.pregel
        .setMaxIter(5)
        .withVertexColumn(
          "rank",
          lit(1.0 / numVertices),
          coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
        .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
        .aggMsgs(sum(Pregel.msg))
        .run()

      val result = ranks
        .sort(col("id"))
        .select("rank")
        .as[Double]
        .collect()
      assert(result.sum === 1.0 +- 1e-6)
      val expected = Seq(0.245, 0.224, 0.303, 0.03, 0.197)
      result.zip(expected).foreach { case (r, e) =>
        assert(r === e +- 1e-3)
      }
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"chain propagation${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x, x + 1))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(n - 1)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(
      s"reverse chain propagation${if (useLocalCheckpoint) " with local checkpoint" else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x + 1, x))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(n - 1)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToSrc(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.dst("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }

    test(s"chain propagation with termination${if (useLocalCheckpoint) " with local checkpoint"
      else ""}") {
      spark.conf.set("spark.graphframes.useLocalCheckpoints", useLocalCheckpoint.toString)
      val n = 5
      val verDF = (1 to n).toDF("id").repartition(3)
      val edgeDF = (1 until n)
        .map(x => (x, x + 1))
        .toDF("src", "dst")
        .repartition(3)

      val graph = GraphFrame(verDF, edgeDF)

      val resultDF = graph.pregel
        .setMaxIter(1000)
        .setEarlyStopping(true)
        .withVertexColumn(
          "value",
          when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
          when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
        .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
        .aggMsgs(max(Pregel.msg))
        .run()

      assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
      spark.conf.set("spark.graphframes.useLocalCheckpoints", "false")
    }
  })

  test("new vertex column is based on the nullable column") {
    val verDF = Seq(1L, 2L, 3L, 4L)
      .toDF("id")
      .withColumn(
        "nullableColumn",
        when(col("id") % lit(2) === lit(0), lit(null)).otherwise(lit(1)))
    val edgeDF = Seq((1L, 2L), (2L, 3L), (3L, 4L), (4L, 1L)).toDF("src", "dst")
    val graph = GraphFrame(verDF, edgeDF)
    val pregel = graph.pregel
      .withVertexColumn(
        "newColumn",
        when(col("nullableColumn").isNull, lit(0)).otherwise(lit(1)),
        col("newColumn") + Pregel.msg)
      .sendMsgToDst(lit(1))
      .aggMsgs(last(Pregel.msg))
      .setCheckpointInterval(0)
      .setMaxIter(1)

    val resultDF = pregel.run()
    assert(
      resultDF
        .select("id", "newColumn")
        .collect()
        .map(r => r.getAs[Long]("id") -> r.getAs[Int]("newColumn"))
        .toMap === Map(1L -> 2, 2L -> 1, 3L -> 2, 4L -> 1))
  }

  test("requiredSrcColumns - only specified columns are used in triplets") {
    // Test that requiredSrcColumns correctly limits the columns in triplets
    // This is a memory optimization test - we verify the result is correct
    // with only required source columns

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    // PageRank only needs "rank" and "outDegree" from source vertex
    val ranks = graph.pregel
      .setMaxIter(5)
      .withVertexColumn(
        "rank",
        lit(1.0 / numVertices),
        coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
      .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
      .aggMsgs(sum(Pregel.msg))
      .requiredSrcColumns("rank", "outDegree")
      .run()

    val result = ranks
      .sort(col("id"))
      .select("rank")
      .as[Double]
      .collect()
    assert(result.sum === 1.0 +- 1e-6)
    val expected = Seq(0.245, 0.224, 0.303, 0.03, 0.197)
    result.zip(expected).foreach { case (r, e) =>
      assert(r === e +- 1e-3)
    }
  }

  test("requiredDstColumns - only specified columns are used in triplets") {
    // Test that requiredDstColumns correctly limits the columns in triplets
    // Reverse chain propagation where we only need dst("value") from destination

    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n)
      .map(x => (x + 1, x))
      .toDF("src", "dst")
      .repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .withVertexColumn(
        "value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
      .sendMsgToSrc(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.dst("value")))
      .aggMsgs(max(Pregel.msg))
      .requiredDstColumns("value") // Only need "value" from destination
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("requiredSrcColumns and requiredDstColumns together") {
    // Test using both requiredSrcColumns and requiredDstColumns
    // Chain propagation where we need "value" from both src and dst

    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n)
      .map(x => (x, x + 1))
      .toDF("src", "dst")
      .repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .withVertexColumn(
        "value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
      .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
      .aggMsgs(max(Pregel.msg))
      .requiredSrcColumns("value") // Only need "value" from source
      .requiredDstColumns("value") // Only need "value" from destination
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("requiredSrcColumns with empty list uses all columns (default behavior)") {
    // Verify that not calling requiredSrcColumns means all columns are used
    // This is the same as the original page rank test

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    val ranks = graph.pregel
      .setMaxIter(5)
      .withVertexColumn(
        "rank",
        lit(1.0 / numVertices),
        coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
      .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
      .aggMsgs(sum(Pregel.msg))
      // No requiredSrcColumns or requiredDstColumns - should use all columns
      .run()

    val result = ranks
      .sort(col("id"))
      .select("rank")
      .as[Double]
      .collect()
    assert(result.sum === 1.0 +- 1e-6)
  }

  test("automatic dst join skipping - PageRank only uses src columns") {
    // PageRank only references Pregel.src("rank") and Pregel.src("outDegree"),
    // so the second join (for dst vertex state) should be automatically skipped.
    // This test verifies the optimization produces correct results.

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 4L), (2L, 0L), (3L, 4L), (4L, 0L), (4L, 2L))
      .toDF("src", "dst")
      .cache()
    val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
    val numVertices = vertices.count()
    val graph = GraphFrame(vertices, edges)

    val alpha = 0.15
    // PageRank only uses Pregel.src(...) - dst state should be automatically skipped
    val ranks = graph.pregel
      .setMaxIter(5)
      .withVertexColumn(
        "rank",
        lit(1.0 / numVertices),
        coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
      .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    val result = ranks
      .sort(col("id"))
      .select("rank")
      .as[Double]
      .collect()
    assert(result.sum === 1.0 +- 1e-6)
    val expected = Seq(0.245, 0.224, 0.303, 0.03, 0.197)
    result.zip(expected).foreach { case (r, e) =>
      assert(r === e +- 1e-3)
    }
  }

  test("automatic dst join NOT skipped when dst columns are referenced") {
    // This test uses Pregel.dst("value") in the message expression,
    // so the second join must NOT be skipped.

    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n)
      .map(x => (x, x + 1))
      .toDF("src", "dst")
      .repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .withVertexColumn(
        "value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
      // This references BOTH src and dst - dst join should NOT be skipped
      .sendMsgToDst(when(Pregel.dst("value") =!= Pregel.src("value"), Pregel.src("value")))
      .aggMsgs(max(Pregel.msg))
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("automatic dst join skipping with skipMessagesFromNonActiveVertices enabled") {
    // When skipMessagesFromNonActiveVertices is true but message expressions don't
    // reference dst columns, the dst join is still skipped. Active-vertex filtering
    // is pushed before the src-edge join to reduce data volume.

    val n = 5
    val verDF = (1 to n).toDF("id").repartition(3)
    val edgeDF = (1 until n)
      .map(x => (x, x + 1))
      .toDF("src", "dst")
      .repartition(3)

    val graph = GraphFrame(verDF, edgeDF)

    // This only uses Pregel.src("value") - dst join should be skipped,
    // and active-vertex filtering is applied before the src-edge join.
    val resultDF = graph.pregel
      .setMaxIter(n - 1)
      .setSkipMessagesFromNonActiveVertices(true)
      .setUpdateActiveVertexExpression(Pregel.msg.isNotNull)
      .withVertexColumn(
        "value",
        when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
        when(Pregel.msg > col("value"), Pregel.msg).otherwise(col("value")))
      .sendMsgToDst(Pregel.src("value"))
      .aggMsgs(max(Pregel.msg))
      .run()

    assert(resultDF.sort("id").select("value").as[Int].collect() === Array.fill(n)(1))
  }

  test("automatic dst join skipping - sendMsgToSrc with only edge columns") {
    // When sending messages to src using only edge columns, dst join should be skipped

    val edges = Seq((1L, 0L, 10L), (2L, 1L, 20L), (3L, 2L, 30L), (4L, 3L, 40L))
      .toDF("src", "dst", "weight")
      .cache()
    val vertices = (0L to 4L).toDF("id").cache()

    val graph = GraphFrame(vertices, edges)

    // Only uses Pregel.edge("weight") - dst join should be skipped
    val resultDF = graph.pregel
      .requiredEdgeColumns("weight")
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToSrc(Pregel.edge("weight"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Each src vertex receives the weight from its outgoing edge
    val received = resultDF.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no outgoing edges
    assert(received(1) === 10L) // vertex 1: edge 1->0 with weight 10
    assert(received(2) === 20L) // vertex 2: edge 2->1 with weight 20
    assert(received(3) === 30L) // vertex 3: edge 3->2 with weight 30
    assert(received(4) === 40L) // vertex 4: edge 4->3 with weight 40
  }

  test("automatic dst join skipping - edge columns only") {
    // When message expressions only reference edge columns, dst join should be skipped

    val edges =
      Seq((0L, 1L, 1.0), (1L, 2L, 2.0), (2L, 3L, 3.0)).toDF("src", "dst", "weight").cache()
    val vertices = Seq(0L, 1L, 2L, 3L).toDF("id").cache()
    val graph = GraphFrame(vertices, edges)

    // Only uses Pregel.edge("weight") - dst join should be skipped
    val result = graph.pregel
      .requiredEdgeColumns("weight")
      .setMaxIter(1) // Single iteration to simplify testing
      .withVertexColumn("total", lit(0.0), coalesce(Pregel.msg, col("total")))
      .sendMsgToDst(Pregel.edge("weight"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify results: vertex 1 gets weight 1.0, vertex 2 gets 2.0, vertex 3 gets 3.0
    val totals = result.sort("id").select("total").as[Double].collect()
    assert(totals(0) === 0.0 +- 1e-6) // vertex 0: no incoming edges
    assert(totals(1) === 1.0 +- 1e-6) // vertex 1: edge 0->1 with weight 1.0
    assert(totals(2) === 2.0 +- 1e-6) // vertex 2: edge 1->2 with weight 2.0
    assert(totals(3) === 3.0 +- 1e-6) // vertex 3: edge 2->3 with weight 3.0
  }

  test("automatic dst join skipping - sendMsgToDst with only src columns in message") {
    // When sendMsgToDst is used but the message expression only references src columns,
    // the second join should be skipped. The dst.id needed for message routing is
    // obtained from the edge's dst column, not from a vertex join.

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 3L)).toDF("src", "dst").cache()
    val vertices = (0L to 3L).toDF("id").cache()
    val graph = GraphFrame(vertices, edges)

    // sendMsgToDst but message only uses Pregel.src("id") - dst join should be skipped
    val result = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToDst(Pregel.src("id")) // Message only uses src.id
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Each dst vertex receives the src.id from incoming edges
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming edges
    assert(received(1) === 0L) // vertex 1: edge 0->1, receives src.id = 0
    assert(received(2) === 1L) // vertex 2: edge 1->2, receives src.id = 1
    assert(received(3) === 2L) // vertex 3: edge 2->3, receives src.id = 2
  }

  test("automatic dst join skipping - message references only dst.id") {
    // When message expressions only reference dst.id (not other dst fields),
    // the join should still be skipped since dst.id is available from the edge.

    val edges = Seq((0L, 1L), (1L, 2L), (2L, 3L)).toDF("src", "dst").cache()
    val vertices = (0L to 3L).toDF("id").cache()
    val graph = GraphFrame(vertices, edges)

    // Message uses Pregel.dst("id") - but since only id is used, dst join should be skipped
    val result = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToDst(Pregel.src("id") + Pregel.dst("id")) // Uses dst.id only
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Each dst vertex receives src.id + dst.id from incoming edges
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming edges
    assert(received(1) === 1L) // vertex 1: edge 0->1, receives 0 + 1 = 1
    assert(received(2) === 3L) // vertex 2: edge 1->2, receives 1 + 2 = 3
    assert(received(3) === 5L) // vertex 3: edge 2->3, receives 2 + 3 = 5
  }

  // ============================================================================
  // Integration tests for complex expression patterns
  // These verify that dst join is correctly performed when dst columns are used
  // in non-trivial ways (map keys, array indices, conditionals, nested structs)
  // ============================================================================

  test("dst join required when dst column used in conditional") {
    // when(Pregel.dst("flag"), Pregel.src("value")) - dst.flag requires the join
    val vertices = Seq((0L, true, 10L), (1L, false, 20L), (2L, true, 30L))
      .toDF("id", "flag", "value")
    val edges = Seq((0L, 1L), (1L, 2L)).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)

    val result = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      .sendMsgToDst(when(Pregel.dst("flag"), Pregel.src("value")).otherwise(lit(null)))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify correct behavior: message only sent when dst.flag is true
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming
    assert(received(1) === 0L) // vertex 1: dst.flag=false, so null message (filtered)
    assert(received(2) === 20L) // vertex 2: dst.flag=true, receives src.value=20
  }

  test("dst join required when dst column used as map key") {
    // Create edges with a map column, use dst vertex attribute as key
    val vertices = Seq((0L, "a"), (1L, "b"), (2L, "a")).toDF("id", "key")
    val edges = Seq((0L, 1L, Map("a" -> 10L, "b" -> 20L)), (1L, 2L, Map("a" -> 30L, "b" -> 40L)))
      .toDF("src", "dst", "weights")
    val graph = GraphFrame(vertices, edges)

    val result = graph.pregel
      .requiredEdgeColumns("weights")
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      // Use dst.key to look up value in edge.weights map
      .sendMsgToDst(element_at(Pregel.edge("weights"), Pregel.dst("key")))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify: dst.key is used to index into map
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming
    assert(received(1) === 20L) // vertex 1: key="b", edge weights has b->20
    assert(received(2) === 30L) // vertex 2: key="a", edge weights has a->30
  }

  test("dst join required when dst column used as array index") {
    // Create edges with array column, use dst vertex attribute as index
    val vertices = Seq((0L, 1), (1L, 2), (2L, 1)).toDF("id", "idx")
    val edges = Seq((0L, 1L, Array(100L, 200L)), (1L, 2L, Array(300L, 400L)))
      .toDF("src", "dst", "values")
    val graph = GraphFrame(vertices, edges)

    val result = graph.pregel
      .requiredEdgeColumns("values")
      .setMaxIter(1)
      .withVertexColumn("received", lit(0L), coalesce(Pregel.msg, col("received")))
      // Use dst.idx to index into edge.values array (element_at is 1-based)
      .sendMsgToDst(element_at(Pregel.edge("values"), Pregel.dst("idx")))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify: dst.idx is used to index into array
    val received = result.sort("id").select("received").as[Long].collect()
    assert(received(0) === 0L) // vertex 0: no incoming
    assert(received(1) === 200L) // vertex 1: idx=2, array element 2 = 200
    assert(received(2) === 300L) // vertex 2: idx=1, array element 1 = 300
  }

  test("dst join required for nested struct field access") {
    // Create vertices with nested struct
    val vertices = spark
      .createDataFrame(Seq((0L, 1.0, 2.0), (1L, 3.0, 4.0), (2L, 5.0, 6.0)))
      .toDF("id", "x", "y")
      .selectExpr("id", "named_struct('x', x, 'y', y) as location")

    val edges = Seq((0L, 1L), (1L, 2L)).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)

    val result = graph.pregel
      .setMaxIter(1)
      .withVertexColumn("received", lit(0.0), coalesce(Pregel.msg, col("received")))
      // Access nested field dst.location.x and src.location.y
      .sendMsgToDst(Pregel.dst("location")("x") + Pregel.src("location")("y"))
      .aggMsgs(sum(Pregel.msg))
      .run()

    // Verify: nested struct fields are accessed correctly
    val received = result.sort("id").select("received").as[Double].collect()
    assert(received(0) === 0.0 +- 1e-6) // vertex 0: no incoming
    assert(received(1) === 5.0 +- 1e-6) // vertex 1: dst.location.x=3.0 + src.location.y=2.0
    assert(received(2) === 9.0 +- 1e-6) // vertex 2: dst.location.x=5.0 + src.location.y=4.0
  }
}
