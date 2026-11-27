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

package org.apache.spark.graph

import org.apache.spark.SparkException
import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.graph.IndexedGraph.{ATTR, LONG_DST, LONG_ID, LONG_SRC}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.graph.{GraphCheckpointer, ParquetCheckpointer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}
import org.apache.spark.util.Utils

class IndexedGraphSuite extends QueryTest with GraphFrameTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def withCheckpointer[T](body: GraphCheckpointer => T): T = {
    val checkpointer = ParquetCheckpointer(spark, "test")
    try body(checkpointer)
    finally checkpointer.close()
  }

  private def stringGraph: GraphFrame = GraphFrame(
    Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)).toDF(ID, "x"),
    Seq(("a", "b", 1.0), ("b", "c", 2.0), ("c", "a", 3.0), ("d", "d", 4.0)).toDF(SRC, DST, "w"))

  private def mapping(indexed: IndexedGraph): Map[String, Long] =
    indexed.vertices.collect().map(r => r.getString(1) -> r.getLong(0)).toMap

  test("integral IDs are cast and nothing is stored") {
    Seq(ByteType, ShortType, IntegerType, LongType).foreach { idType =>
      val vertices = Seq(1, 2, 3).toDF(ID).select(col(ID).cast(idType).as(ID))
      val edges = Seq((1, 2), (2, 3))
        .toDF(SRC, DST)
        .select(col(SRC).cast(idType).as(SRC), col(DST).cast(idType).as(DST))
      withCheckpointer { checkpointer =>
        val indexed = IndexedGraph(GraphFrame(vertices, edges), checkpointer)
        assert(indexed.isIntegral)
        assert(checkpointer.size == 0)
        val v = indexed.vertices
        assert(v.columns.toSeq == Seq(LONG_ID, ID))
        checkColumnType(v.schema, LONG_ID, LongType)
        checkColumnType(v.schema, ID, idType)
        checkAnswer(v.select(LONG_ID), Seq(Row(1L), Row(2L), Row(3L)))
        checkAnswer(indexed.edges.select(LONG_SRC, LONG_DST), Seq(Row(1L, 2L), Row(2L, 3L)))
      }
    }
  }

  test("string IDs get unique long IDs that edges agree on") {
    assertNoPersistedDataLeak {
      withCheckpointer { checkpointer =>
        val g = stringGraph
        val indexed = IndexedGraph(g, checkpointer)
        assert(!indexed.isIntegral)
        assert(checkpointer.size == 1)
        val ids = mapping(indexed)
        assert(ids.keySet == Set("a", "b", "c", "d"))
        assert(ids.values.toSet.size == 4)

        val e = indexed.edges
        assert(e.columns.toSeq == Seq(SRC, LONG_SRC, DST, LONG_DST))
        e.collect().foreach { r =>
          assert(r.getLong(1) == ids(r.getString(0)))
          assert(r.getLong(3) == ids(r.getString(2)))
        }
      }
    }
  }

  test("requested columns go into attr") {
    withCheckpointer { checkpointer =>
      val g = GraphFrame(
        Seq(("a", 1), ("b", 2)).toDF(ID, "x.y"),
        Seq(("a", "b", 1.0)).toDF(SRC, DST, "w"))
      val indexed = IndexedGraph(g, checkpointer)
      val v = indexed.vertices(Seq(ID, "x.y"))
      assert(v.columns.toSeq == Seq(LONG_ID, ID, ATTR))
      checkAnswer(v.select(s"$ATTR.*"), g.vertices)
      val e = indexed.edges(Seq("w"))
      assert(e.columns.toSeq == Seq(SRC, LONG_SRC, DST, LONG_DST, ATTR))
      checkAnswer(e.select(SRC, DST, s"$ATTR.w"), g.edges)
    }
  }

  test("long IDs depend only on the IDs, not on partitioning, AQE or the application") {
    val ids = (0 until 200).map(i => s"v$i")
    // The same vertices built in different ways, including a distinct() that is already hash
    // partitioned by ID (graphframes#934).
    val inputs: Seq[() => DataFrame] = Seq(
      () => ids.toDF(ID),
      () => ids.reverse.toDF(ID).repartition(7),
      () => ids.toDF(ID).union(ids.toDF(ID)).distinct(),
      () => ids.toDF(ID).repartition(1).sortWithinPartitions(col(ID).desc))
    // AQE may coalesce everything into one partition. With 5 shuffle partitions, the input of
    // distinct() already has the layout of the mapping, so Spark removes the repartition.
    val results = for {
      aqe <- Seq("true", "false")
      shufflePartitions <- Seq("5", "7")
      input <- inputs
    } yield {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions,
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB") {
        withCheckpointer { checkpointer =>
          val g = GraphFrame(input(), Seq.empty[(String, String)].toDF(SRC, DST))
          mapping(IndexedGraph(g, checkpointer, 5))
        }
      }
    }
    assert(results.head.size == ids.size)
    results.tail.foreach(r => assert(r == results.head))
  }

  test("a graph read from a directory whose files changed gets its own mapping") {
    withTempDir { dir =>
      def readIds(): DataFrame = spark.read
        .option("recursiveFileLookup", "true")
        .parquet(dir.getCanonicalPath)
        .select(col("value").as(ID))
      Seq("a", "b").toDF().write.parquet(s"$dir/first")
      withCheckpointer { checkpointer =>
        val g = GraphFrame(readIds(), Seq.empty[(String, String)].toDF(SRC, DST))
        assert(mapping(IndexedGraph(g, checkpointer)).keySet == Set("a", "b"))
      }
      Utils.deleteRecursively(new java.io.File(dir, "first"))
      Seq("x", "y", "z").toDF().write.parquet(s"$dir/second")
      withCheckpointer { checkpointer =>
        val g = GraphFrame(readIds(), Seq.empty[(String, String)].toDF(SRC, DST))
        assert(mapping(IndexedGraph(g, checkpointer)).keySet == Set("x", "y", "z"))
      }
    }
  }

  test("edges join the mapping without shuffling it") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withCheckpointer { checkpointer =>
        val indexed = IndexedGraph(stringGraph, checkpointer, 3)
        val e = indexed.edges
        e.collect()
        val joins = collect(e.queryExecution.executedPlan) { case j: SortMergeJoinExec => j }
        assert(joins.length == 2, e.queryExecution.executedPlan)
        joins.foreach { j =>
          assert(collect(j.right) { case s: ShuffleExchangeExec => s }.isEmpty, j)
        }
      }
    }
  }

  // just to not break it by accident in the future, since it is a contract
  test("edges with an endpoint that is not a vertex are dropped") {
    withCheckpointer { checkpointer =>
      val g =
        GraphFrame(Seq("a", "b").toDF(ID), Seq(("a", "b"), ("a", "x"), ("y", "b")).toDF(SRC, DST))
      checkAnswer(IndexedGraph(g, checkpointer).edges.select(SRC, DST), Seq(Row("a", "b")))
    }
  }

  test("empty graph") {
    withCheckpointer { checkpointer =>
      val g = GraphFrame(Seq.empty[String].toDF(ID), Seq.empty[(String, String)].toDF(SRC, DST))
      val indexed = IndexedGraph(g, checkpointer)
      assert(indexed.vertices.isEmpty)
      assert(indexed.edges.isEmpty)
    }
  }

  test("the mapping is released with the checkpointer") {
    val checkpointer = ParquetCheckpointer(spark, "test")
    val indexed =
      try {
        val indexed = IndexedGraph(stringGraph, checkpointer)
        assert(indexed.vertices.count() == 4)
        indexed
      } finally {
        checkpointer.close()
      }
    intercept[SparkException](indexed.vertices.collect())
  }
}
