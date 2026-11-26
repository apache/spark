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

package org.apache.spark.sql.graph

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class GraphCheckpointerSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def withParquet(body: (ParquetCheckpointer, File) => Unit): Unit = {
    withTempDir { base =>
      val dir = new File(base, "run")
      val checkpointer = new ParquetCheckpointer(spark, new Path(dir.toURI))
      try body(checkpointer, dir)
      finally checkpointer.close()
    }
  }

  private def withPersist(body: PersistCheckpointer => Unit): Unit = {
    val checkpointer = PersistCheckpointer(spark)
    try body(checkpointer)
    finally checkpointer.close()
  }

  private def forEachImplementation(name: String)(body: GraphCheckpointer => Unit): Unit = {
    test(s"parquet: $name")(withParquet((checkpointer, _) => body(checkpointer)))
    test(s"persist: $name")(withPersist(body))
  }

  private def names(dir: File): Seq[String] =
    dir.list().filterNot(_.startsWith(".")).map(_.drop(7)).sorted.toSeq

  private def ids(n: Long): DataFrame = spark.range(n).toDF("id")

  private def isCached(df: DataFrame): Boolean =
    spark.sharedState.cacheManager.lookupCachedData(df).isDefined

  private def assertInternalError(message: String)(body: => Any): Unit = {
    checkError(
      exception = intercept[SparkException](body),
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> message))
  }

  forEachImplementation("push keeps data and schema, including nullability") { checkpointer =>
    val df = spark.range(5).selectExpr("id", "named_struct('a', id) AS s")
    assert(!df.schema("id").nullable)
    val restored = checkpointer.push(df, "a")
    assert(restored.schema == df.schema)
    checkAnswer(restored, df)
  }

  forEachImplementation("evict, evictAllButLastN, removeLast and purge") { checkpointer =>
    (1L to 5L).foreach(i => checkpointer.push(ids(i), s"c$i"))
    checkpointer.evict(1)
    assert(checkpointer.size == 4)
    checkpointer.removeLast(1)
    assert(checkpointer.size == 3)
    checkpointer.evictAllButLastN(2)
    assert(checkpointer.size == 2)
    checkpointer.evictAllButLastN(5)
    assert(checkpointer.size == 2)
    checkpointer.purge()
    assert(checkpointer.size == 0)
    assertInternalError("The number of checkpoints must not be negative, but got -1.") {
      checkpointer.evict(-1)
    }
  }

  forEachImplementation("replaceLast reads its inputs and keeps the result") { checkpointer =>
    checkpointer.push(ids(1), "keep")
    val left = checkpointer.push(ids(3), "same")
    val right = checkpointer.push(spark.range(3, 5).toDF("id"), "right")
    val merged = checkpointer.replaceLast(2, left.union(right), "same")
    assert(checkpointer.size == 2)
    checkAnswer(merged, ids(5))
    assertInternalError("Cannot replace 3 of 2 checkpoints.") {
      checkpointer.replaceLast(3, ids(1), "x")
    }
  }

  // Guards the plan shape against changes in Spark: on the final plan of a sort-merge join, the
  // pushed side has no Exchange and no Sort, and only the other side is shuffled to its layout.
  private def checkPartitionedLayout(checkpointer: GraphCheckpointer): Unit = {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val edges = spark.range(100).selectExpr("id % 10 AS SRC", "id AS dst")
      // The key is matched case-insensitively, as in other DataFrame operations.
      val partitioned = checkpointer.pushPartitioned(edges, "edges", "src", numPartitions = 7)
      // Not checkAnswer: it treats a plan with a Sort as globally sorted.
      assert(partitioned.collect().toSet == edges.collect().toSet)

      val joined = partitioned.join(ids(10), col("SRC") === col("id"))
      val expected = edges.join(ids(10), col("SRC") === col("id"))
      assert(joined.collect().toSet == expected.collect().toSet)
      val plan = joined.queryExecution.executedPlan
      val joins = collect(plan) { case j: SortMergeJoinExec => j }
      assert(joins.length == 1, plan)
      assert(collect(joins.head.left) { case e: ShuffleExchangeExec => e }.isEmpty, plan)
      assert(collect(joins.head.left) { case s: SortExec => s }.isEmpty, plan)
      val otherExchanges = collect(joins.head.right) { case e: ShuffleExchangeExec => e }
      assert(otherExchanges.length == 1, plan)
      assert(otherExchanges.head.outputPartitioning.numPartitions == 7, plan)

      val aggregated = partitioned.groupBy("SRC").count()
      assert(aggregated.collect().length == 10)
      val aggregatedPlan = aggregated.queryExecution.executedPlan
      assert(collect(aggregatedPlan) { case e: ShuffleExchangeExec => e }.isEmpty, aggregatedPlan)
    }
  }

  forEachImplementation("pushPartitioned skips the exchange and the sort on its side") {
    checkPartitionedLayout
  }

  forEachImplementation("empty data") { checkpointer =>
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    Seq(
      checkpointer.push(empty, "plain"),
      checkpointer.pushPartitioned(empty, "partitioned", "id")).foreach { restored =>
      assert(restored.schema == schema)
      assert(restored.isEmpty)
    }
  }

  forEachImplementation("close is idempotent and forbids new writes") { checkpointer =>
    checkpointer.push(ids(1), "a")
    checkpointer.close()
    checkpointer.close()
    assert(checkpointer.size == 0)
    assertInternalError("The checkpointer is closed.")(checkpointer.push(ids(1), "b"))
  }

  forEachImplementation("invalid name") { checkpointer =>
    assertInternalError("Invalid checkpoint name: ../a.")(checkpointer.push(ids(1), "../a"))
    assert(checkpointer.size == 0)
  }

  test("parquet: apply fails without a directory and prefers workDir") {
    assert(spark.sparkContext.getCheckpointDir.isEmpty)
    checkError(
      exception = intercept[SparkException] {
        ParquetCheckpointer(spark, "test")
      },
      condition = "CHECKPOINT_DIRECTORY_NOT_SET",
      parameters = Map.empty)
    withTempDir { workDir =>
      withSQLConf(GraphConf.WORK_DIR.key -> workDir.getCanonicalPath) {
        val checkpointer = ParquetCheckpointer(spark, "test")
        try {
          checkpointer.push(ids(1), "a")
          assert(workDir.list().exists(_.startsWith("test-")))
        } finally {
          checkpointer.close()
        }
        assert(workDir.list().isEmpty)
      }
    }
  }

  test("parquet: removed checkpoints are deleted in order") {
    withParquet { (checkpointer, dir) =>
      Seq("a", "b", "c", "d").foreach(checkpointer.push(ids(1), _))
      checkpointer.evict(1)
      assert(names(dir) == Seq("b", "c", "d"))
      checkpointer.removeLast(1)
      assert(names(dir) == Seq("b", "c"))
      val merged = checkpointer.replaceLast(2, ids(2), "b")
      assert(names(dir) == Seq("b"))
      checkAnswer(merged, ids(2))
      checkpointer.close()
      assert(!dir.exists())
    }
  }

  test("parquet: pushPartitioned does not depend on the session configs") {
    withParquet { (checkpointer, _) =>
      withSQLConf(
        SQLConf.MAX_RECORDS_PER_FILE.key -> "3",
        SQLConf.BUCKETING_ENABLED.key -> "false",
        SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING.key -> "false") {
        checkPartitionedLayout(checkpointer)
        assert(!spark.sessionState.conf.bucketingEnabled)
      }
    }
  }

  test("parquet: pushPartitioned reports the size of the files") {
    withParquet { (checkpointer, dir) =>
      val df = spark.range(1000).selectExpr("id % 10 AS src", "id AS dst")
      val partitioned = checkpointer.pushPartitioned(df, "edges", "src", numPartitions = 3)
      val files = dir.listFiles().flatMap(_.listFiles()).filter(_.getName.endsWith(".parquet"))
      assert(files.length == 3)
      assert(partitioned.queryExecution.optimizedPlan.stats.sizeInBytes == files.map(_.length).sum)
    }
  }

  test("persist: releases its entries and keeps a result over a released input") {
    withPersist { checkpointer =>
      val input = checkpointer.push(ids(3), "input")
      val derived = input.selectExpr("id + 1 AS id")
      val merged = checkpointer.replaceLast(1, derived, "merged")
      assert(!isCached(input))
      assert(isCached(merged))
      checkAnswer(merged, spark.range(1, 4).toDF("id"))
      checkpointer.purge()
      assert(!isCached(merged))
    }
  }

  test("persist: does not unpersist data cached by the caller") {
    val df = ids(3).persist()
    try {
      withPersist { checkpointer =>
        checkpointer.push(df, "a")
        checkpointer.purge()
      }
      assert(isCached(df))
    } finally {
      df.unpersist()
    }
  }

  test("persist: uses the configured storage level") {
    withSQLConf(GraphConf.STORAGE_LEVEL.key -> "DISK_ONLY") {
      withPersist { checkpointer =>
        val data = checkpointer.push(ids(3), "a")
        assert(data.storageLevel.useDisk && !data.storageLevel.useMemory)
      }
    }
  }
}
