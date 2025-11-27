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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{SHUFFLE_CONSOLIDATION, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for shuffle consolidation
 */
class ShuffleConsolidationSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val remoteStoragePath = new Path(
    System.getProperty("java.io.tmpdir"), "shuffle-consolidation-test")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.remote.storage.path",
        remoteStoragePath.toString + java.io.File.separator)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.remote.HybridShuffleDataIO")
      .set(SQLConf.ENABLE_SHUFFLE_CONSOLIDATION.key, "true")
      .set(SQLConf.SHUFFLE_CONSOLIDATION_SIZE_THRESHOLD.key, "0")
  }

  override def afterAll(): Unit = {
    try {
      // Clean up the remote storage directory
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(spark.sparkContext.getConf)
      val fs = FileSystem.get(remoteStoragePath.toUri, hadoopConf)
      try {
        if (fs.exists(remoteStoragePath)) {
          fs.delete(remoteStoragePath, true)
        }
      } finally {
        fs.close()
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Helper method to find consolidation shuffle stages from a plan.
   */
  private def findConsolidationShuffles(plan: SparkPlan): Seq[ShuffleQueryStageExec] = {
    collectWithSubqueries(plan) {
      case s: ShuffleQueryStageExec
        if s.shuffle.shuffleOrigin == SHUFFLE_CONSOLIDATION => s
    }
  }

  /**
   * Helper method to find consolidation shuffle exchanges from a plan (non-adaptive mode).
   */
  private def findConsolidationExchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    collect(plan) {
      case e: ShuffleExchangeExec
        if e.shuffleOrigin == SHUFFLE_CONSOLIDATION => e
    }
  }

  test("Check if ShuffleConsolidation ShuffleExchange is introduced, when enabled") {
    val df = spark.range(100)
      .selectExpr("id % 10 as key", "id as value")
      .groupBy("key")
      .count()
    df.collect()
    val plan = df.queryExecution.executedPlan

    // Check that consolidation shuffle stage was introduced
    val consolidationShuffles = findConsolidationShuffles(plan)

    assert(consolidationShuffles.size == 1,
      "Shuffle consolidation stage should be introduced when enabled")

    // Verify output correctness
    checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
  }

  test("shuffle consolidation respects size threshold") {
    withSQLConf(
      SQLConf.SHUFFLE_CONSOLIDATION_SIZE_THRESHOLD.key -> Long.MaxValue.toString) {

      // With a very high threshold, consolidation should not be applied
      val df = spark.range(10)
        .selectExpr("id % 5 as key", "id as value")
        .groupBy("key")
        .count()

      df.collect()
      val plan = df.queryExecution.executedPlan

      // Check that consolidation shuffle stage was introduced
      val consolidationShuffles = findConsolidationShuffles(plan)

      assert(consolidationShuffles.isEmpty,
        "Shuffle consolidation should not be applied when size is below threshold")

      // Verify output correctness
      checkAnswer(df, (0 until 5).map(i => Row(i, 2L)))
    }
  }

  test("shuffle consolidation in non-adaptive mode") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {

      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")
        .groupBy("key")
        .count()

      val plan = df.queryExecution.executedPlan

      // Check that consolidation shuffle stage was introduced
      val consolidationShuffles = findConsolidationExchanges(plan)

      assert(consolidationShuffles.size == 1,
        "Shuffle consolidation stage should be introduced when enabled")

      // Verify output correctness
      checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
    }
  }

  /**
   * Tests shuffle consolidation for aggregate operations.
   *
   * This test verifies the behavior for cases where a query stage maps to a partial
   * logical sub-tree. In aggregates, the shuffle exchange is in the middle of the
   * transformation (e.g., both parent HashAggregate and child ShuffleQueryStage point
   * to the same Aggregate logical node), so the consolidation uses the parent's logical
   * link to ensure the entire subtree is found together during re-planning.
   *
   * See [[org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
   * .replaceWithQueryStagesInLogicalPlan]] for details on how query stages
   * are mapped back to logical plans.
   */
  test("shuffle consolidation for aggregate operations") {
    withSQLConf(
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {

      val df = spark.range(100)
        .selectExpr("id % 10 as key", "id as value")
        .groupBy("key")
        .count()
        .orderBy("key")

      df.collect()
      val plan = df.queryExecution.executedPlan

      val consolidationShuffles = findConsolidationShuffles(plan)

      // Should have consolidation stages for the aggregate shuffle
      assert(consolidationShuffles.nonEmpty,
        "Consolidation stages should be present for aggregate shuffles")

      // Verify output correctness
      checkAnswer(df, (0 until 10).map(i => Row(i, 10L)))
    }
  }

  /**
   * Tests shuffle consolidation for join operations.
   *
   * This test verifies the behavior for cases where a query stage maps to an integral
   * logical sub-tree. In joins, each side of the join (left and right) forms a complete
   * relation, so the query stage and parent typically point to different logical nodes.
   * The consolidation uses the stage's logical link to maintain the proper mapping.
   *
   * See [[org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
   * .replaceWithQueryStagesInLogicalPlan]] for details on how query stages
   * are mapped back to logical plans.
   */
  test("shuffle consolidation for join operations") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val df1 = spark.range(50)
        .selectExpr("id as key", "id * 2 as value1")
      val df2 = spark.range(50)
        .selectExpr("id as key", "id * 3 as value2")

      val joined = df1.join(df2, "key").orderBy("key")

      joined.collect()
      val plan = joined.queryExecution.executedPlan

      val consolidationShuffles = findConsolidationShuffles(plan)

      assert(consolidationShuffles.nonEmpty,
        "Shuffle consolidation stages should be introduced for join operations")

      // Verify output correctness
      checkAnswer(joined,
        (0 until 50).map(i => Row(i, i * 2, i * 3)))
    }
  }

  test("shuffle consolidation writes data to remote storage") {
    // Run a query that should trigger shuffle consolidation
    val df = spark.range(1000)
      .selectExpr("id % 100 as key", "id as value")
      .groupBy("key")
      .count()

    df.collect()

    // Get the Hadoop FileSystem
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(spark.sparkContext.getConf)
    val fs = FileSystem.get(remoteStoragePath.toUri, hadoopConf)
    try {
      // Check if the remote path exists
      assert(fs.exists(remoteStoragePath),
        s"Remote shuffle path does not exist: $remoteStoragePath")

      // Look for shuffle data files recursively
      var foundDataFiles = false
      var totalSize = 0L

      val dataFiles = fs.listFiles(remoteStoragePath, true)
      while (dataFiles.hasNext) {
        val file = dataFiles.next()
        if (file.getPath.getName.startsWith("shuffle_") &&
            !file.getPath.getName.contains("checksum")) {
          foundDataFiles = true
          totalSize += file.getLen
        }
      }

      assert(foundDataFiles,
        "No shuffle data files found in remote storage")
      assert(totalSize > 0,
        s"Shuffle data files exist but contain no data (total size: $totalSize)")
    } finally {
      fs.close()
    }

    // Verify output correctness
    checkAnswer(df, (0 until 100).map(i => Row(i, 10L)))
  }
}
