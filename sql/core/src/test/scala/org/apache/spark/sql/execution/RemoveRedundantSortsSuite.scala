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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType


abstract class RemoveRedundantSortsSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def checkNumSorts(df: DataFrame, count: Int): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) { case s: SortExec => s }.length == count)
  }

  private def checkSorts(query: String, enabledCount: Int, disabledCount: Int): Unit = {
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true") {
      val df = sql(query)
      checkNumSorts(df, enabledCount)
      val result = df.collect()
      withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "false") {
        val df = sql(query)
        checkNumSorts(df, disabledCount)
        checkAnswer(df, result)
      }
    }
  }

  test("remove redundant sorts with limit") {
    withTempView("t") {
      spark.range(100).select($"id" as "key").createOrReplaceTempView("t")
      val query =
        """
          |SELECT key FROM
          | (SELECT key FROM t WHERE key > 10 ORDER BY key DESC LIMIT 10)
          |ORDER BY key DESC
          |""".stripMargin
      checkSorts(query, 0, 1)
    }
  }

  test("remove redundant sorts with broadcast hash join") {
    withTempView("t1", "t2") {
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t1")
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t2")

      val queryTemplate = """
        |SELECT /*+ BROADCAST(%s) */ t1.key FROM
        | (SELECT key FROM t1 WHERE key > 10 ORDER BY key DESC LIMIT 10) t1
        |JOIN
        | (SELECT key FROM t2 WHERE key > 50 ORDER BY key DESC LIMIT 100) t2
        |ON t1.key = t2.key
        |ORDER BY %s
      """.stripMargin

      // No sort should be removed since the stream side (t2) order DESC
      // does not satisfy the required sort order ASC.
      val buildLeftOrderByRightAsc = queryTemplate.format("t1", "t2.key ASC")
      checkSorts(buildLeftOrderByRightAsc, 1, 1)

      // The top sort node should be removed since the stream side (t2) order DESC already
      // satisfies the required sort order DESC.
      val buildLeftOrderByRightDesc = queryTemplate.format("t1", "t2.key DESC")
      checkSorts(buildLeftOrderByRightDesc, 0, 1)

      // No sort should be removed since the sort ordering from broadcast-hash join is based
      // on the stream side (t2) and the required sort order is from t1.
      val buildLeftOrderByLeftDesc = queryTemplate.format("t1", "t1.key DESC")
      checkSorts(buildLeftOrderByLeftDesc, 1, 1)

      // The top sort node should be removed since the stream side (t1) order DESC already
      // satisfies the required sort order DESC.
      val buildRightOrderByLeftDesc = queryTemplate.format("t2", "t1.key DESC")
      checkSorts(buildRightOrderByLeftDesc, 0, 1)
    }
  }

  test("remove redundant sorts with sort merge join") {
    withTempView("t1", "t2") {
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t1")
      spark.range(1000).select($"id" as "key").createOrReplaceTempView("t2")
      val query = """
        |SELECT /*+ MERGE(t1) */ t1.key FROM
        | (SELECT key FROM t1 WHERE key > 10 ORDER BY key DESC LIMIT 10) t1
        |JOIN
        | (SELECT key FROM t2 WHERE key > 50 ORDER BY key DESC LIMIT 100) t2
        |ON t1.key = t2.key
        |ORDER BY t1.key
      """.stripMargin

      val queryAsc = query + " ASC"
      checkSorts(queryAsc, 2, 3)

      // The top level sort should not be removed since the child output ordering is ASC and
      // the required ordering is DESC.
      val queryDesc = query + " DESC"
      checkSorts(queryDesc, 3, 3)
    }
  }

  test("cached sorted data doesn't need to be re-sorted") {
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true") {
      val df = spark.range(1000).select($"id" as "key").sort($"key".desc).cache()
      df.collect()
      val resorted = df.sort($"key".desc)
      val sortedAsc = df.sort($"key".asc)
      checkNumSorts(df, 0)
      checkNumSorts(resorted, 0)
      checkNumSorts(sortedAsc, 1)
      val result = resorted.collect()
      withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "false") {
        val resorted = df.sort($"key".desc)
        resorted.collect()
        checkNumSorts(resorted, 1)
        checkAnswer(resorted, result)
      }
    }
  }

  test("SPARK-58099: remove local sort dangling below a shuffle that does not require ordering") {
    // This dangling-sort shape only appears in the physical plan, e.g. when a shuffle is inserted
    // on top of an existing local sort (skew join optimization does this). It cannot be produced
    // from SQL/DataFrame directly because the logical `EliminateSorts` strips a local sort sitting
    // below a repartition before physical planning, so the rule is exercised on a physical plan.
    val attr = AttributeReference("key", IntegerType)()
    val scan = LocalTableScanExec(Seq(attr), Nil, None)
    val localSort = SortExec(SortOrder(attr, Ascending) :: Nil, global = false, scan)
    val globalSort = SortExec(SortOrder(attr, Ascending) :: Nil, global = true, scan)
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true") {
      // A local sort directly below a shuffle that neither requires nor exposes an ordering
      // is dead and should be removed.
      val danglingLocal = ShuffleExchangeExec(HashPartitioning(Seq(attr), 5), localSort)
      assert(RemoveRedundantSorts(danglingLocal).find(_.isInstanceOf[SortExec]).isEmpty)

      // A global sort below such a shuffle must be kept: it carries its own distribution
      // requirement and is not the dead within-partition sort this rule targets.
      val danglingGlobal = ShuffleExchangeExec(HashPartitioning(Seq(attr), 5), globalSort)
      assert(RemoveRedundantSorts(danglingGlobal).find(_.isInstanceOf[SortExec]).isDefined)
    }
    // With the rule disabled the local sort is preserved.
    withSQLConf(SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "false") {
      val danglingLocal = ShuffleExchangeExec(HashPartitioning(Seq(attr), 5), localSort)
      assert(RemoveRedundantSorts(danglingLocal).find(_.isInstanceOf[SortExec]).isDefined)
    }
  }

  test("SPARK-58099: keep local sort satisfied only by a bucketed scan output ordering") {
    // With `RemoveRedundantSorts` running before `DisableUnnecessaryBucketedScan`, the local sort
    // could be stripped based on the bucketed scan's output ordering and then the ordering would
    // be silently lost once `DisableUnnecessaryBucketedScan` disables the scan. Running
    // `RemoveRedundantSorts` after `DisableUnnecessaryBucketedScan` closes that hole.
    withSQLConf(
      SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true",
      SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {
      withTable("t") {
        // A single file per bucket so the bucketed scan reports an output ordering.
        spark.range(100).selectExpr("id as i").repartition(1)
          .write.format("parquet").bucketBy(8, "i").sortBy("i").saveAsTable("t")
        val df = sql("SELECT * FROM t SORT BY i")
        val plan = df.queryExecution.executedPlan
        // The bucketed scan is disabled since there is no interesting-partition operator above it.
        assert(collect(plan) { case s: FileSourceScanExec if s.bucketedScan => s }.isEmpty)
        // The local sort must be kept, otherwise the within-partition ordering would be lost.
        assert(collect(plan) { case s: SortExec => s }.nonEmpty)
      }
    }
  }

  test("SPARK-33472: shuffled join with different left and right side partition numbers") {
    withTempView("t1", "t2") {
      spark.range(0, 100, 1, 2).select($"id" as "key").createOrReplaceTempView("t1")
      (0 to 100).toDF("key").createOrReplaceTempView("t2")

      val queryTemplate = """
        |SELECT /*+ %s(t1) */ t1.key
        |FROM t1 JOIN t2 ON t1.key = t2.key
        |WHERE t1.key > 10 AND t2.key < 50
        |ORDER BY t1.key ASC
      """.stripMargin

      Seq(("MERGE", 3), ("SHUFFLE_HASH", 1)).foreach { case (hint, count) =>
        val query = queryTemplate.format(hint)
        val df = sql(query)
        val sparkPlan = df.queryExecution.sparkPlan
        val join = sparkPlan.collect { case j: ShuffledJoin => j }.head
        val leftPartitioning = join.left.outputPartitioning
        assert(leftPartitioning.isInstanceOf[RangePartitioning])
        assert(leftPartitioning.numPartitions == 2)
        assert(join.right.outputPartitioning == UnknownPartitioning(0))
        checkSorts(query, count, count)
      }
    }
  }
}

class RemoveRedundantSortsSuite extends RemoveRedundantSortsSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantSortsSuiteAE extends RemoveRedundantSortsSuiteBase
  with EnableAdaptiveExecutionSuite
