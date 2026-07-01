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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{FileSourceScanExec, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for the [[MarkSingleTaskExecution]] optimizer rule and its physical effects. The rule
 * marks small single-partition scans, optionally under a shuffle-inducing operator, so that the
 * scan reports a `SinglePartition` output partitioning and the following shuffle can be elided.
 */
class MarkSingleTaskExecutionSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val t = "single_task_t"
  private val t2 = "single_task_t2"
  private val emptyTable = "single_task_empty"

  private def enabledConfs: Seq[(String, String)] = Seq(
    SQLConf.SINGLE_TASK_EXECUTION_ENABLED.key -> "true",
    // Force the optimization to also apply to zero-file / zero-byte scans so that we can exercise
    // the empty-scan corner case created by dynamic pruning.
    SQLConf.SINGLE_TASK_EXECUTION_MIN_NUM_FILES.key -> "0",
    SQLConf.SINGLE_TASK_EXECUTION_MIN_NUM_BYTES.key -> "0",
    SQLConf.SINGLE_TASK_EXECUTION_LOCAL_TABLE_SCAN_MIN_ROWS.key -> "0")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A single-file Parquet table with a small amount of data.
    spark.range(0, 2).selectExpr("id as col", "cast(id as string) as col_str")
      .repartition(1).write.mode("overwrite").saveAsTable(t)
    spark.range(0, 3).selectExpr("id % 2 as col")
      .repartition(1).write.mode("overwrite").saveAsTable(t2)
    spark.range(0, 0).selectExpr("id as col").write.mode("overwrite").saveAsTable(emptyTable)
  }

  override def afterAll(): Unit = {
    try {
      sql(s"drop table if exists $t")
      sql(s"drop table if exists $t2")
      sql(s"drop table if exists $emptyTable")
    } finally {
      super.afterAll()
    }
  }

  private def getFinalPhysicalPlan(df: org.apache.spark.sql.DataFrame): SparkPlan = {
    df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
      case other => other
    }
  }

  private def hasShuffle(plan: SparkPlan): Boolean =
    collectWithSubqueries(plan) { case s: ShuffleExchangeLike => s }.nonEmpty

  private def isMarked(plan: LogicalPlan): Boolean = {
    val marks = plan.collect {
      case lr: LogicalRelation => lr.getTagValue(MarkSingleTaskExecution.markTag).getOrElse(false)
      case lr: LocalRelation => lr.getTagValue(MarkSingleTaskExecution.markTag).getOrElse(false)
    }
    marks.nonEmpty && marks.forall(identity)
  }

  private def checkMarked(query: String): Unit = withSQLConf(enabledConfs: _*) {
    val plan = sql(query).queryExecution.optimizedPlan
    assert(isMarked(plan), s"expected plan to be marked for single-task execution:\n$plan")
  }

  private def checkNotMarked(query: String, confs: Seq[(String, String)] = enabledConfs): Unit =
    withSQLConf(confs: _*) {
      val plan = sql(query).queryExecution.optimizedPlan
      assert(!isMarked(plan), s"expected plan NOT to be marked:\n$plan")
    }

  private def checkSinglePartition(
      query: String,
      expected: Seq[Row],
      confs: Seq[(String, String)] = enabledConfs): Unit = withSQLConf(confs: _*) {
    val df = sql(query)
    QueryTest.checkAnswer(df, expected)
    val plan = getFinalPhysicalPlan(df)
    assert(!hasShuffle(plan), s"expected no shuffle in:\n$plan")
    val scans = collect(plan) {
      case s: FileSourceScanExec => s.outputPartitioning
      case s: LocalTableScanExec => s.outputPartitioning
    }
    assert(scans.nonEmpty, s"expected a scan in:\n$plan")
    assert(scans.forall(_ == SinglePartition),
      s"expected all scans to report SinglePartition, got $scans in:\n$plan")
  }

  test("marks scan + sort") {
    checkMarked(s"select col from $t order by col")
    checkMarked(s"select col from (select col from $t where col = 0) order by col")
  }

  test("marks scan + aggregation") {
    checkMarked(s"select count(1) from $t group by col")
    checkMarked(s"select sum(col) from (select col from $t where col < 42)")
  }

  test("marks scan + window") {
    checkMarked(
      s"select col, row_number() over (partition by col order by col) from $t")
  }

  test("does not mark when the feature is disabled") {
    checkNotMarked(
      s"select col from $t order by col",
      Seq(SQLConf.SINGLE_TASK_EXECUTION_ENABLED.key -> "false"))
  }

  test("does not mark when the per-operator flag is disabled") {
    checkNotMarked(
      s"select col from $t order by col",
      enabledConfs :+ (SQLConf.SINGLE_TASK_EXECUTION_SORT.key -> "false"))
  }

  test("does not mark unsupported plan shapes (join)") {
    // Join is not a supported operator in this port, so the presence of a join makes the whole
    // plan ineligible.
    checkNotMarked(s"select a.col from $t a join $t b on a.col = b.col order by a.col")
  }

  test("does not mark plans with subquery expressions") {
    checkNotMarked(s"select col from $t where col = (select max(col) from $t2) order by col")
  }

  test("output partitioning is SinglePartition, scan + sort") {
    checkSinglePartition(s"select col from $t order by col", Seq(Row(0), Row(1)))
  }

  test("output partitioning is SinglePartition, scan + aggregation with group by") {
    checkSinglePartition(
      s"select count(1) as c from $t2 group by col",
      Seq(Row(1), Row(2)))
  }

  test("output partitioning is SinglePartition, scan + aggregation without group by") {
    checkSinglePartition(s"select sum(col) from $t", Seq(Row(1)))
  }

  test("output partitioning is SinglePartition, scan + distinct") {
    checkSinglePartition(s"select distinct col from $t2", Seq(Row(0), Row(1)))
  }

  test("empty table scan + aggregation is correct and single-partition") {
    // Without single-task execution eliding the shuffle before the aggregation, an empty scan
    // could incorrectly return zero rows instead of a single NULL row for a global aggregation.
    checkSinglePartition(s"select sum(col) from $emptyTable", Seq(Row(null)))
  }

  test("in-memory local relation is scanned in a single partition") {
    checkSinglePartition(
      "select col from values (0), (1) as tab(col) order by col",
      Seq(Row(0), Row(1)))
  }

  test("does not mark when a leaf-node parallelism override is set") {
    checkNotMarked(
      "select col from values (0), (1) as tab(col) order by col",
      enabledConfs :+ (SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "4"))
  }
}
