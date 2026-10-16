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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PushDownJoinThroughUnionSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.PUSH_DOWN_JOIN_THROUGH_UNION_ENABLED.key, "true")

  test("UNION ALL + broadcast JOIN produces correct results") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        // Every branch must stay larger than the dimension table in bytes, otherwise the planner
        // builds from the branch and the rule declines to push the join down.
        val fact1 = Seq((1, "a"), (2, "b"), (5, "e"), (6, "f")).toDF("id", "val1")
        val fact2 = Seq((3, "c"), (4, "d"), (7, "g"), (8, "h")).toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(2, "b", "y"),
          Row(3, "c", "z")
        ))
        assertJoinCount(result, 2)
      }
    }
  }

  test("3-way UNION ALL + broadcast JOIN (TPC-DS pattern)") {
    withTempView("fact1", "fact2", "fact3", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        // Twelve rows per branch keep every branch strictly larger than the dimension table in
        // bytes; the extra ids join with nothing and leave the expected rows unchanged.
        val fact1 = ((1, 10) +: (2, 20) +: (21 to 30).map(i => (i, i * 10))).toDF("id", "amount")
        val fact2 = ((3, 30) +: (4, 40) +: (31 to 40).map(i => (i, i * 10))).toDF("id", "amount")
        val fact3 = ((1, 50) +: (5, 60) +: (41 to 50).map(i => (i, i * 10))).toDF("id", "amount")
        val dim = Seq((1, "web"), (2, "store"), (3, "catalog"), (5, "other"))
          .toDF("id", "channel")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        fact3.createOrReplaceTempView("fact3")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.amount, d.channel
            |FROM (
            |  SELECT * FROM fact1
            |  UNION ALL SELECT * FROM fact2
            |  UNION ALL SELECT * FROM fact3
            |) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, 10, "web"),
          Row(2, 20, "store"),
          Row(3, 30, "catalog"),
          Row(1, 50, "web"),
          Row(5, 60, "other")
        ))
        assertJoinCount(result, 3)
      }
    }
  }

  test("LeftOuter Join through UNION ALL produces correct results") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
        val fact2 = Seq((3, "c"), (99, "d")).toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |LEFT OUTER JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(2, "b", "y"),
          Row(3, "c", "z"),
          Row(99, "d", null)
        ))
        assertJoinCount(result, 2)
      }
    }
  }

  test("Optimization disabled produces same results") {
    withTempView("fact1", "fact2", "dim") {
      // Branches larger than the dimension table in bytes, so the rule fires in the first run and
      // the comparison against the excluded-rule run is meaningful.
      val fact1 = Seq((1, "a"), (2, "b"), (5, "e"), (6, "f")).toDF("id", "val1")
      val fact2 = Seq((3, "c"), (4, "d"), (7, "g"), (8, "h")).toDF("id", "val1")
      val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

      fact1.createOrReplaceTempView("fact1")
      fact2.createOrReplaceTempView("fact2")
      dim.createOrReplaceTempView("dim")

      val query =
        """SELECT f.id, f.val1, d.label
          |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
          |JOIN dim d ON f.id = d.id
        """.stripMargin

      val expected = Seq(
        Row(1, "a", "x"),
        Row(2, "b", "y"),
        Row(3, "c", "z")
      )

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        val enabled = sql(query)
        checkAnswer(enabled, expected)
        assertJoinCount(enabled, 2)
      }

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.PushDownJoinThroughUnion") {
        val excluded = sql(query)
        checkAnswer(excluded, expected)
        assertJoinCount(excluded, 1)
      }
    }
  }

  test("ColumnPruning works after join push down") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        // Column pruning shrinks both sides before the rule reads their stats, so the branches need
        // enough rows to stay strictly larger than the dimension table once pruned.
        val fact1 = ((1, "a", 100) +: (2, "b", 200) +: (21 to 30).map(i => (i, "x", i)))
          .toDF("id", "val1", "val2")
        val fact2 = ((3, "c", 300) +: (4, "d", 400) +: (31 to 40).map(i => (i, "y", i)))
          .toDF("id", "val1", "val2")
        val dim = Seq((1, "x", "extra1"), (2, "y", "extra2"), (3, "z", "extra3"))
          .toDF("id", "label", "info")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "x"),
          Row(2, "y"),
          Row(3, "z")
        ))
        assertJoinCount(result, 2)
      }
    }
  }

  test("2-way UNION ALL reuses broadcast exchange") {
    withTempView("fact1", "fact2", "dim") {
      val fact1 = Seq((1, "a"), (2, "b")).toDF("id", "val1")
      val fact2 = Seq((3, "c"), (4, "d")).toDF("id", "val1")
      val dim = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "label")

      fact1.createOrReplaceTempView("fact1")
      fact2.createOrReplaceTempView("fact2")
      dim.createOrReplaceTempView("dim")

      val result = sql(
        """SELECT /*+ BROADCAST(d) */ f.id, f.val1, d.label
          |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
          |JOIN dim d ON f.id = d.id
        """.stripMargin)

      result.collect()
      val plan = result.queryExecution.executedPlan

      val broadcastStages = collect(plan) {
        case b: BroadcastQueryStageExec => b
      }
      val reusedBroadcasts = collectWithSubqueries(plan) {
        case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
      }

      assert(broadcastStages.size == 2,
        "Expected 2 BroadcastQueryStageExec (1 original + 1 reused) but found " +
          broadcastStages.size)
      assert(reusedBroadcasts.size == 1,
        "Expected exactly 1 ReusedExchangeExec inside BroadcastQueryStageExec but found " +
          reusedBroadcasts.size)
    }
  }

  test("PushPredicateThroughJoin works after join push down") {
    withTempView("fact1", "fact2", "dim") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
        // Each branch must stay strictly larger than the dimension table in bytes: at equal sizes
        // the rule fires only through the `right <= left` tie-break in `getSmallerSide`.
        val fact1 = Seq((1, "a"), (2, "b"), (5, "e"), (6, "f"), (9, "i"), (10, "j"))
          .toDF("id", "val1")
        val fact2 = Seq((3, "c"), (4, "d"), (7, "g"), (8, "h"), (11, "k"), (12, "l"))
          .toDF("id", "val1")
        val dim = Seq((1, "x"), (2, "y"), (3, "z"), (4, "w")).toDF("id", "label")

        fact1.createOrReplaceTempView("fact1")
        fact2.createOrReplaceTempView("fact2")
        dim.createOrReplaceTempView("dim")

        val result = sql(
          """SELECT f.id, f.val1, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.id
            |WHERE d.label IN ('x', 'z')
          """.stripMargin)

        checkAnswer(result, Seq(
          Row(1, "a", "x"),
          Row(3, "c", "z")
        ))
        assertJoinCount(result, 2)
      }
    }
  }

  test("SPARK-58449: right side is scanned once when only the Union side is broadcastable") {
    withTable("fact1", "fact2", "dim") {
      // The threshold sits between the two Union branches and the right side, so the Union is
      // broadcastable but the right side is not. An inner join can broadcast either side, so the
      // join still plans as a broadcast hash join and the rule used to fire, cloning the right side
      // once per branch. Nothing reuses a bare probe-side scan, so the right side would be read
      // twice.
      spark.range(0, 4).selectExpr("id", "id AS v").write.format("parquet").saveAsTable("fact1")
      spark.range(4, 8).selectExpr("id", "id AS v").write.format("parquet").saveAsTable("fact2")
      spark.range(0, 2000).selectExpr("id AS did", "id AS label").write
        .format("parquet").saveAsTable("dim")

      val unionSize = Seq("fact1", "fact2").map(t =>
        spark.table(t).queryExecution.optimizedPlan.stats.sizeInBytes).sum
      val rightSize = spark.table("dim").queryExecution.optimizedPlan.stats.sizeInBytes
      assert(unionSize < rightSize, "test setup: the Union must be smaller than the right side")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> unionSize.toString) {
        val df = sql(
          """SELECT f.id, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.did
          """.stripMargin)
        checkAnswer(df, (0 until 8).map(i => Row(i.toLong, i.toLong)))

        assertBuildsFromTheLeft(df)
        assertJoinCount(df, 1)
        assert(rightScansOf(df) == 1,
          s"the right side must be scanned once, found ${rightScansOf(df)} scans of it")
      }
    }
  }

  test("SPARK-58449: right side is scanned once when it is larger than each Union branch") {
    withTable("fact1", "fact2", "dim") {
      // Both sides are under the threshold, so an inner join can build from either one and the
      // planner picks the smaller side, which is the left one. The rule used to fire anyway,
      // because a broadcast hash join was plannable, and every branch then probed its own copy of
      // the right side.
      spark.range(0, 4).selectExpr("id", "id AS v").write.format("parquet").saveAsTable("fact1")
      spark.range(4, 8).selectExpr("id", "id AS v").write.format("parquet").saveAsTable("fact2")
      spark.range(0, 2000).selectExpr("id AS did", "id AS label").write
        .format("parquet").saveAsTable("dim")

      val branchSizes = Seq("fact1", "fact2").map(t =>
        spark.table(t).queryExecution.optimizedPlan.stats.sizeInBytes)
      val rightSize = spark.table("dim").queryExecution.optimizedPlan.stats.sizeInBytes
      assert(branchSizes.forall(_ < rightSize) && branchSizes.sum < rightSize,
        "test setup: the right side must be larger than the whole Union")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> (rightSize * 2).toString) {
        val df = sql(
          """SELECT f.id, d.label
            |FROM (SELECT * FROM fact1 UNION ALL SELECT * FROM fact2) f
            |JOIN dim d ON f.id = d.did
          """.stripMargin)
        checkAnswer(df, (0 until 8).map(i => Row(i.toLong, i.toLong)))

        assertBuildsFromTheLeft(df)
        assertJoinCount(df, 1)
        assert(rightScansOf(df) == 1,
          s"the right side must be scanned once, found ${rightScansOf(df)} scans of it")
      }
    }
  }

  /**
   * Asserts how many `Join` nodes the optimized plan holds. The rule turns one join into one per
   * Union branch, so this pins whether it fired. Without it, a fixture whose branches stopped being
   * larger than the dimension table would still produce the right rows and pass silently.
   */
  private def assertJoinCount(df: DataFrame, expected: Int): Unit = {
    val joins = df.queryExecution.optimizedPlan.collect { case j: Join => j }
    assert(joins.size == expected,
      s"expected $expected Join nodes in the optimized plan, found ${joins.size}")
  }

  /**
   * Asserts that every broadcast hash join in the plan builds from the left, the condition under
   * which the rule used to fire and duplicate the probe side. Without it the scan count would also
   * be satisfied by a plan that never became a broadcast hash join, which is a different reason for
   * the rule not to fire.
   *
   * This is a premise of the two tests above rather than a property of the rule: the number of
   * joins is left to their own assertion. Should a planner change stop picking a build-left
   * broadcast hash join here, retune the table sizes and the threshold.
   */
  private def assertBuildsFromTheLeft(df: DataFrame): Unit = {
    val joins = collectWithSubqueries(df.queryExecution.executedPlan) {
      case j: BroadcastHashJoinExec => j
    }
    assert(joins.nonEmpty && joins.forall(_.buildSide == BuildLeft),
      s"expected every broadcast hash join to build from the left, found ${joins.map(_.buildSide)}")
  }

  private def rightScansOf(df: DataFrame): Int = {
    collectWithSubqueries(df.queryExecution.executedPlan) {
      case s: FileSourceScanExec if s.tableIdentifier.exists(_.table == "dim") => s
    }.size
  }
}
