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

package org.apache.spark.sql.catalyst.optimizer

import scala.language.existentials

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, EqualNullSafe}
import org.apache.spark.sql.catalyst.plans.{LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStat, Distinct, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf

class RewriteIntersectWithSemiJoinSuite extends PlanTest with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        ReplaceIntersectWithSemiJoin) :: Nil
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("t1.col1") -> rangeColumnStat(10, 0),
    attr("t1.col2") -> rangeColumnStat(1000, 0),
    attr("t2.col1") -> rangeColumnStat(10, 0),
    attr("t2.col2") -> rangeColumnStat(1000, 0),
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  /**
   * Create 2 tables each with 1000 rows.
   * The first column `col1` has each value repeated 100 times. So it has only 10 unique values
   * The second column `col2` has each distinct value. So it has 1000 unique values
   */
  private val tbl1 = StatsTestPlan(
    outputList = Seq("t1.col1", "t1.col2").map(nameToAttr),
    rowCount = 1000,
    size = Some(1000 * (8 + 4 + 4)), // size = rows * (overhead + column length)
    attributeStats = AttributeMap(Seq("t1.col1", "t1.col2").map(nameToColInfo)))

  private val tbl2 = StatsTestPlan(
    outputList = Seq("t2.col1", "t2.col2").map(nameToAttr),
    rowCount = 1000,
    size = Some(1000 * (8 + 4 + 4)), // size = rows * (overhead + column length)
    attributeStats = AttributeMap(Seq("t2.col1", "t2.col2").map(nameToColInfo)))

  test("optimize intersect should not be used when distinct" +
    " reduction threshold is not met and it should happen when threshold is met") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> "101",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
    ) {
      // col1 has 10 distinct values in 1000 rows. So it reduces rows by ratio 100
      // which is less than OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD - 101
      val originalPlan = tbl1.select(nameToAttr("t1.col1")).intersect(
        tbl2.select(nameToAttr("t2.col1")), isAll = false)
      val optimizedPlan = Optimize.execute(originalPlan.analyze)

      val expectedPlan = Distinct(Join(
        tbl1.select(nameToAttr("t1.col1")),
        tbl2.select(nameToAttr("t2.col1")),
        LeftSemi,
        Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col1"))),
        JoinHint.NONE
      ))
      comparePlans(optimizedPlan, expectedPlan)

      // After setting OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD to 99,
      // Distinct pushdown should happen as reduction ration (100) is > 99
      withSQLConf(
        SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> "99"
      ) {
        val optimizedPlanNew = Optimize.execute(originalPlan.analyze)
        val expectedPlanNew = Join(
          Distinct(tbl1.select(nameToAttr("t1.col1"))),
          Distinct(tbl2.select(nameToAttr("t2.col1"))),
          LeftSemi,
          Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col1"))),
          JoinHint.NONE
        )
        comparePlans(optimizedPlanNew, expectedPlanNew)
      }
    }
  }

  test("test Distinct pushdown on right side when distinct reduction threshold is met") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> "99",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
    ) {
      val originalPlan = tbl1.select(nameToAttr("t1.col2")).intersect(
        tbl2.select(nameToAttr("t2.col1")), isAll = false)
      val optimizedPlan = Optimize.execute(originalPlan.analyze)

      val expectedPlan = Distinct(Join(
        tbl1.select(nameToAttr("t1.col2")),
        Distinct(tbl2.select(nameToAttr("t2.col1"))),
        LeftSemi,
        Some(EqualNullSafe(nameToAttr("t1.col2"), nameToAttr("t2.col1"))),
        JoinHint.NONE
      ))
      comparePlans(optimizedPlan, expectedPlan)
    }
  }

  test("test Distinct pushdown on right side when applying Distinct " +
    "makes it broadcastable") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> s"${Int.MaxValue}"
    ) {
      val sizePerRowRightSide = 4 + 8 // 4 byte - size of Integer, 8 byte - row overhead

      // Set AUTO_BROADCASTJOIN_THRESHOLD such that it allows broadcasting upto 15 Rows
      // But right side will have 10 rows, so it can be broadcasted.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"${15 * sizePerRowRightSide}") {
        val originalPlan = tbl1.select(nameToAttr("t1.col1")).intersect(
          tbl2.select(nameToAttr("t2.col1")), isAll = false)

        val expectedPlan = Distinct(Join(
          tbl1.select(nameToAttr("t1.col1")),
          Distinct(tbl2.select(nameToAttr("t2.col1"))),
          LeftSemi,
          Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col1"))),
          JoinHint.NONE))
        val optimizedPlan = Optimize.execute(originalPlan.analyze)
        comparePlans(optimizedPlan, expectedPlan)
      }

      // Set AUTO_BROADCASTJOIN_THRESHOLD such that it allows broadcasting upto 5 Rows
      // But right side will have 10 rows, so it can't be broadcasted.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"${5 * sizePerRowRightSide}") {
        val originalPlan = tbl1.select(nameToAttr("t1.col1")).intersect(
          tbl2.select(nameToAttr("t2.col1")), isAll = false)
        val expectedPlan = Distinct(Join(
          tbl1.select(nameToAttr("t1.col1")),
          tbl2.select(nameToAttr("t2.col1")),
          LeftSemi,
          Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col1"))),
          JoinHint.NONE))

        val optimizedPlan = Optimize.execute(originalPlan.analyze)
        comparePlans(optimizedPlan, expectedPlan)
      }
    }
  }

  test("test Distinct pushdown on left side") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> "99",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
    ) {
      val originalPlan = tbl1.select(nameToAttr("t1.col1")).intersect(
        tbl2.select(nameToAttr("t2.col2")), isAll = false)
      val optimizedPlan = Optimize.execute(originalPlan.analyze)

      val expectedPlan = Join(
        Distinct(tbl1.select(nameToAttr("t1.col1"))),
        tbl2.select(nameToAttr("t2.col2")),
        LeftSemi,
        Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col2"))),
        JoinHint.NONE
      )
      comparePlans(optimizedPlan, expectedPlan)
    }
  }

  test("optimize intersect should not be used when " +
    "SQLConf.OPTIMIZE_INTERSECT_ENABLED is disabled") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.OPTIMIZE_INTERSECT_ENABLED.key -> "false",
      SQLConf.OPTIMIZE_INTERSECT_DISTINCT_REDUCTION_THRESHOLD.key -> "99"
    ) {
      val originalPlan = tbl1.select(nameToAttr("t1.col1")).intersect(
        tbl2.select(nameToAttr("t2.col1")), isAll = false)
      val optimizedPlan = Optimize.execute(originalPlan.analyze)

      val expectedPlan = Distinct(Join(
        tbl1.select(nameToAttr("t1.col1")),
        tbl2.select(nameToAttr("t2.col1")),
        LeftSemi,
        Some(EqualNullSafe(nameToAttr("t1.col1"), nameToAttr("t2.col1"))),
        JoinHint.NONE))
      comparePlans(optimizedPlan, expectedPlan)
    }
  }
}
