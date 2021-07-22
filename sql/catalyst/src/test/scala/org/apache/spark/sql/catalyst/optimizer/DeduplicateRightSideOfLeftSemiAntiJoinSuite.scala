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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStat, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class DeduplicateRightSideOfLeftSemiAntiJoinSuite extends PlanTest
  with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Deduplicate Right Side of LeftSemiAnti Join", FixedPoint(10),
        DeduplicateRightSideOfLeftSemiAntiJoin) :: Nil
  }

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()
  private val c = AttributeReference("c", IntegerType)()

  private val x = StatsTestPlan(
    outputList = Seq(a),
    attributeStats = AttributeMap(Seq(a ->
      ColumnStat(distinctCount = Some(10000), min = Some(1), max = Some(10),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))),
    rowCount = 10000,
    size = Some(10000 * (8 + 4))).as("x")

  private val y = StatsTestPlan(
    outputList = Seq(b),
    attributeStats = AttributeMap(Seq(b ->
      ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(100),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))),
    rowCount = 1000,
    size = Some(1000 * (8 + 4))).as("y")

  private val z = StatsTestPlan(
    outputList = Seq(c),
    attributeStats = AttributeMap(Seq(c ->
      ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))),
    rowCount = 100,
    size = Some(100 * (8 + 4))).as("z")

  test("SPARK-36245: Deduplicate the right side of left semi/anti join") {
    Seq(true, false).foreach { cboEnabled =>
      Seq(-1, 10485760).foreach { threshold =>
        Seq(LeftSemi, LeftAnti).foreach { joinType =>
          withSQLConf(
            SQLConf.CBO_ENABLED.key -> cboEnabled.toString,
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
            // The right side has duplicate values
            val query = x.join(y, joinType, Some('a <=> 'b)).analyze
            val correctAnswer = if (cboEnabled && threshold == -1) {
              x.join(Aggregate(y.output, y.output, y), joinType, Some('a <=> 'b)).analyze
            } else {
              query
            }

            comparePlans(Optimize.execute(query), correctAnswer)
          }
        }
      }
    }
  }

  test("SPARK-36245: Should not deduplicate the right side if can not reduce the data") {
    withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // The right side do not have duplicate values
      val query = x.join(z, LeftSemi, Some('a <=> 'c)).analyze

      comparePlans(Optimize.execute(query), query)
    }
  }
}
