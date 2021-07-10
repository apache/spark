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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}

class BroadcastJoinOuterJoinStreamSideSuite extends PlanTest with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(10),
        CombineFilters,
        BroadcastJoinOuterJoinStreamSide,
        BooleanSimplification,
        CollapseProject) :: Nil
  }

  private val testRelation = StatsTestPlan(
    outputList = Seq("a", "b", "c").map(attr),
    rowCount = 4,
    AttributeMap.empty,
    size = Some(32))
  private val testRelation1 = StatsTestPlan(
    outputList = Seq("d", "e", "f").map(attr),
    rowCount = 4000000,
    AttributeMap.empty,
    size = Some(10737418240L))
  private val testRelation2 = StatsTestPlan(
    outputList = Seq("x", "y").map(attr),
    rowCount = 500000,
    AttributeMap.empty,
    size = Some(53687091200L))

  test("Broadcast the stream side by size") {
    val joinCondition = Some('a === 'd)
    Seq(LeftOuter, LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = testRelation
        .join(testRelation1, joinType = joinType, condition = joinCondition)

      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = testRelation.join(
        Join(testRelation1,
          RebalancePartitions(Seq('a), testRelation, true),
          LeftSemi,
          Some('a === 'd),
          JoinHint.NONE), joinType = joinType, condition = joinCondition)
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("Broadcast the stream side by hint") {
    val joinCondition = Some('d === 'y)
    Seq(LeftOuter, LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = Join(testRelation1, testRelation2, joinType = joinType,
        condition = joinCondition, JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))

      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = Join(testRelation1,
        Join(testRelation2,
          RebalancePartitions(Seq('d), testRelation1, true),
          LeftSemi,
          Some('d === 'y),
          JoinHint.NONE), joinType = joinType, condition = joinCondition,
        JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("Should not broadcast the stream side if stream side is large") {
    val joinCondition = Some('d === 'y)
    Seq(LeftOuter, LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = Join(testRelation1, testRelation2, joinType = joinType,
        condition = joinCondition, JoinHint.NONE)

      val optimized = Optimize.execute(originalQuery.analyze)

      comparePlans(optimized, originalQuery.analyze)
    }
  }
}
