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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.optimizer.customAnalyze._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class DeduplicateRightSideOfLeftSemiAntiJoinSuite extends PlanTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED)
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Push Partial Aggregation", FixedPoint(10),
        PullOutGroupingExpressions,
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject,
        DeduplicateRightSideOfLeftSemiAntiJoin,
        ResolveTimeZone,
        SimplifyCasts) :: Nil
  }

  val testRelation1 = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation2 = LocalRelation($"x".int, $"y".int, $"z".int)

  test("Deduplicate the right side of left semi anti join") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(LeftSemi, LeftAnti).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType, condition = Some('a === 'x))
            .analyze

          val correctRight = PartialAggregate(Seq('x), Seq('x), testRelation2.select('x)).as("r")
          val correctAnswer = testRelation1.join(correctRight, joinType = joinType,
            condition = Some('a === 'x))
            .analyzePlan

          if (threshold < 0) {
            comparePlans(Optimize.execute(originalQuery), correctAnswer)
          } else {
            comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
          }
        }
      }
    }
  }

  test("Do not deduplicate if right side is aggregate") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(LeftSemi, LeftAnti).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2.groupBy('x, 'y)('x, 'y), joinType = joinType,
              condition = Some('a === 'x))
            .analyze

          comparePlans(Optimize.execute(originalQuery),
            CollapseProject(ColumnPruning(originalQuery)))
        }
      }
    }
  }
}
