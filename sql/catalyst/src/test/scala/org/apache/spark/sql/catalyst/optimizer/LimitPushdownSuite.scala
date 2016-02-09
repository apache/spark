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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class LimitPushdownSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("Limit pushdown", FixedPoint(100),
        LimitPushDown,
        CombineLimits,
        ConstantFolding,
        BooleanSimplification) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  private val testRelation2 = LocalRelation('d.int, 'e.int, 'f.int)
  private val x = testRelation.subquery('x)
  private val y = testRelation.subquery('y)

  test("Union: limit to each side") {
    val unionQuery = Union(testRelation, testRelation2).limit(1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(1, Union(LocalLimit(1, testRelation), LocalLimit(1, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each side with the new limit number") {
    val unionQuery = Union(testRelation, testRelation2.limit(3)).limit(1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(1, Union(LocalLimit(1, testRelation), LocalLimit(1, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: no limit to both sides if children having smaller limit values") {
    val unionQuery = Union(testRelation.limit(1), testRelation2.select('d).limit(1)).limit(2)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(2, Union(testRelation.limit(1), testRelation2.select('d).limit(1))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each sides if children having larger limit values") {
    val testLimitUnion = Union(testRelation.limit(3), testRelation2.select('d).limit(4))
    val unionQuery = testLimitUnion.limit(2)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(2, Union(LocalLimit(2, testRelation), LocalLimit(2, testRelation2.select('d)))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("push down left outer join") {
    val originalQuery = x.join(y, LeftOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, y).join(y, LeftOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("push down right outer join") {
    val originalQuery = x.join(y, RightOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, x.join(LocalLimit(1, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("push down full outer join") {
    val originalQuery = x.join(y, FullOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, x).join(LocalLimit(1, y), FullOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }
}

