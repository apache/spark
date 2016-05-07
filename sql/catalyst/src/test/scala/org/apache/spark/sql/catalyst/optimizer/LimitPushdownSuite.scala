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
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class LimitPushdownSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
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

  // Union ---------------------------------------------------------------------------------------

  test("Union: limit to each side") {
    val unionQuery = Union(testRelation, testRelation2).limit(1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(1, Union(LocalLimit(1, testRelation), LocalLimit(1, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each side with constant-foldable limit expressions") {
    val unionQuery = Union(testRelation, testRelation2).limit(Add(1, 1))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(2, Union(LocalLimit(2, testRelation), LocalLimit(2, testRelation2))).analyze
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

  // Outer join ----------------------------------------------------------------------------------

  test("left outer join") {
    val originalQuery = x.join(y, LeftOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, y).join(y, LeftOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("right outer join") {
    val originalQuery = x.join(y, RightOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, x.join(LocalLimit(1, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("larger limits are not pushed on top of smaller ones in right outer join") {
    val originalQuery = x.join(y.limit(5), RightOuter).limit(10)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(10, x.join(Limit(5, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("full outer join where neither side is limited and both sides have same statistics") {
    assert(x.statistics.sizeInBytes === y.statistics.sizeInBytes)
    val originalQuery = x.join(y, FullOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, x).join(y, FullOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("full outer join where neither side is limited and left side has larger statistics") {
    val xBig = testRelation.copy(data = Seq.fill(2)(null)).subquery('x)
    assert(xBig.statistics.sizeInBytes > y.statistics.sizeInBytes)
    val originalQuery = xBig.join(y, FullOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, xBig).join(y, FullOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("full outer join where neither side is limited and right side has larger statistics") {
    val yBig = testRelation.copy(data = Seq.fill(2)(null)).subquery('y)
    assert(x.statistics.sizeInBytes < yBig.statistics.sizeInBytes)
    val originalQuery = x.join(yBig, FullOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, x.join(LocalLimit(1, yBig), FullOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("full outer join where both sides are limited") {
    val originalQuery = x.limit(2).join(y.limit(2), FullOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, Limit(2, x).join(Limit(2, y), FullOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }
}

