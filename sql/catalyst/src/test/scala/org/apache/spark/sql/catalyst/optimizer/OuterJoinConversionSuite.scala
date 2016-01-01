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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class OuterJoinConversionSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("OuterJoinConversion", Once,
        OuterJoinConversion,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int, 'e.int, 'f.int)

  test("joins: full outer to inner") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where("x.b".attr >= 1 && "y.d".attr >= 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('b >= 1)
    val right = testRelation1.where('d >= 2)
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: full outer to right") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr)).where("y.d".attr > 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where('d > 2)
    val correctAnswer =
      left.join(right, RightOuter, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: full outer to left") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr)).where("x.a".attr <=> 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('a <=> 2)
    val right = testRelation1
    val correctAnswer =
      left.join(right, LeftOuter, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: right to inner") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery =
      x.join(y, RightOuter, Option("x.a".attr === "y.d".attr)).where("x.b".attr > 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where('b > 2)
    val right = testRelation1
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: left to inner") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr)).where("y.e".attr.isNotNull)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where('e.isNotNull)
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }
}
