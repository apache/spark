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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.DoubleType

class JoinFilterSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
        Batch("Filter Pushdown", Once,
          SamplePushDown,
          CombineFilters,
          PushPredicateThroughProject,
          BooleanSimplification,
          PushPredicateThroughJoin,
          PushPredicateThroughGenerate,
          PushPredicateThroughAggregate,
          ColumnPruning,
          ProjectCollapsing,
          AddJoinKeyNullabilityFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  test("joins infer is NOT NULL on equijoin keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = x.join(y).
      where("x.b".attr === "y.b".attr)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
        Filter(IsNotNull("x.b".attr), x).join(
        Filter(IsNotNull("y.b".attr), y), Inner, Some("x.b".attr === "y.b".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins infer is NOT NULL one key") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = x.join(y).
      where("x.b".attr + 1 === "y.b".attr)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = x.join(
        Filter(IsNotNull("y.b".attr), y), Inner, Some("x.b".attr + 1 === "y.b".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins infer is NOT NULL for cast") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = x.join(y).
      where(Cast("x.b".attr, DoubleType) === "y.b".attr)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      Filter(IsNotNull("x.b".attr), x).join(
        Filter(IsNotNull("y.b".attr), y), Inner,
            Some(Cast("x.b".attr, DoubleType) === Cast("y.b".attr, DoubleType))).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("joins infer is NOT NULL on join keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = x.join(y).
      where("x.b".attr >= "y.b".attr).where("x.b".attr < "y.b".attr)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      Filter(IsNotNull("x.b".attr), x).join(
        Filter(IsNotNull("y.b".attr), y), Inner,
        Some(And("x.b".attr >= "y.b".attr, "x.b".attr < "y.b".attr))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins infer is NOT NULL on not equal keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery = x.join(y).
      where("x.b".attr !== "y.a".attr)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      Filter(IsNotNull("x.b".attr), x).join(
        Filter(IsNotNull("y.a".attr), y), Inner, Some("x.b".attr !== "y.a".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins infer is NOT NULL with multiple joins") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)
    val t3 = testRelation.subquery('t3)
    val t4 = testRelation.subquery('t4)

    val originalQuery = t1.join(t2).join(t3).join(t4)
      .where("t1.b".attr === "t2.b".attr)
      .where("t1.b".attr === "t3.b".attr)
      .where("t1.b".attr === "t4.b".attr)
      .where("t2.b".attr === "t3.b".attr)
      .where("t2.b".attr === "t4.b".attr)
      .where("t3.b".attr === "t4.b".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    var numFilters = 0
    optimized.foreach { p => p match {
      case Filter(condition, _) => {
        var containsIsNotNull = false
        condition.foreach { c =>
          if (c.isInstanceOf[IsNotNull]) containsIsNotNull = true
        }
        if (containsIsNotNull) {
          // If the condition contained IsNotNull, then it must be generated. Verify the entire
          // condition is IsNotNull (to verify IsNotNull is not repeated) and that we generate the
          // expected number of filters.
          assert(condition.isInstanceOf[IsNotNull])
          numFilters += 1
        }
      }
      case _ =>
    }}
    assertResult(4)(numFilters)
  }
}
