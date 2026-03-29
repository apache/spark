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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, EmptyRelation, GlobalLimit, LocalLimit, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

// Test class to verify correct functioning of OptimizeLimitZero rule in various scenarios
class OptimizeLimitZeroSuite extends PlanTest {

  /** [[EmptyRelation]] or empty [[LocalRelation]] vs reference `expectedEmptyLocalRelation`. */
  private def assertEmptyRelationOutput(
      optimized: LogicalPlan,
      expectedEmptyLocalRelation: LocalRelation): Unit = {
    val expectedAttrs = expectedEmptyLocalRelation.output
    optimized match {
      case e: EmptyRelation =>
        assert(
          e.output.map(a => (a.name, a.dataType, a.nullable, a.metadata)) ==
            expectedAttrs.map(a => (a.name, a.dataType, a.nullable, a.metadata)))
      case lr: LocalRelation if lr.data.isEmpty =>
        comparePlans(optimized, expectedEmptyLocalRelation)
      case other =>
        fail(
          s"Expected EmptyRelation or empty LocalRelation, got ${other.getClass.getSimpleName}:\n" +
            other)
    }
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("OptimizeLimitZero", Once,
        ReplaceIntersectWithSemiJoin,
        EliminateLimits,
        PropagateEmptyRelation) :: Nil
  }

  val testRelation1 = LocalRelation.fromExternalRows(Seq($"a".int), data = Seq(Row(1)))
  val testRelation2 = LocalRelation.fromExternalRows(Seq($"b".int), data = Seq(Row(1)))

  test("Limit 0: return empty local relation") {
    val query = testRelation1.limit(0)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int)

    assertEmptyRelationOutput(optimized, correctAnswer)
  }

  test("Limit 0: individual LocalLimit 0 node") {
    val query = LocalLimit(0, testRelation1)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int)

    assertEmptyRelationOutput(optimized, correctAnswer)
  }

  test("Limit 0: individual GlobalLimit 0 node") {
    val query = GlobalLimit(0, testRelation1)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int)

    assertEmptyRelationOutput(optimized, correctAnswer)
  }

  Seq(
    (Inner, LocalRelation($"a".int, $"b".int), true),
    (
      LeftOuter,
      Project(Seq($"a", Literal(null).cast(IntegerType).as("b")), testRelation1).analyze,
      false),
    (RightOuter, LocalRelation($"a".int, $"b".int), true),
    (
      FullOuter,
      Project(Seq($"a", Literal(null).cast(IntegerType).as("b")), testRelation1).analyze,
      false)
  ).foreach { case (jt, correctAnswer, expectEmptyRelationOrEmptyLocal) =>
    test(s"Limit 0: for join type $jt") {
      val query = testRelation1
        .join(testRelation2.limit(0), joinType = jt, condition = Some($"a".attr === $"b".attr))

      val optimized = Optimize.execute(query.analyze)

      if (expectEmptyRelationOrEmptyLocal) {
        assertEmptyRelationOutput(optimized, correctAnswer.asInstanceOf[LocalRelation])
      } else {
        comparePlans(optimized, correctAnswer)
      }
    }
  }

  test("Limit 0: 3-way join") {
    val testRelation3 = LocalRelation.fromExternalRows(Seq($"c".int), data = Seq(Row(1)))

    val subJoinQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some($"a".attr === $"b".attr))
    val query = subJoinQuery
      .join(testRelation3.limit(0), joinType = Inner, condition = Some($"a".attr === $"c".attr))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int, $"b".int, $"c".int)

    assertEmptyRelationOutput(optimized, correctAnswer)
  }

  test("Limit 0: intersect") {
    val query = testRelation1
      .intersect(testRelation1.limit(0), isAll = false)

    val optimized = Optimize.execute(query.analyze)

    optimized match {
      case Distinct(child: EmptyRelation) =>
        val correctAnswer = LocalRelation($"a".int)

        assertEmptyRelationOutput(child, correctAnswer)
      case other =>
        fail(s"Expected Distinct(EmptyRelation), got ${other.getClass.getSimpleName}:\n$other")
    }
  }
}
