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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StructType}

class PropagateEmptyRelationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PropagateEmptyRelation", Once,
        CombineUnions,
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin,
        PushDownPredicate,
        PruneFilters,
        PropagateEmptyRelation,
        CollapseProject) :: Nil
  }

  object OptimizeWithoutPropagateEmptyRelation extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("OptimizeWithoutPropagateEmptyRelation", Once,
        CombineUnions,
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin,
        PushDownPredicate,
        PruneFilters,
        CollapseProject) :: Nil
  }

  val testRelation1 = LocalRelation.fromExternalRows(Seq('a.int), data = Seq(Row(1)))
  val testRelation2 = LocalRelation.fromExternalRows(Seq('b.int), data = Seq(Row(1)))

  test("propagate empty relation through Union") {
    val query = testRelation1
      .where(false)
      .union(testRelation2.where(false))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation('a.int)

    comparePlans(optimized, correctAnswer)
  }

  test("propagate empty relation through Join") {
    // Testcases are tuples of (left predicate, right predicate, joinType, correct answer)
    // Note that `None` is used to compare with OptimizeWithoutPropagateEmptyRelation.
    val testcases = Seq(
      (true, true, Inner, None),
      (true, true, Cross, None),
      (true, true, LeftOuter, None),
      (true, true, RightOuter, None),
      (true, true, FullOuter, None),
      (true, true, LeftAnti, None),
      (true, true, LeftSemi, None),

      (true, false, Inner, Some(LocalRelation('a.int, 'b.int))),
      (true, false, Cross, Some(LocalRelation('a.int, 'b.int))),
      (true, false, LeftOuter,
        Some(Project(Seq('a, Literal(null).cast(IntegerType).as('b)), testRelation1).analyze)),
      (true, false, RightOuter, Some(LocalRelation('a.int, 'b.int))),
      (true, false, FullOuter,
        Some(Project(Seq('a, Literal(null).cast(IntegerType).as('b)), testRelation1).analyze)),
      (true, false, LeftAnti, Some(testRelation1)),
      (true, false, LeftSemi, Some(LocalRelation('a.int))),

      (false, true, Inner, Some(LocalRelation('a.int, 'b.int))),
      (false, true, Cross, Some(LocalRelation('a.int, 'b.int))),
      (false, true, LeftOuter, Some(LocalRelation('a.int, 'b.int))),
      (false, true, RightOuter,
        Some(Project(Seq(Literal(null).cast(IntegerType).as('a), 'b), testRelation2).analyze)),
      (false, true, FullOuter,
        Some(Project(Seq(Literal(null).cast(IntegerType).as('a), 'b), testRelation2).analyze)),
      (false, true, LeftAnti, Some(LocalRelation('a.int))),
      (false, true, LeftSemi, Some(LocalRelation('a.int))),

      (false, false, Inner, Some(LocalRelation('a.int, 'b.int))),
      (false, false, Cross, Some(LocalRelation('a.int, 'b.int))),
      (false, false, LeftOuter, Some(LocalRelation('a.int, 'b.int))),
      (false, false, RightOuter, Some(LocalRelation('a.int, 'b.int))),
      (false, false, FullOuter, Some(LocalRelation('a.int, 'b.int))),
      (false, false, LeftAnti, Some(LocalRelation('a.int))),
      (false, false, LeftSemi, Some(LocalRelation('a.int)))
    )

    testcases.foreach { case (left, right, jt, answer) =>
      val query = testRelation1
        .where(left)
        .join(testRelation2.where(right), joinType = jt, condition = Some('a.attr == 'b.attr))
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer =
        answer.getOrElse(OptimizeWithoutPropagateEmptyRelation.execute(query.analyze))
      comparePlans(optimized, correctAnswer)
    }
  }

  test("propagate empty relation through UnaryNode") {
    val query = testRelation1
      .where(false)
      .select('a)
      .groupBy('a)('a)
      .where('a > 1)
      .orderBy('a.asc)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation('a.int)

    comparePlans(optimized, correctAnswer)
  }

  test("propagate empty streaming relation through multiple UnaryNode") {
    val output = Seq('a.int)
    val data = Seq(Row(1))
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val relation = LocalRelation(
      output,
      data.map(converter(_).asInstanceOf[InternalRow]),
      isStreaming = true)

    val query = relation
      .where(false)
      .select('a)
      .where('a > 1)
      .where('a =!= 200)
      .orderBy('a.asc)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation(output, isStreaming = true)

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate empty streaming relation through agg") {
    val output = Seq('a.int)
    val data = Seq(Row(1))
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val relation = LocalRelation(
      output,
      data.map(converter(_).asInstanceOf[InternalRow]),
      isStreaming = true)

    val query = relation
      .groupBy('a)('a)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate non-empty local relation") {
    val query = testRelation1
      .where(true)
      .groupBy('a)('a)
      .where('a > 1)
      .orderBy('a.asc)
      .select('a)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation1
      .where('a > 1)
      .groupBy('a)('a)
      .orderBy('a.asc)
      .select('a)

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("propagate empty relation through Aggregate with grouping expressions") {
    val query = testRelation1
      .where(false)
      .groupBy('a)('a, ('a + 1).as('x))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation('a.int, 'x.int).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate empty relation through Aggregate without grouping expressions") {
    val query = testRelation1
      .where(false)
      .groupBy()()

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation('a.int).groupBy()().analyze

    comparePlans(optimized, correctAnswer)
  }

  test("propagate empty relation keeps the plan resolved") {
    val query = testRelation1.join(
      LocalRelation('a.int, 'b.int), UsingJoin(FullOuter, "a" :: Nil), None)
    val optimized = Optimize.execute(query.analyze)
    assert(optimized.resolved)
  }
}
