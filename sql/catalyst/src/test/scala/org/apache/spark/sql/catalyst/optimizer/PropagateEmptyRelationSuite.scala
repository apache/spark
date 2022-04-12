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
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructType}

class PropagateEmptyRelationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PropagateEmptyRelation", Once,
        CombineUnions,
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin,
        PushPredicateThroughNonJoin,
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
        PushPredicateThroughNonJoin,
        PruneFilters,
        CollapseProject) :: Nil
  }

  val testRelation1 = LocalRelation.fromExternalRows(Seq($"a".int), data = Seq(Row(1)))
  val testRelation2 = LocalRelation.fromExternalRows(Seq($"b".int), data = Seq(Row(1)))
  val metadata = new MetadataBuilder().putLong("test", 1).build()
  val testRelation3 =
    LocalRelation.fromExternalRows(Seq($"c".int.notNull.withMetadata(metadata)), data = Seq(Row(1)))

  test("propagate empty relation through Union") {
    val query = testRelation1
      .where(false)
      .union(testRelation2.where(false))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int)

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32241: remove empty relation children from Union") {
    val query = testRelation1.union(testRelation2.where(false))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation1
    comparePlans(optimized, correctAnswer)

    val query2 = testRelation1.where(false).union(testRelation2)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = testRelation2.select($"b".as(Symbol("a"))).analyze
    comparePlans(optimized2, correctAnswer2)

    val query3 = testRelation1.union(testRelation2.where(false)).union(testRelation3)
    val optimized3 = Optimize.execute(query3.analyze)
    val correctAnswer3 = testRelation1.union(testRelation3)
    comparePlans(optimized3, correctAnswer3)

    val query4 = testRelation1.where(false).union(testRelation2).union(testRelation3)
    val optimized4 = Optimize.execute(query4.analyze)
    val correctAnswer4 = testRelation2.union(testRelation3).select($"b".as(Symbol("a"))).analyze
    comparePlans(optimized4, correctAnswer4)

    // Nullability can change from nullable to non-nullable
    val query5 = testRelation1.where(false).union(testRelation3)
    val optimized5 = Optimize.execute(query5.analyze)
    assert(query5.output.head.nullable, "Original output should be nullable")
    assert(!optimized5.output.head.nullable, "New output should be non-nullable")

    // Keep metadata
    val query6 = testRelation3.where(false).union(testRelation1)
    val optimized6 = Optimize.execute(query6.analyze)
    assert(optimized6.output.head.metadata == metadata, "New output should keep metadata")
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

      (true, false, Inner, Some(LocalRelation($"a".int, $"b".int))),
      (true, false, Cross, Some(LocalRelation($"a".int, $"b".int))),
      (true, false, LeftOuter,
        Some(Project(Seq($"a", Literal(null).cast(IntegerType).as(Symbol("b"))), testRelation1)
          .analyze)),
      (true, false, RightOuter, Some(LocalRelation($"a".int, $"b".int))),
      (true, false, FullOuter,
        Some(Project(Seq($"a", Literal(null).cast(IntegerType).as(Symbol("b"))), testRelation1)
          .analyze)),
      (true, false, LeftAnti, Some(testRelation1)),
      (true, false, LeftSemi, Some(LocalRelation($"a".int))),

      (false, true, Inner, Some(LocalRelation($"a".int, $"b".int))),
      (false, true, Cross, Some(LocalRelation($"a".int, $"b".int))),
      (false, true, LeftOuter, Some(LocalRelation($"a".int, $"b".int))),
      (false, true, RightOuter,
        Some(Project(Seq(Literal(null).cast(IntegerType).as(Symbol("a")), $"b"), testRelation2)
          .analyze)),
      (false, true, FullOuter,
        Some(Project(Seq(Literal(null).cast(IntegerType).as(Symbol("a")), $"b"), testRelation2)
          .analyze)),
      (false, true, LeftAnti, Some(LocalRelation($"a".int))),
      (false, true, LeftSemi, Some(LocalRelation($"a".int))),

      (false, false, Inner, Some(LocalRelation($"a".int, $"b".int))),
      (false, false, Cross, Some(LocalRelation($"a".int, $"b".int))),
      (false, false, LeftOuter, Some(LocalRelation($"a".int, $"b".int))),
      (false, false, RightOuter, Some(LocalRelation($"a".int, $"b".int))),
      (false, false, FullOuter, Some(LocalRelation($"a".int, $"b".int))),
      (false, false, LeftAnti, Some(LocalRelation($"a".int))),
      (false, false, LeftSemi, Some(LocalRelation($"a".int)))
    )

    testcases.foreach { case (left, right, jt, answer) =>
      val query = testRelation1
        .where(left)
        .join(testRelation2.where(right), joinType = jt, condition = Some($"a".attr === $"b".attr))
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer =
        answer.getOrElse(OptimizeWithoutPropagateEmptyRelation.execute(query.analyze))
      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-28220: Propagate empty relation through Join if condition is FalseLiteral") {
    val testcases = Seq(
      (Inner, Some(LocalRelation($"a".int, $"b".int))),
      (Cross, Some(LocalRelation($"a".int, $"b".int))),
      (LeftOuter,
        Some(Project(Seq($"a", Literal(null).cast(IntegerType).as(Symbol("b"))), testRelation1)
          .analyze)),
      (RightOuter,
        Some(Project(Seq(Literal(null).cast(IntegerType).as(Symbol("a")), $"b"), testRelation2)
          .analyze)),
      (FullOuter, None),
      (LeftAnti, Some(testRelation1)),
      (LeftSemi, Some(LocalRelation($"a".int)))
    )

    testcases.foreach { case (jt, answer) =>
      val query = testRelation1.join(testRelation2, joinType = jt, condition = Some(FalseLiteral))
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer =
        answer.getOrElse(OptimizeWithoutPropagateEmptyRelation.execute(query.analyze))
      comparePlans(optimized, correctAnswer)
    }
  }

  test("propagate empty relation through UnaryNode") {
    val query = testRelation1
      .where(false)
      .select($"a")
      .groupBy($"a")($"a")
      .where($"a" > 1)
      .orderBy($"a".asc)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int)

    comparePlans(optimized, correctAnswer)
  }

  test("propagate empty streaming relation through multiple UnaryNode") {
    val output = Seq($"a".int)
    val data = Seq(Row(1))
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val relation = LocalRelation(
      output,
      data.map(converter(_).asInstanceOf[InternalRow]),
      isStreaming = true)

    val query = relation
      .where(false)
      .select($"a")
      .where($"a" > 1)
      .where($"a" =!= 200)
      .orderBy($"a".asc)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation(output, isStreaming = true)

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate empty streaming relation through agg") {
    val output = Seq($"a".int)
    val data = Seq(Row(1))
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val relation = LocalRelation(
      output,
      data.map(converter(_).asInstanceOf[InternalRow]),
      isStreaming = true)

    val query = relation
      .groupBy($"a")($"a")

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate non-empty local relation") {
    val query = testRelation1
      .where(true)
      .groupBy($"a")($"a")
      .where($"a" > 1)
      .orderBy($"a".asc)
      .select($"a")

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation1
      .where($"a" > 1)
      .groupBy($"a")($"a")
      .orderBy($"a".asc)
      .select($"a")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("propagate empty relation through Aggregate with grouping expressions") {
    val query = testRelation1
      .where(false)
      .groupBy($"a")($"a", ($"a" + 1).as(Symbol("x")))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int, $"x".int).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("don't propagate empty relation through Aggregate without grouping expressions") {
    val query = testRelation1
      .where(false)
      .groupBy()()

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = LocalRelation($"a".int).groupBy()().analyze

    comparePlans(optimized, correctAnswer)
  }

  test("propagate empty relation keeps the plan resolved") {
    val query = testRelation1.join(
      LocalRelation($"a".int, $"b".int), UsingJoin(FullOuter, "a" :: Nil), None)
    val optimized = Optimize.execute(query.analyze)
    assert(optimized.resolved)
  }

  test("should not optimize away limit if streaming") {
    val query = LocalRelation(Nil, Nil, isStreaming = true).limit(1).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-37689: Expand should be supported PropagateEmptyRelation") {
    val query = Expand(Seq(Seq($"a", $"b", "null"), Seq($"a", "null", $"c")), Seq($"a", $"b", $"c"),
      LocalRelation.fromExternalRows(Seq($"a".int, $"b".int, $"c".int), Nil)).analyze
    val optimized = Optimize.execute(query)
    val expected = LocalRelation.fromExternalRows(Seq($"a".int, $"b".int, $"c".int), Nil)
    comparePlans(optimized, expected)
  }

  test("SPARK-37904: Improve rebalance in PropagateEmptyRelation") {
    val emptyRelation = LocalRelation($"a".int)
    val expected = emptyRelation.analyze

    // test root node
    val plan1 = emptyRelation.rebalance($"a").analyze
    val optimized1 = Optimize.execute(plan1)
    comparePlans(optimized1, expected)

    // test non-root node
    val plan2 = emptyRelation.rebalance($"a").where($"a" > 0).select($"a").analyze
    val optimized2 = Optimize.execute(plan2)
    comparePlans(optimized2, expected)
  }
}
