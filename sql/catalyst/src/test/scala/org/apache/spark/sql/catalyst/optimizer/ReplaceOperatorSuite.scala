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
import org.apache.spark.sql.catalyst.expressions.{Alias, Coalesce, If, Literal, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.BooleanType

class ReplaceOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        ReplaceDistinctWithAggregate,
        ReplaceExceptWithFilter,
        ReplaceExceptWithAntiJoin,
        ReplaceIntersectWithSemiJoin,
        ReplaceDeduplicateWithAggregate) :: Nil
  }

  test("replace Intersect with Left-semi Join") {
    val table1 = LocalRelation($"a".int, $"b".int)
    val table2 = LocalRelation($"c".int, $"d".int)

    val query = Intersect(table1, table2, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftSemi, Option($"a" <=> $"c" && $"b" <=> $"d"), JoinHint.NONE))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter while both the nodes are of type Filter") {
    val attributeA = $"a".int
    val attributeB = $"b".int

    val table1 = LocalRelation.fromExternalRows(Seq(attributeA, attributeB), data = Seq(Row(1, 2)))
    val table2 = Filter(attributeB === 2, Filter(attributeA === 1, table1))
    val table3 = Filter(attributeB < 1, Filter(attributeA >= 2, table1))

    val query = Except(table2, table3, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Filter(Not(Coalesce(Seq(attributeA >= 2 && attributeB < 1, Literal.FalseLiteral))),
          Filter(attributeB === 2, Filter(attributeA === 1, table1)))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter while only right node is of type Filter") {
    val attributeA = $"a".int
    val attributeB = $"b".int

    val table1 = LocalRelation.fromExternalRows(Seq(attributeA, attributeB), data = Seq(Row(1, 2)))
    val table2 = Filter(attributeB < 1, Filter(attributeA >= 2, table1))

    val query = Except(table1, table2, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Filter(Not(Coalesce(Seq(attributeA >= 2 && attributeB < 1, Literal.FalseLiteral))),
          table1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter while both the nodes are of type Project") {
    val attributeA = $"a".int
    val attributeB = $"b".int

    val table1 = LocalRelation.fromExternalRows(Seq(attributeA, attributeB), data = Seq(Row(1, 2)))
    val table2 = Project(Seq(attributeA, attributeB), table1)
    val table3 = Project(Seq(attributeA, attributeB),
      Filter(attributeB < 1, Filter(attributeA >= 2, table1)))

    val query = Except(table2, table3, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Filter(Not(Coalesce(Seq(attributeA >= 2 && attributeB < 1, Literal.FalseLiteral))),
          Project(Seq(attributeA, attributeB), table1))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter while only right node is of type Project") {
    val attributeA = $"a".int
    val attributeB = $"b".int

    val table1 = LocalRelation.fromExternalRows(Seq(attributeA, attributeB), data = Seq(Row(1, 2)))
    val table2 = Filter(attributeB === 2, Filter(attributeA === 1, table1))
    val table3 = Project(Seq(attributeA, attributeB),
      Filter(attributeB < 1, Filter(attributeA >= 2, table1)))

    val query = Except(table2, table3, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
          Filter(Not(Coalesce(Seq(attributeA >= 2 && attributeB < 1, Literal.FalseLiteral))),
            Filter(attributeB === 2, Filter(attributeA === 1, table1)))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter while left node is Project and right node is Filter") {
    val attributeA = $"a".int
    val attributeB = $"b".int

    val table1 = LocalRelation.fromExternalRows(Seq(attributeA, attributeB), data = Seq(Row(1, 2)))
    val table2 = Project(Seq(attributeA, attributeB),
      Filter(attributeB < 1, Filter(attributeA >= 2, table1)))
    val table3 = Filter(attributeB === 2, Filter(attributeA === 1, table1))

    val query = Except(table2, table3, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Filter(Not(Coalesce(Seq(attributeA === 1 && attributeB === 2, Literal.FalseLiteral))),
          Project(Seq(attributeA, attributeB),
            Filter(attributeB < 1, Filter(attributeA >= 2, table1))))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Left-anti Join") {
    val table1 = LocalRelation($"a".int, $"b".int)
    val table2 = LocalRelation($"c".int, $"d".int)

    val query = Except(table1, table2, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(table1.output, table1.output,
        Join(table1, table2, LeftAnti, Option($"a" <=> $"c" && $"b" <=> $"d"), JoinHint.NONE))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Except with Filter when only right filter can be applied to the left") {
    val table = LocalRelation(Seq($"a".int, $"b".int))
    val left = table.where($"b" < 1).select($"a").as("left")
    val right = table.where($"b" < 3).select($"a").as("right")

    val query = Except(left, right, isAll = false)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(left.output, right.output,
        Join(left, right, LeftAnti, Option($"left.a" <=> $"right.a"), JoinHint.NONE)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("replace Distinct with Aggregate") {
    val input = LocalRelation($"a".int, $"b".int)

    val query = Distinct(input)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = Aggregate(input.output, input.output, input)

    comparePlans(optimized, correctAnswer)
  }

  test("replace batch Deduplicate with Aggregate") {
    val input = LocalRelation($"a".int, $"b".int)
    val attrA = input.output(0)
    val attrB = input.output(1)
    val query = Deduplicate(Seq(attrA), input) // dropDuplicates("a")
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer =
      Aggregate(
        Seq(attrA),
        Seq(
          attrA,
          Alias(new First(attrB).toAggregateExpression(), attrB.name)(attrB.exprId)
        ),
        input)

    comparePlans(optimized, correctAnswer)
  }

  test("add one grouping key if necessary when replace Deduplicate with Aggregate") {
    val input = LocalRelation()
    val query = Deduplicate(Seq.empty, input) // dropDuplicates()
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Aggregate(Seq(Literal(1)), input.output, input)
    comparePlans(optimized, correctAnswer)
  }

  test("don't replace streaming Deduplicate") {
    val input = LocalRelation(Seq($"a".int, $"b".int), isStreaming = true)
    val attrA = input.output(0)
    val query = Deduplicate(Seq(attrA), input) // dropDuplicates("a")
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query)
  }

  test("SPARK-26366: ReplaceExceptWithFilter should handle properly NULL") {
    val basePlan = LocalRelation(Seq($"a".int, $"b".int))
    val otherPlan = basePlan.where($"a".in(1, 2) || $"b".in())
    val except = Except(basePlan, otherPlan, false)
    val result = OptimizeIn(Optimize.execute(except.analyze))
    val correctAnswer = Aggregate(basePlan.output, basePlan.output,
      Filter(!Coalesce(Seq(
        $"a".in(1, 2) || If($"b".isNotNull, Literal.FalseLiteral, Literal(null, BooleanType)),
        Literal.FalseLiteral)),
        basePlan)).analyze
    comparePlans(result, correctAnswer)
  }

  test("SPARK-26366: ReplaceExceptWithFilter should not transform non-deterministic") {
    val basePlan = LocalRelation(Seq($"a".int, $"b".int))
    val otherPlan = basePlan.where($"a" > rand(1L))
    val except = Except(basePlan, otherPlan, false)
    val result = Optimize.execute(except.analyze)
    val condition = basePlan.output.zip(otherPlan.output).map { case (a1, a2) =>
      a1 <=> a2 }.reduce( _ && _)
    val correctAnswer = Aggregate(basePlan.output, otherPlan.output,
      Join(basePlan, otherPlan, LeftAnti, Option(condition), JoinHint.NONE)).analyze
    comparePlans(result, correctAnswer)
  }
}
