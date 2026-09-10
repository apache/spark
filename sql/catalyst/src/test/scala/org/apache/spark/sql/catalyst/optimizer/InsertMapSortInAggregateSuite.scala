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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, MapSort}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate, LocalRelation, LogicalPlan, Project, Union}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

class InsertMapSortInAggregateSuite extends PlanTest {
  private val input = LocalRelation(Symbol("m").map(StringType, IntegerType))
  private val mapAttribute = input.output.head

  private def aliasesNamed(plan: LogicalPlan, name: String): Seq[Alias] = {
    plan.flatMap { node =>
      node.expressions.flatMap(_.collect {
        case alias @ Alias(_, aliasName) if aliasName == name => alias
      })
    }
  }

  test("reuse map sort when a grouping key is also a distinct argument") {
    val plan = Aggregate(
      Seq(mapAttribute),
      Seq(mapAttribute, countDistinct(mapAttribute).as("count")),
      input)
    val rewritten = InsertMapSortInAggregate(plan)
    val groupingAliases = aliasesNamed(rewritten, "_groupingmapsort")

    assert(groupingAliases.size == 1)
    assert(groupingAliases.head.child.isInstanceOf[MapSort])
    rewritten match {
      case Aggregate(Seq(groupingExpression), aggregateExpressions, _: Project, _) =>
        assert(groupingExpression.semanticEquals(groupingAliases.head.toAttribute))
        val distinctChildren = aggregateExpressions.flatMap(_.collect {
          case expression: AggregateExpression if expression.isDistinct =>
            expression.aggregateFunction.children
        }).flatten
        assert(distinctChildren.size == 1)
        assert(distinctChildren.head.semanticEquals(groupingAliases.head.toAttribute))
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }

  test("project complex distinct arguments only when needed") {
    val attributePlan = Aggregate(
      Nil,
      Seq(countDistinct(mapAttribute).as("count")),
      input)
    val complexPlan = Aggregate(
      Nil,
      Seq(countDistinct(namedStruct("m", mapAttribute)).as("count")),
      input)

    val rewrittenAttributePlan = InsertMapSortInAggregate(attributePlan)
    assert(rewrittenAttributePlan.collect { case _: Project => 1 }.size == 1)
    assert(aliasesNamed(rewrittenAttributePlan, "_distinctaggregateexpression").isEmpty)
    assert(aliasesNamed(rewrittenAttributePlan, "_distinctmapsort").size == 1)

    val rewrittenComplexPlan = InsertMapSortInAggregate(complexPlan)
    assert(rewrittenComplexPlan.collect { case _: Project => 1 }.size == 2)
    assert(aliasesNamed(rewrittenComplexPlan, "_distinctaggregateexpression").size == 1)
    assert(aliasesNamed(rewrittenComplexPlan, "_distinctmapsort").size == 1)
  }

  test("skip distinct argument normalization when disabled") {
    val plan = Aggregate(
      Nil,
      Seq(countDistinct(mapAttribute).as("count")),
      input)

    withSQLConf(SQLConf.INSERT_MAP_SORT_IN_DISTINCT_AGGREGATES_ENABLED.key -> "false") {
      comparePlans(InsertMapSortInAggregate(plan), plan)
    }
  }

  test("leave map-free aggregates untouched when another aggregate needs rewriting") {
    val scalarInput = LocalRelation(Symbol("i").int)
    val scalarAggregate = Aggregate(
      Nil,
      Seq(count(scalarInput.output.head).as("count")),
      scalarInput)
    val mapAggregate = Aggregate(
      Nil,
      Seq(countDistinct(mapAttribute).as("count")),
      input)

    InsertMapSortInAggregate(Union(Seq(scalarAggregate, mapAggregate))) match {
      case Union(Seq(rewrittenScalarAggregate, rewrittenMapAggregate), _, _) =>
        comparePlans(rewrittenScalarAggregate, scalarAggregate)
        assert(rewrittenScalarAggregate.collect { case _: Project => 1 }.isEmpty)
        assert(rewrittenMapAggregate.collect { case _: Project => 1 }.size == 1)
      case other =>
        fail(s"Unexpected plan:\n$other")
    }
  }
}
