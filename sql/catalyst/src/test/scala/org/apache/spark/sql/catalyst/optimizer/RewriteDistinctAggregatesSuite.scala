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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Literal, Round}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{IntegerType, StringType}

class RewriteDistinctAggregatesSuite extends PlanTest {
  val nullInt = Literal(null, IntegerType)
  val nullString = Literal(null, StringType)
  val testRelation = LocalRelation($"a".string, $"b".string, $"c".string, $"d".string, $"e".int)
  val testRelation2 = LocalRelation($"a".double, $"b".int, $"c".int, $"d".int, $"e".int)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, Aggregate(_, _, _: Expand, _), _) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single distinct group") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"e"))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with partial aggregates") {
    val input = testRelation
      .groupBy($"a", $"d")(
        countDistinct($"e", $"c").as("agg1"),
        max($"b").as("agg2"))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("multiple distinct groups") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"b", $"c"), countDistinct($"d"))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with partial aggregates") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"b", $"c"), countDistinct($"d"), sum($"e"))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with non-partial aggregates") {
    val input = testRelation
      .groupBy($"a")(
        countDistinct($"b", $"c"),
        countDistinct($"d"),
        CollectSet($"b").toAggregateExpression())
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("SPARK-40382: eliminate multiple distinct groups due to superficial differences") {
    val input = testRelation2
      .groupBy($"a")(
        countDistinct($"b" + $"c").as("agg1"),
        countDistinct($"c" + $"b").as("agg2"),
        max($"c").as("agg3"))
      .analyze

    val rewrite = RewriteDistinctAggregates(input)
    rewrite match {
      case Aggregate(_, _, _: LocalRelation, _) =>
      case _ => fail(s"Plan is not as expected:\n$rewrite")
    }
  }

  test("SPARK-40382: reduce multiple distinct groups due to superficial differences") {
    val input = testRelation2
      .groupBy($"a")(
        countDistinct($"b" + $"c" + $"d").as("agg1"),
        countDistinct($"d" + $"c" + $"b").as("agg2"),
        countDistinct($"b" + $"c").as("agg3"),
        countDistinct($"c" + $"b").as("agg4"),
        max($"c").as("agg5"))
      .analyze

    val rewrite = RewriteDistinctAggregates(input)
    rewrite match {
      case Aggregate(_, _, Aggregate(_, _, e: Expand, _), _) =>
        assert(e.projections.size == 3)
      case _ => fail(s"Plan is not rewritten:\n$rewrite")
    }
  }

  test("SPARK-49261: Literals in grouping expressions shouldn't result in unresolved aggregation") {
    val relation = testRelation2
      .select(Literal(6).as("gb"), $"a", $"b", $"c", $"d")
    val input = relation
      .groupBy($"a", $"gb")(
        countDistinct($"b").as("agg1"),
        countDistinct($"d").as("agg2"),
        Round(sum($"c").as("sum1"), 6)).analyze
    val rewriteFold = FoldablePropagation(input)
    // without the fix, the below produces an unresolved plan
    val rewrite = RewriteDistinctAggregates(rewriteFold)
    if (!rewrite.resolved) {
      fail(s"Plan is not as expected:\n$rewrite")
    }
  }
}
