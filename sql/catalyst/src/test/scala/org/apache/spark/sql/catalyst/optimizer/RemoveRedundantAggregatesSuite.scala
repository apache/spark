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
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PythonUDAF}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StringType}

class RemoveRedundantAggregatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveRedundantAggregates", FixedPoint(10),
      RemoveRedundantAggregates,
      RemoveNoopOperators) :: Nil
  }

  private val relation = LocalRelation($"a".int, $"b".int)
  private val x = relation.subquery("x")
  private val y = relation.subquery("y")

  private def aggregates(e: Expression): Seq[Expression] = {
    Seq(
      count(e),
      PythonUDAF("pyUDAF", null, IntegerType, Seq(e), udfDeterministic = true)
        .toAggregateExpression()
    )
  }

  test("Remove redundant aggregate") {
    for (agg <- aggregates($"b")) {
      val query = relation
        .groupBy($"a")($"a", agg)
        .groupBy($"a")($"a")
        .analyze
      val expected = relation
        .select($"a")
        .groupBy($"a")($"a")
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove 2 redundant aggregates") {
    for (agg <- aggregates($"b")) {
      val query = relation
        .groupBy($"a")($"a", agg)
        .groupBy($"a")($"a")
        .groupBy($"a")($"a")
        .analyze
      val expected = relation
        .select($"a")
        .groupBy($"a")($"a")
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove redundant aggregate with different grouping") {
    val query = relation
      .groupBy($"a", $"b")($"a")
      .groupBy($"a")($"a")
      .analyze
    val expected = relation
      .select($"a")
      .groupBy($"a")($"a")
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove redundant aggregate with aliases") {
    for (agg <- aggregates($"b")) {
      val query = relation
        .groupBy($"a" + $"b")(($"a" + $"b") as "c", agg)
        .groupBy($"c")($"c")
        .analyze
      val expected = relation
        .select(($"a" + $"b") as "c")
        .groupBy($"c")($"c")
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove redundant aggregate with non-deterministic upper") {
    val query = relation
      .groupBy($"a")($"a")
      .groupBy($"a")($"a", rand(0) as "c")
      .analyze
    val expected = relation
      .select($"a")
      .groupBy($"a")($"a", rand(0) as "c")
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove redundant aggregate with non-deterministic lower") {
    // An integer key: the lower aggregate normalizes a float/double key, so it would be kept.
    val nondeterministic = (rand(0) * 10).cast(IntegerType)
    val query = relation
      .groupBy($"a", $"c")($"a", nondeterministic as "c")
      .groupBy($"a", $"c")($"a", $"c")
      .analyze
    val expected = relation
      .select($"a", $"b", rand(0) as "_nondeterministic")
      .select($"a", ($"_nondeterministic" * 10).cast(IntegerType) as "c")
      .groupBy($"a", $"c")($"a", $"c")
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Keep non-redundant aggregate - upper has duplicate sensitive agg expression") {
    for (agg <- aggregates($"b")) {
      val query = relation
        .groupBy($"a", $"b")($"a", $"b")
        // The count would change if we remove the first aggregate
        .groupBy($"a")($"a", agg)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, query)
    }
  }

  test("Remove redundant aggregate - upper has duplicate agnostic agg expression") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b")
      // The max and countDistinct does not change if there are duplicate values
      .groupBy($"a")($"a", max($"b"), countDistinct($"b"))
      .analyze
    val expected = relation
      .groupBy($"a")($"a", max($"b"), countDistinct($"b"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove redundant aggregate - upper has contains foldable expressions") {
    val originalQuery = x.groupBy($"a", $"b")($"a", $"b").groupBy($"a")($"a", TrueLiteral).analyze
    val correctAnswer = x.select($"a").groupBy($"a")($"a", TrueLiteral).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("Keep non-redundant aggregate - upper references agg expression") {
    for (agg <- aggregates($"b")) {
      val query = relation
        .groupBy($"a")($"a", agg as "c")
        .groupBy($"c")($"c")
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, query)
    }
  }

  test("Remove non-redundant aggregate - upper references non-deterministic non-grouping") {
    // An integer c: an upper aggregate on a float/double key c would be kept.
    val c = (($"a" + rand(0)) * 10).cast(IntegerType)
    val query = relation
      .groupBy($"a")($"a", c as "c")
      .groupBy($"a", $"c")($"a", $"c")
      .analyze
    val expected = relation
      .groupBy($"a")($"a", c as "c")
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("SPARK-60150: Keep upper aggregate on a non-deterministic double key") {
    // The upper aggregate normalizes the double key c, which a project would not.
    val query = relation
      .groupBy($"a")($"a", ($"a" + rand(0)) as "c")
      .groupBy($"a", $"c")($"a", $"c")
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-36194: Remove aggregation from left semi/anti join if aggregation the same") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr, "x.b".attr)
      val correctAnswer = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("SPARK-36194: Remove aggregation from left semi/anti join with alias") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy($"a", $"b")($"a", $"b".as("d"))
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "d".attr === "y.b".attr))
        .groupBy("x.a".attr, "d".attr)("x.a".attr, "d".attr)
      val correctAnswer = x.groupBy($"a", $"b")($"a", $"b".as("d"))
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "d".attr === "y.b".attr))

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("SPARK-36194: Remove aggregation from left semi/anti join if it is the sub aggregateExprs") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr)
      val correctAnswer = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .select("x.a".attr)

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("SPARK-36194: Transform down to remove more aggregates") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr, "x.b".attr)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr)
      val correctAnswer = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .select("x.a".attr)

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("SPARK-36194: Child distinct keys is the subset of required keys") {
    val originalQuery = relation
      .groupBy($"a")($"a", count($"b").as("cnt"))
      .groupBy($"a", $"cnt")($"a", $"cnt")
      .analyze
    val correctAnswer = relation
      .groupBy($"a")($"a", count($"b").as("cnt"))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-60150: Lower aggregate on a float key is kept only if the upper one reads it raw") {
    val d = $"d".double
    val a = $"a".int
    val doubles = LocalRelation(d, a)
    val lower = doubles.groupBy(d, a)(d, a)
    // Grouped by directly, the key is normalized by the upper aggregate itself.
    comparePlans(
      Optimize.execute(lower.groupBy(d)(d, max(a).as("m")).analyze),
      doubles.groupBy(d)(d, max(a).as("m")).analyze)
    // Inside an aggregate function or a computed grouping expression, the upper aggregate would
    // read the raw key without the lower one.
    val inFunction = lower.groupBy(a)(a, max(d).as("m")).analyze
    comparePlans(Optimize.execute(inFunction), inFunction)
    val inGrouping = lower.groupBy(d.cast(StringType))(d.cast(StringType).as("s")).analyze
    comparePlans(Optimize.execute(inGrouping), inGrouping)
    // So would an output of the lower aggregate computed from the key.
    val computed = doubles.groupBy(d)(d.cast(StringType).as("s"))
    val readsComputed = computed.groupBy($"s")($"s").analyze
    comparePlans(Optimize.execute(readsComputed), readsComputed)
    // Removed when the upper aggregate does not read the float key.
    comparePlans(
      Optimize.execute(lower.groupBy(a)(a).analyze),
      doubles.select(a).groupBy(a)(a).analyze)
  }

  test("SPARK-60150: Streaming aggregates on a float key are removed as before") {
    val d = $"d".double
    val a = $"a".int
    val streaming = LocalRelation(Seq(d, a), isStreaming = true)
    // Keeping them would add stateful operators, which existing checkpoints cannot restore.
    comparePlans(
      Optimize.execute(streaming.groupBy(d, a)(d, a).groupBy(a)(a, max(d).as("m")).analyze),
      streaming.groupBy(a)(a, max(d).as("m")).analyze)
    val lower = streaming.groupBy(d)(d, count(a).as("cnt"))
    val cnt = lower.analyze.output(1)
    comparePlans(Optimize.execute(lower.groupBy(d, cnt)(d, cnt).analyze), lower.analyze)
  }

  test("SPARK-60150: Keep group only aggregate that normalizes a float key") {
    val d = $"d".double
    val lower = LocalRelation(d, $"b".int).groupBy(d)(d, count($"b").as("cnt"))
    val cnt = lower.analyze.output(1)
    // The rule only sees that the upper aggregate outputs the float key normalized. It does not
    // check whether the child already does, so it keeps the upper aggregate.
    val originalQuery = lower.groupBy(d, cnt)(d, cnt).analyze
    comparePlans(Optimize.execute(originalQuery), originalQuery)
    // Removed when the float key is not output.
    comparePlans(Optimize.execute(lower.groupBy(d, cnt)(cnt).analyze), lower.select(cnt).analyze)
  }

  test("SPARK-36194: Child distinct keys are subsets and aggregateExpressions are foldable") {
    val originalQuery = x.groupBy($"a", $"b")($"a", $"b")
      .join(y, LeftSemi, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
      .groupBy("x.a".attr, "x.b".attr)(TrueLiteral)
      .analyze
    val correctAnswer = x.groupBy($"a", $"b")($"a", $"b")
      .join(y, LeftSemi, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
      .select(TrueLiteral)
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36194: Negative case: child distinct keys is not the subset of required keys") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery1 = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr)("x.a".attr)
        .analyze
      comparePlans(Optimize.execute(originalQuery1), originalQuery1)

      val originalQuery2 = x.groupBy($"a", $"b")($"a", $"b")
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr)(count("x.b".attr))
        .analyze
      comparePlans(Optimize.execute(originalQuery2), originalQuery2)
    }
  }

  test("SPARK-36194: Negative case: child distinct keys is empty") {
    val originalQuery = Distinct(x.groupBy($"a", $"b")($"a", TrueLiteral)).analyze
    comparePlans(Optimize.execute(originalQuery), originalQuery)
  }

  test("SPARK-53155: global lower aggregation should not be removed") {
    object OptimizeNonRemovedRedundantAgg extends RuleExecutor[LogicalPlan] {
      val batches = Batch("RemoveRedundantAggregates", FixedPoint(10),
        PropagateEmptyRelation,
        RemoveRedundantAggregates) :: Nil
    }

    val query = relation
      .groupBy()(Literal(1).as("col1"), Literal(2).as("col2"), Literal(3).as("col3"))
      .groupBy($"col1")(max($"col1"))
      .analyze
    val expected = relation
      .groupBy()(Literal(1).as("col1"), Literal(2).as("col2"), Literal(3).as("col3"))
      .groupBy($"col1")(max($"col1"))
      .analyze
    val optimized = OptimizeNonRemovedRedundantAgg.execute(query)
    comparePlans(optimized, expected)
  }
}
