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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class PruneFiltersSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown and Pruning", FixedPoint(1),
        CombineFilters,
        PruneFilters,
        PushPredicateThroughNonJoin,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  test("Constraints of isNull + LeftOuter") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val query = x.where("x.b".attr.isNull).join(y, LeftOuter)
    val queryWithUselessFilter = query.where("x.b".attr.isNull)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constraints of unionall") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int)
    val tr2 = LocalRelation($"d".int, $"e".int, $"f".int)
    val tr3 = LocalRelation($"g".int, $"h".int, $"i".int)

    val query =
      tr1.where($"a".attr > 10)
        .union(tr2.where($"d".attr > 10)
        .union(tr3.where($"g".attr > 10)))
    val queryWithUselessFilter = query.where($"a".attr > 10)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Pruning multiple constraints in the same run") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")

    val query = tr1
      .where("tr1.a".attr > 10 || "tr1.c".attr < 10)
      .join(tr2.where($"d".attr < 100), Inner, Some("tr1.a".attr === "tr2.a".attr))
    // different order of "tr2.a" and "tr1.a"
    val queryWithUselessFilter =
      query.where(
        ("tr1.a".attr > 10 || "tr1.c".attr < 10) &&
          $"d".attr < 100 &&
          "tr2.a".attr === "tr1.a".attr)

    val optimized = Optimize.execute(queryWithUselessFilter.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Partial pruning") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")

    // One of the filter condition does not exist in the constraints of its child
    // Thus, the filter is not removed
    val query = tr1
      .where("tr1.a".attr > 10)
      .join(tr2.where($"d".attr < 100), Inner, Some("tr1.a".attr === "tr2.d".attr))
    val queryWithExtraFilters =
      query.where("tr1.a".attr > 10 && $"d".attr < 100 && "tr1.a".attr === "tr2.a".attr)

    val optimized = Optimize.execute(queryWithExtraFilters.analyze)
    val correctAnswer = tr1
      .where("tr1.a".attr > 10)
      .join(tr2.where($"d".attr < 100),
        Inner,
        Some("tr1.a".attr === "tr2.a".attr && "tr1.a".attr === "tr2.d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("No predicate is pruned") {
    val x = testRelation.subquery("x")
    val y = testRelation.subquery("y")

    val query = x.where("x.b".attr.isNull).join(y, LeftOuter)
    val queryWithExtraFilters = query.where("x.b".attr.isNotNull)

    val optimized = Optimize.execute(queryWithExtraFilters.analyze)
    val correctAnswer =
      testRelation.where("b".attr.isNull).where("b".attr.isNotNull)
        .join(testRelation, LeftOuter).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Nondeterministic predicate is not pruned") {
    val originalQuery = testRelation.where(Rand(10) > 5).select($"a").where(Rand(10) > 5).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = testRelation.where(Rand(10) > 5).where(Rand(10) > 5).select($"a").analyze
    comparePlans(optimized, correctAnswer)
  }

  test("No pruning when constraint propagation is disabled") {
    val tr1 = LocalRelation($"a".int, $"b".int, $"c".int).subquery("tr1")
    val tr2 = LocalRelation($"a".int, $"d".int, $"e".int).subquery("tr2")

    val query = tr1
      .where("tr1.a".attr > 10 || "tr1.c".attr < 10)
      .join(tr2.where($"d".attr < 100), Inner, Some("tr1.a".attr === "tr2.a".attr))

    val queryWithUselessFilter =
      query.where(
        ("tr1.a".attr > 10 || "tr1.c".attr < 10) &&
          $"d".attr < 100)

    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val optimized = Optimize.execute(queryWithUselessFilter.analyze)
      // When constraint propagation is disabled, the useless filter won't be pruned.
      // It gets pushed down. Because the rule `CombineFilters` runs only once, there are redundant
      // and duplicate filters.
      val correctAnswer = tr1
        .where("tr1.a".attr > 10 || "tr1.c".attr < 10).where("tr1.a".attr > 10 || "tr1.c".attr < 10)
        .join(tr2.where($"d".attr < 100).where($"d".attr < 100),
          Inner, Some("tr1.a".attr === "tr2.a".attr)).analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-35273: CombineFilters support non-deterministic expressions") {
    val x = testRelation.where(!$"a".attr.in(1, 3, 5)).subquery("x")

    comparePlans(
      Optimize.execute(x.where($"a".attr === 7 && Rand(10) > 0.1).analyze),
      testRelation.where(!$"a".attr.in(1, 3, 5) && $"a".attr === 7).where(Rand(10) > 0.1).analyze)

    comparePlans(
      Optimize.execute(
        x.where($"a".attr === 7 && Rand(10) > 0.1 && $"b".attr === 1 && Rand(10) < 1.1).analyze),
      testRelation.where(!$"a".attr.in(1, 3, 5) && $"a".attr === 7 && $"b".attr === 1)
        .where(Rand(10) > 0.1 && Rand(10) < 1.1).analyze)
  }

  test("Streaming relation is not lost under true filter") {
    Seq("true", "false").foreach(x => withSQLConf(
        SQLConf.PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN.key -> x) {
      val streamingRelation =
        LocalRelation(Seq($"a".int, $"b".int, $"c".int), Nil, isStreaming = true)
      val originalQuery = streamingRelation.where(10 > 5).select($"a").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = streamingRelation.select($"a").analyze
      comparePlans(optimized, correctAnswer)
    })
  }

  test("Streaming relation is not lost under false filter") {
    withSQLConf(
        SQLConf.PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN.key -> "true") {
      val streamingRelation =
        LocalRelation(Seq($"a".int, $"b".int, $"c".int), Nil, isStreaming = true)
      val originalQuery = streamingRelation.where(10 < 5).select($"a").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = streamingRelation.select($"a").analyze
      comparePlans(optimized, correctAnswer)
    }

    withSQLConf(
        SQLConf.PRUNE_FILTERS_CAN_PRUNE_STREAMING_SUBPLAN.key -> "false") {
      val streamingRelation =
        LocalRelation(Seq($"a".int, $"b".int, $"c".int), Nil, isStreaming = true)
      val originalQuery = streamingRelation.where(10 < 5).select($"a").analyze
      val optimized = Optimize.execute(originalQuery)
      val correctAnswer = streamingRelation.where(10 < 5).select($"a").analyze
      comparePlans(optimized, correctAnswer)
    }
  }
}
