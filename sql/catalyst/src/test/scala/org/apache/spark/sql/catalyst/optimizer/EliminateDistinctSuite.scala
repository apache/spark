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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class EliminateDistinctSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", Once,
        EliminateDistinct) :: Nil
  }

  val testRelation = LocalRelation($"a".int)
  val testRelation2 = LocalRelation($"a".int, $"b".string)

  Seq(
    Max(_),
    Min(_),
    BitAndAgg(_),
    BitOrAgg(_),
    First(_, ignoreNulls = true),
    First(_, ignoreNulls = false),
    Last(_, ignoreNulls = true),
    Last(_, ignoreNulls = false),
    CollectSet(_: Expression)
  ).foreach {
    aggBuilder =>
      val agg = aggBuilder($"a")
      test(s"Eliminate Distinct in $agg") {
        val query = testRelation
          .select(agg.toAggregateExpression(isDistinct = true).as("result"))
          .analyze
        val answer = testRelation
          .select(agg.toAggregateExpression(isDistinct = false).as("result"))
          .analyze
        assert(query != answer)
        comparePlans(Optimize.execute(query), answer)
      }

      test(s"SPARK-38177: Eliminate Distinct in non-root $agg") {
        val query = testRelation
          .select(agg.toAggregateExpression(isDistinct = true).as("result"))
          .limit(1)
          .analyze
        val answer = testRelation
          .select(agg.toAggregateExpression(isDistinct = false).as("result"))
          .limit(1)
          .analyze
        assert(query != answer)
        comparePlans(Optimize.execute(query), answer)
      }
  }

  test("SPARK-38832: Remove unnecessary distinct in aggregate expression by distinctKeys") {
    val q1 = testRelation2.groupBy($"a")($"a")
      .rebalance().groupBy()(countDistinct($"a") as "x", sumDistinct($"a") as "y").analyze
    val r1 = testRelation2.groupBy($"a")($"a")
      .rebalance().groupBy()(count($"a") as "x", sum($"a") as "y").analyze
    comparePlans(Optimize.execute(q1), r1)

    // not a subset of distinct attr
    val q2 = testRelation2.groupBy($"a", $"b")($"a", $"b")
      .rebalance().groupBy()(countDistinct($"a") as "x", sumDistinct($"a") as "y").analyze
    comparePlans(Optimize.execute(q2), q2)

    // child distinct key is empty
    val q3 = testRelation2.groupBy($"a")(countDistinct($"a") as "x").analyze
    comparePlans(Optimize.execute(q3), q3)
  }
}
