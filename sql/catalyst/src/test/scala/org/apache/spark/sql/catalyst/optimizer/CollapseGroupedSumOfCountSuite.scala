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
import org.apache.spark.sql.catalyst.expressions.{EvalMode, Literal, NumericEvalContext}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class CollapseGroupedSumOfCountSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CollapseGroupedSumOfCount", Once,
      CollapseGroupedSumOfCount,
      RemoveNoopOperators) :: Nil
  }

  private val relation = LocalRelation($"a".int, $"b".int)

  test("SPARK-57043: collapse grouped sum of count in legacy and ANSI modes") {
    Seq("false", "true").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val query = relation
          .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
          .groupBy($"a")($"a", sum($"cnt").as("total"))
          .analyze
        val expected = relation
          .select($"a")
          .groupBy($"a")($"a", sum(Literal(1L)).as("total"))
          .analyze
        comparePlans(Optimize.execute(query), expected)
      }
    }
  }

  test("SPARK-57043: collapse global sum of grouped count while preserving empty input semantics") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
      .groupBy()(sum($"cnt").as("total"))
      .analyze
    val expected = relation
      .select()
      .groupBy()(sum(Literal(1L)).as("total"))
      .analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("SPARK-57043: keep grouped try_sum of count because overflow behavior differs") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
      .groupBy($"a")(
        $"a",
        Sum($"cnt", NumericEvalContext(EvalMode.TRY)).toAggregateExpression().as("total"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-57043: keep grouped sum of nullable count") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count($"b").as("cnt"))
      .groupBy($"a")($"a", sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-57043: keep sum of global count to preserve empty input semantics") {
    val query = relation
      .groupBy()(count(Literal(1)).as("cnt"))
      .groupBy()(sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-57043: collapse grouped sum of count with max and min of grouping output") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
      .groupBy($"a")(
        $"a", sum($"cnt").as("total"), max($"b").as("max_b"), min($"b").as("min_b"))
      .analyze
    val expected = relation.groupBy($"a")(
      $"a", sum(Literal(1L)).as("total"), max($"b").as("max_b"), min($"b").as("min_b"))
      .analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("SPARK-57043: keep grouped sum of count with max of count output") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
      .groupBy($"a")($"a", sum($"cnt").as("total"), max($"cnt").as("max_cnt"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-57043: keep grouped sum of filtered and distinct counts") {
    val filtered = relation
      .groupBy($"a", $"b")(
        $"a", $"b", count(Literal(1), Some($"b" > Literal(0))).as("cnt"))
      .groupBy($"a")($"a", sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(filtered), filtered)

    val distinct = relation
      .groupBy($"a", $"b")($"a", $"b", countDistinct(Literal(1)).as("cnt"))
      .groupBy($"a")($"a", sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(distinct), distinct)
  }

  test("SPARK-57043: keep grouped sum when outer grouping depends on count output") {
    val query = relation
      .groupBy($"a", $"b")($"a", $"b", count(Literal(1)).as("cnt"))
      .groupBy($"a", $"cnt")($"a", $"cnt", sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }

  test("SPARK-57043: keep grouped sum of count with non-deterministic lower grouping") {
    val query = relation
      .groupBy($"a", rand(0))($"a", count(Literal(1)).as("cnt"))
      .groupBy($"a")($"a", sum($"cnt").as("total"))
      .analyze
    comparePlans(Optimize.execute(query), query)
  }
}
