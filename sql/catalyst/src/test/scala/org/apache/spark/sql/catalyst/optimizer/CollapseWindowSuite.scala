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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CollapseWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseWindow", FixedPoint(10),
        CollapseWindow,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation($"a".double, $"b".double, $"c".string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val partitionSpec1 = Seq(c)
  val partitionSpec2 = Seq(c + 1)
  val orderSpec1 = Seq(c.asc)
  val orderSpec2 = Seq(c.desc)

  test("collapse two adjacent windows with the same partition/order") {
    val query = testRelation
      .window(Seq(min(a).as(Symbol("min_a"))), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as(Symbol("max_a"))), partitionSpec1, orderSpec1)
      .window(Seq(sum(b).as(Symbol("sum_b"))), partitionSpec1, orderSpec1)
      .window(Seq(avg(b).as(Symbol("avg_b"))), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation.window(Seq(
      min(a).as(Symbol("min_a")),
      max(a).as(Symbol("max_a")),
      sum(b).as(Symbol("sum_b")),
      avg(b).as(Symbol("avg_b"))), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("Don't collapse adjacent windows with different partitions or orders") {
    val query1 = testRelation
      .window(Seq(min(a).as(Symbol("min_a"))), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as(Symbol("max_a"))), partitionSpec1, orderSpec2)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = query1.analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .window(Seq(min(a).as(Symbol("min_a"))), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as(Symbol("max_a"))), partitionSpec2, orderSpec1)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Don't collapse adjacent windows with dependent columns") {
    val query = testRelation
      .window(Seq(sum(a).as(Symbol("sum_a"))), partitionSpec1, orderSpec1)
      .window(Seq(max($"sum_a").as(Symbol("max_sum_a"))), partitionSpec1, orderSpec1)
      .analyze

    val expected = query.analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("Skip windows with empty window expressions") {
    val query = testRelation
      .window(Seq(), partitionSpec1, orderSpec1)
      .window(Seq(sum(a).as(Symbol("sum_a"))), partitionSpec1, orderSpec1)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-34565: collapse two windows with the same partition/order " +
    "and a Project between them") {

    val query = testRelation
      .window(Seq(min(a).as("_we0")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"_we0" as "min_a")
      .window(Seq(max(a).as("_we1")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"_we1" as "max_a")
      .window(Seq(sum(b).as("_we2")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"max_a", $"_we2" as "sum_b")
      .window(Seq(avg(b).as("_we3")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"max_a", $"sum_b", $"_we3" as "avg_b")
      .analyze

    val optimized = Optimize.execute(query)
    assert(query.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(
        min(a).as("_we0"),
        max(a).as("_we1"),
        sum(b).as("_we2"),
        avg(b).as("_we3")
      ), partitionSpec1, orderSpec1)
      .select(
        a, b, c,
        $"_we0" as "min_a", $"_we1" as "max_a", $"_we2" as "sum_b", $"_we3" as "avg_b")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-34565: do not collapse two windows if project between them " +
    "generates an input column") {

    val query = testRelation
      .window(Seq(min(a).as("min_a")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", ($"a" + $"b").as("d"))
      .window(Seq(max($"d").as("max_d")), partitionSpec1, orderSpec1)
      .analyze

    val optimized = Optimize.execute(query)
    assert(query.output === optimized.output)

    comparePlans(optimized, query)
  }
}
