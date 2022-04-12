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
import org.apache.spark.sql.catalyst.expressions.{Concat, Rand}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class TransposeWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseProject", FixedPoint(100), CollapseProject, RemoveNoopOperators) ::
      Batch("FlipWindow", Once, CollapseWindow, TransposeWindow) :: Nil
  }

  val testRelation = LocalRelation($"a".string, $"b".string, $"c".int, $"d".string)

  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val d = testRelation.output(3)

  val partitionSpec1 = Seq(a)
  val partitionSpec2 = Seq(a, b)
  val partitionSpec3 = Seq(d)
  val partitionSpec4 = Seq(b, a, d)

  val orderSpec1 = Seq(d.asc)
  val orderSpec2 = Seq(d.desc)

  test("transpose two adjacent windows with compatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec2, orderSpec2)
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, orderSpec1)
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec2, orderSpec2)
      .select($"a", $"b", $"c", $"d", $"sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("transpose two adjacent windows with differently ordered compatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec4, Seq.empty)
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec2, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec2, Seq.empty)
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec4, Seq.empty)
      .select($"a", $"b", $"c", $"d", $"sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("don't transpose two adjacent windows with incompatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("don't transpose two adjacent windows with intersection of partition and output set") {
    val query = testRelation
      .window(Seq(Concat(Seq($"a", $"b")).as(Symbol("e")),
        sum(c).as(Symbol("sum_a_2"))), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), Seq(a, $"e"), Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("don't transpose two adjacent windows with non-deterministic expressions") {
    val query = testRelation
      .window(Seq(Rand(0).as(Symbol("e")), sum(c).as(Symbol("sum_a_2"))), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

  test("SPARK-34807: transpose two windows with compatible partitions " +
    "and a Project between them") {
    val query = testRelation
      .window(Seq(sum(c).as("_we0")), partitionSpec2, orderSpec2)
      .select(a, b, c, d, $"_we0" as "sum_a_2")
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_1"))), partitionSpec1, orderSpec1)
      .window(Seq(sum(c).as(Symbol("_we0"))), partitionSpec2, orderSpec2)
      .select($"a", $"b", $"c", $"d", $"_we0" as "sum_a_2", $"sum_a_1")

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("SPARK-34807: don't transpose two windows if project between them " +
    "generates an input column") {
    val query = testRelation
      .window(Seq(sum(c).as(Symbol("sum_a_2"))), partitionSpec2, orderSpec2)
      .select(a, b, c, d, $"sum_a_2", c + d as "e")
      .window(Seq(sum($"e").as(Symbol("sum_a_1"))), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    comparePlans(optimized, analyzed)
  }

}
