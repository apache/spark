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
import org.apache.spark.sql.catalyst.expressions.{RowFrame, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnspecifiedFrame}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class TransposeWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseProject", FixedPoint(100), CollapseProject, RemoveRedundantProject) ::
      Batch("FlipWindow", Once, CollapseWindow, TransposeWindow) :: Nil
  }

  val testRelation = LocalRelation('a.string, 'b.string, 'c.int, 'd.string)

  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val d = testRelation.output(3)

  val partitionSpec1 = Seq(a)
  val partitionSpec2 = Seq(a, b)
  val partitionSpec3 = Seq(d)

  val orderSpec1 = Seq(d.asc)
  val orderSpec2 = Seq(d.desc)

  test("flip two adjacent windows with compatible partitions in multiple selects") {
    val wexpr1 = windowExpr(sum('c), windowSpec(partitionSpec2, Seq.empty, UnspecifiedFrame))
    val wexpr2 = windowExpr(sum('c), windowSpec(partitionSpec1, Seq.empty, UnspecifiedFrame))

    val query = testRelation
      .select('a, 'b, 'c,  wexpr1.as('sum_a_2))
      .select('a, 'b, 'c, 'sum_a_2, wexpr2.as('sum_a_1))

    val optimized = Optimize.execute(query.analyze)

    val query2 = testRelation
      .select('a, 'b, 'c)
      .select('a, 'b, 'c,  wexpr2.as('sum_a_1))
      .select('a, 'b, 'c,  wexpr1.as('sum_a_2), 'sum_a_1)
      .select('a, 'b, 'c, 'sum_a_2, 'sum_a_1)

    val correctAnswer = Optimize.execute(query2.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("flip two adjacent windows with compatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as('sum_a_2)), partitionSpec2, orderSpec2)
      .window(Seq(sum(c).as('sum_a_1)), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as('sum_a_1)), partitionSpec1, orderSpec1)
      .window(Seq(sum(c).as('sum_a_2)), partitionSpec2, orderSpec2)
      .select('a, 'b, 'c, 'd, 'sum_a_2, 'sum_a_1)

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("don't flip two adjacent windows with incompatible partitions") {
    val query = testRelation
      .window(Seq(sum(c).as('sum_a_2)), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as('sum_a_1)), partitionSpec1, Seq.empty)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)

    val correctAnswer = testRelation
      .window(Seq(sum(c).as('sum_a_2)), partitionSpec3, Seq.empty)
      .window(Seq(sum(c).as('sum_a_1)), partitionSpec1, Seq.empty)

    comparePlans(optimized, correctAnswer.analyze)
  }

}
