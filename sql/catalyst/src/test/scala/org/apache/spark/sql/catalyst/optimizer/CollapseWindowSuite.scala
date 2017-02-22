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
        CollapseWindow) :: Nil
  }

  val testRelation = LocalRelation('a.double, 'b.double, 'c.string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val partitionSpec1 = Seq(c)
  val partitionSpec2 = Seq(c + 1)
  val orderSpec1 = Seq(c.asc)
  val orderSpec2 = Seq(c.desc)

  test("collapse two adjacent windows with the same partition/order") {
    val query = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec1, orderSpec1)
      .window(Seq(sum(b).as('sum_b)), partitionSpec1, orderSpec1)
      .window(Seq(avg(b).as('avg_b)), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation.window(Seq(
      min(a).as('min_a),
      max(a).as('max_a),
      sum(b).as('sum_b),
      avg(b).as('avg_b)), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("Don't collapse adjacent windows with different partitions or orders") {
    val query1 = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec1, orderSpec2)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = query1.analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .window(Seq(min(a).as('min_a)), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as('max_a)), partitionSpec2, orderSpec1)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }
}
