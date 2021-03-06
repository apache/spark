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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveDuplicateWindowExprsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RemoveDuplicateWindowExprs", Once, RemoveDuplicateWindowExprs) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)

  val a = testRelation.output(0)
  val b = testRelation.output(1)

  test("Deduplicate equal expressions") {
    val query = testRelation
      .window(Seq(sum(a).as("a_sum1"), sum(a).as("a_sum2")), Seq.empty, Seq.empty)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = testRelation
      .window(Seq(sum(a).as("a_sum1")), Seq.empty, Seq.empty)
      .select($"a", $"b", $"a_sum1", $"a_sum1" as "a_sum2")
      .analyze

    comparePlans(optimized, expected)
  }

  test("Deduplicate equivalent expressions") {
    val query = testRelation
      .window(
        Seq(sum(Literal(2) * a).as("a_sum1"), sum(a * Literal(2)).as("a_sum2")),
        Seq.empty, Seq.empty)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = testRelation
      .window(Seq(sum(Literal(2) * a).as("a_sum1")), Seq.empty, Seq.empty)
      .select($"a", $"b", $"a_sum1", $"a_sum1" as "a_sum2")
      .analyze

    comparePlans(optimized, expected)
  }

  test("Do not deduplicate distinct expressions") {
    val query = testRelation
      .window(Seq(sum(a).as("a_sum"), sum(b).as("b_sum")), Seq.empty, Seq.empty)
      .analyze

    val optimized = Optimize.execute(query)

    comparePlans(optimized, query)
  }
}
